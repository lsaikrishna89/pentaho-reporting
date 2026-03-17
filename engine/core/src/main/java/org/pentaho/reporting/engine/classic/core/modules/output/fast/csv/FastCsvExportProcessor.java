/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.reporting.engine.classic.core.modules.output.fast.csv;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.EmptyReportException;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.ReportProcessingException;
import org.pentaho.reporting.engine.classic.core.event.ReportEvent;
import org.pentaho.reporting.engine.classic.core.event.ReportProgressEvent;
import org.pentaho.reporting.engine.classic.core.function.OutputFunction;
import org.pentaho.reporting.engine.classic.core.layout.model.LogicalPageBox;
import org.pentaho.reporting.engine.classic.core.layout.output.AbstractOutputProcessor;
import org.pentaho.reporting.engine.classic.core.layout.output.AbstractReportProcessor;
import org.pentaho.reporting.engine.classic.core.layout.output.ContentProcessingException;
import org.pentaho.reporting.engine.classic.core.layout.output.DefaultProcessingContext;
import org.pentaho.reporting.engine.classic.core.layout.output.LogicalPageKey;
import org.pentaho.reporting.engine.classic.core.layout.output.OutputProcessorFeature;
import org.pentaho.reporting.engine.classic.core.layout.output.OutputProcessorMetaData;
import org.pentaho.reporting.engine.classic.core.modules.output.fast.FastExportOutputFunction;
import org.pentaho.reporting.engine.classic.core.modules.output.table.csv.CSVTableModule;
import org.pentaho.reporting.engine.classic.core.modules.output.table.csv.helper.CSVOutputProcessorMetaData;
import org.pentaho.reporting.engine.classic.core.states.process.EndDetailsHandler;
import org.pentaho.reporting.engine.classic.core.states.process.ProcessState;
import org.pentaho.reporting.libraries.base.config.Configuration;
import org.pentaho.reporting.libraries.base.util.CSVQuoter;
import org.pentaho.reporting.libraries.fonts.encoding.EncodingRegistry;
import javax.swing.table.TableModel;
import java.io.IOException;
import java.io.OutputStream;
import java.io.BufferedOutputStream;

/**
 * A fast CSV export processor optimized for large datasets (millions of rows).
 * <p>
 * Uses a 3-phase hybrid approach:
 * <ol>
 *   <li><b>Phase 1 — Structural headers:</b> Lets the ProcessState state machine handle
 *       structural events (REPORT_INITIALIZED, REPORT_STARTED, GROUP_STARTED, ITEMS_STARTED)
 *       via normal {@code advance()/commit()}. This fires the corresponding
 *       {@code FastExportOutputFunction} callbacks which write report headers, group headers,
 *       details headers, page headers via the template (~5 state transitions).</li>
 *   <li><b>Phase 2 — Bulk data:</b> When the state machine reaches {@code ProcessDetailsHandler}
 *       (ITEMS_ADVANCED), switches to direct {@code TableModel} iteration for the bulk data rows,
 *       bypassing the per-row ProcessState cloning that causes OOM for millions of rows.</li>
 *   <li><b>Phase 3 — Structural footers:</b> Sets the handler to {@code EndDetailsHandler} and
 *       resumes the state machine for closing events (ITEMS_FINISHED, GROUP_FINISHED,
 *       REPORT_FINISHED, REPORT_DONE) which write footers via the template (~6 state transitions).</li>
 * </ol>
 * This preserves all report/group header and footer bands while avoiding the ~10 objects
 * per row created by the standard {@code processPage()} loop.
 */
public class FastCsvExportProcessor extends AbstractReportProcessor {
  private static final Log logger = LogFactory.getLog( FastCsvExportProcessor.class );

  private static final int FLUSH_INTERVAL = 10000;
  private static final int PROGRESS_INTERVAL = 5000;
  private static final int WRITE_BUFFER_SIZE = 65536;
  private static final String LINE_SEPARATOR = "\r\n";

  /**
   * Excel's maximum number of rows per sheet (1,048,576 total = 1 header + 1,048,575 data rows).
   * When data exceeds this limit, a header row is re-written at each boundary so that if the CSV
   * file is split into chunks (e.g., by a script or tool), each chunk starts with column headers.
   */

  private static class CSVDataOutputProcessor extends AbstractOutputProcessor {
    private final OutputProcessorMetaData metaData;

    private CSVDataOutputProcessor() {
      metaData = new CSVOutputProcessorMetaData( CSVOutputProcessorMetaData.PAGINATION_NONE ) {
        public void initialize( final Configuration configuration ) {
          super.initialize( configuration );
          addFeature( OutputProcessorFeature.FAST_EXPORT );
        }
      };
    }

    protected void processPageContent( final LogicalPageKey logicalPageKey, final LogicalPageBox logicalPage )
      throws ContentProcessingException {
      // CSV has no page content processing
    }

    public OutputProcessorMetaData getMetaData() {
      return metaData;
    }
  }

  private final OutputStream outputStream;
  private final String encoding;

  public FastCsvExportProcessor( final MasterReport report, final OutputStream outputStream )
    throws ReportProcessingException {
    this( report, outputStream, null );
  }

  public FastCsvExportProcessor( final MasterReport report, final OutputStream outputStream, final String encoding )
    throws ReportProcessingException {
    super( report, new CSVDataOutputProcessor() );

    this.outputStream = outputStream;
    this.encoding = encoding;
  }

  /**
   * Creates the layout manager. The template is wired to the real output stream so that
   * structural elements (such as report and group headers/footers) can be written via
   * the template during Phase 3, while Phase 2 still writes title, column headers, and
   * data rows directly to the output stream(s).
   */
  protected OutputFunction createLayoutManager() {
    // Use a no-op OutputStream for the template so Phase 1 state machine events
    // (reportStarted, itemsStarted, etc.) don't produce output that would duplicate
    // what Phase 2 writes explicitly.
    return new FastExportOutputFunction( new FastCsvExportTemplate( new NullOutputStream(), encoding ) );
  }

  /**
   * A no-op OutputStream that discards all bytes written to it.
   * Used to suppress Phase 1 template output.
   */
  private static class NullOutputStream extends OutputStream {
    @Override
    public void write( int b ) {
      // discard
    }

    @Override
    public void write( byte[] b, int off, int len ) {
      // discard
    }
  }

  /**
   * Overrides the standard report processing with a hybrid approach
   * optimized for large datasets. See class Javadoc for details.
   *
   * @throws ReportProcessingException if the report processing fails
   */
  @Override
  public void processReport() throws ReportProcessingException {
    try {
      fireProcessingStarted( new ReportProgressEvent( this ) );

      final ProcessState startState = initializeReportState();
      final TableModel tableModel = extractTableModel( startState );

      final int rowCount = tableModel.getRowCount();
      final int columnCount = computeRealColumnCount( tableModel );

      // Phase 1: advance state machine through structural headers (output suppressed)
      ProcessState state = advanceThroughHeaders( startState );
      if ( state.isFinish() ) {
        fireProcessingFinished( new ReportProgressEvent( this ) );
        return;
      }

      // Phase 2: write title + column headers + bulk data rows directly from TableModel
      writeDataRows( tableModel, rowCount, columnCount );

      // Phase 3: resume state machine through structural footers
      advanceThroughFooters( state );

      fireProcessingFinished( new ReportProgressEvent( this ) );
    } catch ( ReportProcessingException re ) {
      throw re;
    } catch ( IOException ioe ) {
      throw new ReportProcessingException( "Failed to write CSV output", ioe );
    } catch ( Exception e ) {
      throw new ReportProcessingException( "Failed to process the report", e );
    }
  }

  /**
   * Initializes the ProcessState by executing the report query and obtaining the TableModel.
   * Sets the process state handle for proper resource cleanup via {@code close()}.
   */
  private ProcessState initializeReportState() throws ReportProcessingException {
    final DefaultProcessingContext processingContext = createProcessingContext();
    final MasterReport report = getReport();
    final OutputFunction lm = createLayoutManager();

    final ProcessState startState = new ProcessState();
    try {
      boolean isQueryLimitReached = startState.initializeForMasterReport(
          report, processingContext, (OutputFunction) lm.getInstance() );
      setQueryLimitReached( isQueryLimitReached );
    } finally {
      setProcessStateHandle( startState.getProcessHandle() );
    }
    return startState;
  }

  /**
   * Extracts and validates the TableModel from the initialized ProcessState.
   *
   * @throws EmptyReportException if the query returned no data or empty columns
   */
  private TableModel extractTableModel( final ProcessState startState ) throws EmptyReportException {
    final TableModel tableModel = startState.getFlowController().getMasterRow().getReportData();
    if ( tableModel == null ) {
      throw new EmptyReportException( "Report query returned no data." );
    }
    if ( tableModel.getRowCount() == 0 || tableModel.getColumnCount() == 0 ) {
      throw new EmptyReportException( "Report did not generate any content." );
    }
    return tableModel;
  }

  /**
   * Computes the real (non-synthetic) column count from the TableModel.
   * <p>
   * The Pentaho engine wraps all TableModels in {@code IndexedTableModel} (via
   * {@code CachingDataFactory}), which doubles the column count by appending synthetic
   * index columns named {@code ::column::0}, {@code ::column::1}, etc. These index columns
   * duplicate the data from the real columns and are used internally for column-index-based
   * lookups in expressions. They must NOT be written to the CSV output.
   * <p>
   * This method scans column names and returns the count of columns whose names do NOT
   * start with the {@code ::column::} prefix.
   *
   * @param tableModel the table model (may be an IndexedTableModel wrapper)
   * @return the number of real data columns, excluding synthetic index columns
   */
  private int computeRealColumnCount( final TableModel tableModel ) {
    final int totalColumns = tableModel.getColumnCount();
    int realCount = 0;
    for ( int col = 0; col < totalColumns; col++ ) {
      final String name = tableModel.getColumnName( col );
      if ( name == null || !name.startsWith( ClassicEngineBoot.INDEX_COLUMN_PREFIX ) ) {
        realCount++;
      }
    }
    return realCount;
  }

  /**
   * Phase 1: Advances the ProcessState state machine through all opening structural events.
   * <p>
   * Fires REPORT_INITIALIZED, REPORT_STARTED, GROUP_STARTED, ITEMS_STARTED events
   * (and nested group starts for multi-level groups). Each event triggers the corresponding
   * {@code FastExportOutputFunction} callback which writes header bands via the template:
   * <ul>
   *   <li>{@code reportInitialized()} → template.initialize()</li>
   *   <li>{@code reportStarted()} → PageHeader + ReportHeader</li>
   *   <li>{@code groupStarted()} → GroupHeader for each group level</li>
   *   <li>{@code itemsStarted()} → DetailsHeader</li>
   * </ul>
   * Stops when the handler becomes ProcessDetailsHandler (ITEMS_ADVANCED event code),
   * meaning the state machine is ready to process the first data row.
   *
   * @return the ProcessState positioned at the first ITEMS_ADVANCED handler, or a finished state
   */
  private ProcessState advanceThroughHeaders( ProcessState state ) throws ReportProcessingException {
    while ( !state.isFinish() ) {
      checkInterrupted();
      if ( state.getAdvanceHandler().getEventCode() == ReportEvent.ITEMS_ADVANCED ) {
        break;
      }
      state = state.advance();
      state = state.commit();
    }
    if ( !state.isFinish() ) {
      logger.debug( "FastCsvExportProcessor: Phase 1 complete - structural headers written" );
    }
    return state;
  }

  /**
   * Phase 2: Writes all data rows directly from the TableModel to the OutputStream.
   * <p>
   * Writes the report title (from {@code MasterReport.getTitle()}) as row 1, followed by the
   * column header row using {@code TableModel.getColumnName()}, then all data rows.
   * <p>
   * <p>
   * Bypasses the ProcessState state machine entirely for data rows. In the standard flow,
   * each row triggers {@code state.advance()} + {@code state.commit()} which creates ~10
   * objects per row (ProcessState clone, 2 FlowControllers, GlobalMasterRow, FastGlobalView
   * with 4 array clones, ExpressionDataRow, DataRowRuntime). For 4.5M rows this produces
   * ~45M short-lived objects, overwhelming the GC and causing OOM.
   * <p>
   * This method instead creates ~2 objects per row (StringBuilder reuse + byte[]).
   */
  private void writeDataRows( final TableModel tableModel, final int rowCount, final int columnCount )
      throws IOException {
    final CsvWriteConfig csvConfig = createCsvWriteConfig();
    final StringBuilder line = new StringBuilder( columnCount * 20 );
    final ReportProgressEvent progressEvent = new ReportProgressEvent( this );
    final String reportTitle = getReport().getTitle();
    OutputStream currentOut = wrapOutputStream( outputStream );


      // Write title and column headers for the first file
    writeReportTitle( line, reportTitle, csvConfig, currentOut );
    writeColumnHeaders( line, tableModel, columnCount, csvConfig, currentOut );

    for ( int row = 0; row < rowCount; row++ ) {
      writeSingleRow( line, tableModel, row, columnCount, csvConfig, currentOut );

      if ( ( row + 1 ) % FLUSH_INTERVAL == 0 ) {
        currentOut.flush();
      }
      fireProgressIfNeeded( progressEvent, row, rowCount );
    }
    currentOut.flush();
  }

  /**
   * Phase 3: Resumes the ProcessState state machine through all closing structural events.
   * <p>
   * Sets the handler to EndDetailsHandler and advances through:
   * ITEMS_FINISHED, GROUP_BODY_FINISHED, GROUP_FINISHED, REPORT_FINISHED, REPORT_DONE.
   * <p>
   * These events trigger {@code FastExportOutputFunction} callbacks which write:
   * DetailsFooter, GroupFooter(s), ReportFooter, PageFooter, and call template.finishReport().
   * <p>
   * The FlowController's {@code advanceRequested} flag remains {@code false} because Phase 2
   * never called {@code performAdvance()}. This causes {@code JoinEndGroupHandler.commit()}
   * to evaluate its re-enter-group condition as {@code false AND true = false}, correctly
   * transitioning to ReportFooterHandler instead of re-entering groups.
   */
  private void advanceThroughFooters( ProcessState state ) throws ReportProcessingException {
    state.setAdvanceHandler( EndDetailsHandler.HANDLER );
    state.setInItemGroup( false );

    while ( !state.isFinish() ) {
      checkInterrupted();
      state = state.advance();
      state = state.commit();
    }
    logger.info( "FastCsvExportProcessor: Phase 3 complete - structural footers written" );
  }

  /**
   * Writes a CSV header row with column names from the TableModel.
   * Each column name is quoted using the configured CSV quoter to handle special characters.
   */
  private void writeColumnHeaders( final StringBuilder line, final TableModel tableModel,
                                   final int columnCount, final CsvWriteConfig csvConfig,
                                   final OutputStream out ) throws IOException {
    line.setLength( 0 );
    for ( int col = 0; col < columnCount; col++ ) {
      if ( col > 0 ) {
        line.append( csvConfig.separatorChar );
      }
      final String columnName = tableModel.getColumnName( col );
      if ( columnName != null ) {
        line.append( csvConfig.quoter.doQuoting( columnName ) );
      }
    }
    line.append( LINE_SEPARATOR );
    out.write( line.toString().getBytes( csvConfig.encoding ) );
  }

  /**
   * Writes the report title as the first row in the CSV output.
   * The title is placed in the first column; remaining columns are left empty.
   * If the report has no title configured, this method writes nothing.
   *
   * @param line       reusable StringBuilder
   * @param title      the report title (may be null)
   * @param csvConfig  CSV formatting configuration
   * @param out        the output stream to write to
   */
  private void writeReportTitle( final StringBuilder line, final String title,
                                 final CsvWriteConfig csvConfig, final OutputStream out ) throws IOException {
    if ( title == null || title.trim().isEmpty() ) {
      return;
    }
    line.setLength( 0 );
    line.append( csvConfig.quoter.doQuoting( title ) );
    line.append( LINE_SEPARATOR );
    out.write( line.toString().getBytes( csvConfig.encoding ) );
  }


  /**
   * Writes a single CSV row from the TableModel to the output stream.
   */
  private void writeSingleRow( final StringBuilder line, final TableModel tableModel,
                               final int row, final int columnCount,
                               final CsvWriteConfig csvConfig, final OutputStream out ) throws IOException {
    line.setLength( 0 );
    for ( int col = 0; col < columnCount; col++ ) {
      if ( col > 0 ) {
        line.append( csvConfig.separatorChar );
      }
      final Object value = tableModel.getValueAt( row, col );
      if ( value != null ) {
        line.append( csvConfig.quoter.doQuoting( String.valueOf( value ) ) );
      }
    }
    line.append( LINE_SEPARATOR );
    out.write( line.toString().getBytes( csvConfig.encoding ) );
  }

  /**
   * Fires progress events at the configured interval.
   */
  private void fireProgressIfNeeded( final ReportProgressEvent progressEvent,
                                     final int row, final int rowCount ) {
    if ( ( row + 1 ) % PROGRESS_INTERVAL == 0 ) {
      progressEvent.reuse( ReportProgressEvent.GENERATING_CONTENT, row, rowCount, 0, 0, 1 );
      fireStateUpdate( progressEvent );

      if ( logger.isDebugEnabled() && ( row + 1 ) % 100000 == 0 ) {
        logger.debug( "FastCsvExportProcessor: Phase 2 - Processed "
            + ( row + 1 ) + " / " + rowCount + " rows" );
      }
    }
  }

  /**
   * Creates the CSV write configuration from report properties.
   */
  private CsvWriteConfig createCsvWriteConfig() {
    final Configuration config = getReport().getConfiguration();
    final String separator = config.getConfigProperty(
        CSVTableModule.SEPARATOR, CSVTableModule.SEPARATOR_DEFAULT );
    if ( separator == null || separator.isEmpty() ) {
        throw new IllegalStateException( "CSV separator must not be null or empty" );
    }
    final char separatorChar = separator.charAt( 0 );
      // Read enclosure character and force-enclosure settings to match CsvTemplateProducer behavior
    final String enclosure = config.getConfigProperty(
              CSVTableModule.ENCLOSURE_CHAR, CSVTableModule.ENCLOSURE_CHAR_DEFAULT );
    final char enclosureChar = enclosure.charAt( 0 );
    final String forceEnclosureProp = config.getConfigProperty(
             CSVTableModule.FORCE_ENCLOSURE, CSVTableModule.FORCE_ENCLOSURE_DEFAULT );
    final boolean forceEnclosure = Boolean.parseBoolean( forceEnclosureProp );
    final CSVQuoter quoter = new CSVQuoter( separatorChar, enclosureChar, forceEnclosure );


      String csvEncoding = this.encoding;
    if ( csvEncoding == null ) {
      csvEncoding = config.getConfigProperty(
          "org.pentaho.reporting.engine.classic.core.modules.output.table.csv.Encoding",
          EncodingRegistry.getPlatformDefaultEncoding() );
    }
    return new CsvWriteConfig( separatorChar, quoter, csvEncoding );
  }

  /**
   * Holds CSV formatting configuration to avoid passing multiple parameters.
   */
  private static final class CsvWriteConfig {
    final char separatorChar;
    final CSVQuoter quoter;
    final String encoding;

    CsvWriteConfig( final char separatorChar, final CSVQuoter quoter, final String encoding ) {
      this.separatorChar = separatorChar;
      this.quoter = quoter;
      this.encoding = encoding;
    }
  }

    /**
     * Wraps the given output stream in a BufferedOutputStream if not already buffered.
     */
  private OutputStream wrapOutputStream( final OutputStream out ) {
    if ( out instanceof BufferedOutputStream ) {
      return out;
    }
    return new BufferedOutputStream( out, WRITE_BUFFER_SIZE );
  }


}
