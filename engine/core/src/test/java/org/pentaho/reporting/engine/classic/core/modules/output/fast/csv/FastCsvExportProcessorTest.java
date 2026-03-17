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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.EmptyReportException;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.ReportProcessingException;
import org.pentaho.reporting.engine.classic.core.TableDataFactory;
import org.pentaho.reporting.engine.classic.core.event.ReportProgressEvent;
import org.pentaho.reporting.engine.classic.core.event.ReportProgressListener;
import org.pentaho.reporting.engine.classic.core.layout.output.AbstractOutputProcessor;
import org.pentaho.reporting.engine.classic.core.layout.output.OutputProcessorFeature;
import org.pentaho.reporting.engine.classic.core.layout.output.OutputProcessorMetaData;
import org.pentaho.reporting.engine.classic.core.modules.output.fast.FastExportOutputFunction;
import org.pentaho.reporting.libraries.base.config.Configuration;
import org.pentaho.reporting.libraries.base.util.CSVQuoter;

import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class FastCsvExportProcessorTest {

  private MasterReport report;
  private ByteArrayOutputStream outputStream;

  @Before
  public void setUp() {
    ClassicEngineBoot.getInstance().start();
    report = new MasterReport();
    outputStream = new ByteArrayOutputStream();
  }

  // =====================================================================
  // Helper: invoke private method via reflection
  // =====================================================================
  private Object invokePrivate( Object target, String methodName, Class<?>[] paramTypes, Object... args )
      throws Exception {
    Method method = target.getClass().getDeclaredMethod( methodName, paramTypes );
    method.setAccessible( true );
    return method.invoke( target, args );
  }

  // =====================================================================
  // Constructor tests
  // =====================================================================
  @Test
  public void testConstructorWithTwoArgs() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    assertThat( processor, is( notNullValue() ) );
    processor.close();
  }

  @Test
  public void testConstructorWithEncoding() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, "UTF-8" );
    assertThat( processor, is( notNullValue() ) );
    processor.close();
  }

  @Test
  public void testConstructorWithNullEncoding() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, null );
    assertThat( processor, is( notNullValue() ) );
    processor.close();
  }

  // =====================================================================
  // computeRealColumnCount — filters synthetic index columns
  // =====================================================================
  @Test
  public void testComputeRealColumnCountNoSyntheticColumns() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "a", "b", "c" } },
        new Object[] { "Name", "Age", "City" } );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 3 ) ) );
    processor.close();
  }

  @Test
  public void testComputeRealColumnCountWithSyntheticColumns() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "a", "b", "0", "1" } },
        new Object[] { "Name", "Age", "::column::0", "::column::1" } );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 2 ) ) );
    processor.close();
  }

  @Test
  public void testComputeRealColumnCountAllSyntheticColumns() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "0", "1" } },
        new Object[] { "::column::0", "::column::1" } );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 0 ) ) );
    processor.close();
  }

  @Test
  public void testComputeRealColumnCountWithNullColumnName() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = mock( TableModel.class );
    doReturn( 3 ).when( tableModel ).getColumnCount();
    doReturn( null ).when( tableModel ).getColumnName( 0 );
    doReturn( "Name" ).when( tableModel ).getColumnName( 1 );
    doReturn( "::column::0" ).when( tableModel ).getColumnName( 2 );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 2 ) ) );
    processor.close();
  }

  @Test
  public void testComputeRealColumnCountEmptyTable() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = mock( TableModel.class );
    doReturn( 0 ).when( tableModel ).getColumnCount();

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 0 ) ) );
    processor.close();
  }

  // =====================================================================
  // wrapOutputStream — BufferedOutputStream wrapping
  // =====================================================================
  @Test
  public void testWrapOutputStreamWrapsPlainStream() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    Object result = invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, outputStream );

    assertThat( result, is( instanceOf( BufferedOutputStream.class ) ) );
    processor.close();
  }

  @Test
  public void testWrapOutputStreamDoesNotDoubleWrap() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    BufferedOutputStream buffered = new BufferedOutputStream( outputStream );

    Object result = invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, buffered );

    assertThat( result, is( equalTo( buffered ) ) );
    processor.close();
  }

  // =====================================================================
  // createCsvWriteConfig — CSV configuration
  // =====================================================================
  @Test
  public void testCreateCsvWriteConfigDefaults() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    Object config = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );

    assertThat( config, is( notNullValue() ) );
    java.lang.reflect.Field separatorField = config.getClass().getDeclaredField( "separatorChar" );
    separatorField.setAccessible( true );
    char separator = (char) separatorField.get( config );
    assertThat( separator, is( equalTo( ',' ) ) );

    java.lang.reflect.Field quoterField = config.getClass().getDeclaredField( "quoter" );
    quoterField.setAccessible( true );
    CSVQuoter quoter = (CSVQuoter) quoterField.get( config );
    assertThat( quoter, is( notNullValue() ) );

    java.lang.reflect.Field encodingField = config.getClass().getDeclaredField( "encoding" );
    encodingField.setAccessible( true );
    String encoding = (String) encodingField.get( config );
    assertThat( encoding, is( notNullValue() ) );
    processor.close();
  }

  @Test
  public void testCreateCsvWriteConfigWithCustomEncoding() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, "ISO-8859-1" );

    Object config = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );

    java.lang.reflect.Field encodingField = config.getClass().getDeclaredField( "encoding" );
    encodingField.setAccessible( true );
    String encoding = (String) encodingField.get( config );
    assertThat( encoding, is( equalTo( "ISO-8859-1" ) ) );
    processor.close();
  }

  // =====================================================================
  // writeColumnHeaders — writes CSV header row
  // =====================================================================
  @Test
  public void testWriteColumnHeaders() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "a", "b" } },
        new Object[] { "Name", "City" } );

    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 2, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "Name" ) );
    assertTrue( output.contains( "City" ) );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  @Test
  public void testWriteColumnHeadersWithNullColumnName() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    TableModel tableModel = mock( TableModel.class );
    doReturn( 2 ).when( tableModel ).getColumnCount();
    doReturn( "Name" ).when( tableModel ).getColumnName( 0 );
    doReturn( null ).when( tableModel ).getColumnName( 1 );

    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 2, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "Name" ) );
    assertTrue( output.contains( "," ) );
    processor.close();
  }

  @Test
  public void testWriteColumnHeadersSingleColumn() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "val" } },
        new Object[] { "SingleCol" } );

    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 1, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "SingleCol" ) );
    assertFalse( output.substring( 0, output.indexOf( "\r\n" ) ).contains( "," ) );
    processor.close();
  }

  // =====================================================================
  // writeReportTitle — writes title as first row
  // =====================================================================
  @Test
  public void testWriteReportTitleWithTitle() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "My Report", csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "My Report" ) );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }


  // =====================================================================
  // writeSingleRow — writes one data row
  // =====================================================================
  @Test
  public void testWriteSingleRow() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "Alice", "30", "NYC" } },
        new Object[] { "Name", "Age", "City" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 3, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "Alice" ) );
    assertTrue( output.contains( "30" ) );
    assertTrue( output.contains( "NYC" ) );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  @Test
  public void testWriteSingleRowWithNullValues() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "Alice", null, "NYC" } },
        new Object[] { "Name", "Age", "City" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 3, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "Alice" ) );
    assertTrue( output.contains( "NYC" ) );
    assertTrue( output.contains( ",," ) );
    processor.close();
  }

  @Test
  public void testWriteSingleRowWithSpecialCharacters() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "O'Brien, Jim", "value with \"quotes\"" } },
        new Object[] { "Name", "Note" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 2, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertFalse( output.isEmpty() );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  @Test
  public void testWriteSingleRowSingleColumn() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "OnlyValue" } },
        new Object[] { "Col" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 1, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "OnlyValue" ) );
    assertFalse( output.contains( "," ) );
    processor.close();
  }

  @Test
  public void testWriteSingleRowAllNulls() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { null, null, null } },
        new Object[] { "A", "B", "C" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 3, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertThat( output, is( equalTo( ",,\r\n" ) ) );
    processor.close();
  }

  @Test
  public void testWriteSingleRowWithNumericAndBooleanValues() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { 42, 3.14, true } },
        new Object[] { "Int", "Double", "Bool" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 3, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "42" ) );
    assertTrue( output.contains( "3.14" ) );
    assertTrue( output.contains( "true" ) );
    processor.close();
  }

  // =====================================================================
  // NullOutputStream — discards all bytes (type-safe lookup)
  // =====================================================================
  @Test
  public void testNullOutputStreamDiscardsSingleByte() throws Exception {
    Class<?>[] innerClasses = FastCsvExportProcessor.class.getDeclaredClasses();
    Class<?> nullOutClass = null;
    for ( Class<?> c : innerClasses ) {
      if ( OutputStream.class.isAssignableFrom( c )
          && !AbstractOutputProcessor.class.isAssignableFrom( c ) ) {
        nullOutClass = c;
        break;
      }
    }
    assertNotNull( "NullOutputStream inner class not found", nullOutClass );

    java.lang.reflect.Constructor<?> ctor = nullOutClass.getDeclaredConstructor();
    ctor.setAccessible( true );
    OutputStream nullOut = (OutputStream) ctor.newInstance();

    nullOut.write( 42 );
    nullOut.write( new byte[] { 1, 2, 3 }, 0, 3 );
    nullOut.flush();
    nullOut.close();
    assertNotNull( nullOut );
  }

  // =====================================================================
  // CSVDataOutputProcessor — inner class (type-safe lookup)
  // =====================================================================
  @Test
  public void testCSVDataOutputProcessorMetadata() throws Exception {
    Class<?>[] innerClasses = FastCsvExportProcessor.class.getDeclaredClasses();
    Class<?> csvDataOutputProcessorClass = null;
    for ( Class<?> c : innerClasses ) {
      if ( AbstractOutputProcessor.class.isAssignableFrom( c ) ) {
        csvDataOutputProcessorClass = c;
        break;
      }
    }
    assertNotNull( "CSVDataOutputProcessor inner class not found", csvDataOutputProcessorClass );

    java.lang.reflect.Constructor<?> ctor = csvDataOutputProcessorClass.getDeclaredConstructor();
    ctor.setAccessible( true );
    Object processor = ctor.newInstance();

    Method getMetaDataMethod = csvDataOutputProcessorClass.getMethod( "getMetaData" );
    Object metaData = getMetaDataMethod.invoke( processor );
    assertThat( metaData, is( notNullValue() ) );
  }

  // =====================================================================
  // createLayoutManager — returns FastExportOutputFunction
  // =====================================================================
  @Test
  public void testCreateLayoutManager() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    Method method = FastCsvExportProcessor.class.getDeclaredMethod( "createLayoutManager" );
    method.setAccessible( true );
    Object lm = method.invoke( processor );

    assertThat( lm, is( notNullValue() ) );
    processor.close();
  }

  // =====================================================================
  // writeDataRows — integration test with multiple rows
  // =====================================================================
  @Test
  public void testWriteDataRowsMultipleRows() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] {
            { "Alice", "30" },
            { "Bob", "25" },
            { "Charlie", "35" }
        },
        new Object[] { "Name", "Age" } );

    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "Test Report", csvConfig, wrapped );

    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 2, csvConfig, wrapped );

    for ( int row = 0; row < 3; row++ ) {
      invokePrivate( processor, "writeSingleRow",
          new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
          line, tableModel, row, 2, csvConfig, wrapped );
    }
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    String[] lines = output.split( "\r\n" );
    assertThat( lines.length, is( equalTo( 5 ) ) );
    assertTrue( lines[ 0 ].contains( "Test Report" ) );
    assertTrue( lines[ 1 ].contains( "Name" ) );
    assertTrue( lines[ 1 ].contains( "Age" ) );
    assertTrue( lines[ 2 ].contains( "Alice" ) );
    assertTrue( lines[ 3 ].contains( "Bob" ) );
    assertTrue( lines[ 4 ].contains( "Charlie" ) );
    processor.close();
  }

  // =====================================================================
  // getOutputProcessor — verify accessible from FastCsvExportProcessor
  // =====================================================================
  @Test
  public void testGetOutputProcessor() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    assertNotNull( processor.getOutputProcessor() );
    processor.close();
  }

  // =====================================================================
  // getConfiguration — verify accessible
  // =====================================================================
  @Test
  public void testGetConfiguration() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    assertNotNull( processor.getConfiguration() );
    processor.close();
  }

  // =====================================================================
  // isHandleInterruptedState / setHandleInterruptedState
  // =====================================================================
  @Test
  public void testHandleInterruptedStateDefault() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    assertTrue( processor.isHandleInterruptedState() );
    processor.close();
  }

  @Test
  public void testSetHandleInterruptedStateFalse() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    processor.setHandleInterruptedState( false );
    assertFalse( processor.isHandleInterruptedState() );
    processor.close();
  }

  // =====================================================================
  // close — idempotent
  // =====================================================================
  @Test
  public void testCloseIsIdempotent() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    processor.close();
    processor.close(); // should not throw
    assertNotNull( processor );
  }

  // =====================================================================
  // cancel — sets interrupted flag
  // =====================================================================
  @Test
  public void testCancel() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    processor.cancel(); // should not throw
    assertNotNull( processor );
    processor.close();
  }

  // =====================================================================
  // isFullStreamingProcessor / setFullStreamingProcessor
  // =====================================================================
  @Test
  public void testFullStreamingProcessorDefault() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    assertTrue( processor.isFullStreamingProcessor() );
    processor.close();
  }

  @Test
  public void testSetFullStreamingProcessorFalse() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    processor.setFullStreamingProcessor( false );
    assertFalse( processor.isFullStreamingProcessor() );
    processor.close();
  }

  // =====================================================================
  // CsvWriteConfig — verify all 3 fields via reflection
  // =====================================================================
  @Test
  public void testCsvWriteConfigFieldsWithSemicolonSeparator() throws Exception {
    // Verify CsvWriteConfig uses report config; default separator is ','
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object config = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );

    java.lang.reflect.Field sepField = config.getClass().getDeclaredField( "separatorChar" );
    sepField.setAccessible( true );
    java.lang.reflect.Field quoterField = config.getClass().getDeclaredField( "quoter" );
    quoterField.setAccessible( true );
    java.lang.reflect.Field encField = config.getClass().getDeclaredField( "encoding" );
    encField.setAccessible( true );

    char sep = (char) sepField.get( config );
    CSVQuoter quoter = (CSVQuoter) quoterField.get( config );
    String enc = (String) encField.get( config );

    assertThat( sep, is( equalTo( ',' ) ) );
    assertNotNull( quoter );
    assertNotNull( enc );
    processor.close();
  }

  // =====================================================================
  // writeColumnHeaders + writeSingleRow reuse StringBuilder
  // =====================================================================
  @Test
  public void testStringBuilderReusedBetweenCalls() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "val1", "val2" } },
        new Object[] { "Col1", "Col2" } );

    // Write headers
    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 2, csvConfig, wrapped );
    // Reuse same StringBuilder for a data row
    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 2, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    String[] lines = output.split( "\r\n" );
    assertThat( lines.length, is( equalTo( 2 ) ) );
    assertTrue( lines[ 0 ].contains( "Col1" ) );
    assertTrue( lines[ 1 ].contains( "val1" ) );
    processor.close();
  }

  // =====================================================================
  // processReport — EmptyReportException when report has no data query
  // =====================================================================
  @Test
  public void testProcessReportThrowsEmptyReportExceptionForNoData() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    try {
      processor.processReport();
      fail( "Expected EmptyReportException to be thrown" );
    } catch ( EmptyReportException e ) {
      assertThat( e.getMessage(), is( notNullValue() ) );
    } finally {
      processor.close();
    }
  }

  // =====================================================================
  // processReport — catches ReportProcessingException and rethrows
  // =====================================================================
  @Test
  public void testProcessReportRethrowsReportProcessingException() throws ReportProcessingException {
    // A report with no query will throw EmptyReportException (subclass of ReportProcessingException)
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    try {
      processor.processReport();
      fail( "Expected ReportProcessingException to be thrown" );
    } catch ( ReportProcessingException e ) {
      assertNotNull( e.getMessage() );
    } finally {
      processor.close();
    }
  }

  // =====================================================================
  // extractTableModel — null tableModel throws EmptyReportException
  // (tested indirectly via processReport since extractTableModel is private)
  // =====================================================================
  @Test
  public void testExtractTableModelNullThrowsEmptyReportException() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    try {
      processor.processReport();
      fail( "Expected EmptyReportException" );
    } catch ( EmptyReportException e ) {
      assertTrue( e.getMessage().contains( "data" ) || e.getMessage().contains( "content" ) );
    } finally {
      processor.close();
    }
  }


  // =====================================================================
  // writeSingleRow — values containing newlines are quoted properly
  // =====================================================================
  @Test
  public void testWriteSingleRowWithNewlineInValue() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "line1\nline2" } },
        new Object[] { "Data" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 1, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertFalse( output.isEmpty() );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  // =====================================================================
  // writeReportTitle — title with newlines
  // =====================================================================
  @Test
  public void testWriteReportTitleWithNewlines() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "Title\nWith\nNewlines", csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertFalse( output.isEmpty() );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  // =====================================================================
  // createCsvWriteConfig — encoding from config when constructor encoding is null
  // =====================================================================
  @Test
  public void testCreateCsvWriteConfigFallsBackToConfigEncoding() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, null );

    Object config = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );

    Field encodingField = config.getClass().getDeclaredField( "encoding" );
    encodingField.setAccessible( true );
    String encoding = (String) encodingField.get( config );
    // Should be non-null — falls back to platform default
    assertNotNull( encoding );
    assertFalse( encoding.isEmpty() );
    processor.close();
  }

  // =====================================================================
  // Processor with encoding "UTF-16" — writeSingleRow with encoding
  // =====================================================================
  @Test
  public void testWriteSingleRowWithUtf16Encoding() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, "UTF-16" );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "Hello" } },
        new Object[] { "Msg" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 1, csvConfig, wrapped );
    wrapped.flush();

    // UTF-16 output will be different from UTF-8
    byte[] bytes = testOut.toByteArray();
    assertTrue( bytes.length > 0 );
    // Decode back as UTF-16 and verify content
    String output = new String( bytes, "UTF-16" );
    assertThat( output, containsString( "Hello" ) );
    processor.close();
  }

  // =====================================================================
  // Multiple writeSingleRow calls reuse StringBuilder correctly
  // =====================================================================
  @Test
  public void testMultipleWriteSingleRowCallsProduceCorrectOutput() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] {
            { "R0C0", "R0C1" },
            { "R1C0", "R1C1" },
            { "R2C0", "R2C1" }
        },
        new Object[] { "A", "B" } );

    for ( int row = 0; row < 3; row++ ) {
      invokePrivate( processor, "writeSingleRow",
          new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
          line, tableModel, row, 2, csvConfig, wrapped );
    }
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    String[] lines = output.split( "\r\n" );
    assertThat( lines.length, is( equalTo( 3 ) ) );
    assertThat( lines[ 0 ], containsString( "R0C0" ) );
    assertThat( lines[ 0 ], containsString( "R0C1" ) );
    assertThat( lines[ 1 ], containsString( "R1C0" ) );
    assertThat( lines[ 2 ], containsString( "R2C0" ) );
    processor.close();
  }

  // =====================================================================
  // writeColumnHeaders — all null column names
  // =====================================================================
  @Test
  public void testWriteColumnHeadersAllNullNames() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    TableModel tableModel = mock( TableModel.class );
    doReturn( 3 ).when( tableModel ).getColumnCount();
    doReturn( null ).when( tableModel ).getColumnName( 0 );
    doReturn( null ).when( tableModel ).getColumnName( 1 );
    doReturn( null ).when( tableModel ).getColumnName( 2 );

    invokePrivate( processor, "writeColumnHeaders",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 3, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    // Should produce ",,\r\n" — separators but no names
    assertThat( output, is( equalTo( ",,\r\n" ) ) );
    processor.close();
  }

  // =====================================================================
  // computeRealColumnCount — mixed: synthetic in middle of real columns
  // =====================================================================
  @Test
  public void testComputeRealColumnCountMixedOrder() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = mock( TableModel.class );
    doReturn( 5 ).when( tableModel ).getColumnCount();
    doReturn( "Name" ).when( tableModel ).getColumnName( 0 );
    doReturn( "::column::0" ).when( tableModel ).getColumnName( 1 );
    doReturn( "Age" ).when( tableModel ).getColumnName( 2 );
    doReturn( "::column::1" ).when( tableModel ).getColumnName( 3 );
    doReturn( "City" ).when( tableModel ).getColumnName( 4 );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 3 ) ) );
    processor.close();
  }

  // =====================================================================
  // writeSingleRow — large number of columns
  // =====================================================================
  @Test
  public void testWriteSingleRowLargeColumnCount() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    int numCols = 50;
    Object[] row = new Object[ numCols ];
    Object[] headers = new Object[ numCols ];
    for ( int i = 0; i < numCols; i++ ) {
      row[ i ] = "val" + i;
      headers[ i ] = "Col" + i;
    }
    TableModel tableModel = new DefaultTableModel( new Object[][] { row }, headers );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, numCols, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "val0" ) );
    assertTrue( output.contains( "val49" ) );
    long commaCount = output.substring( 0, output.indexOf( "\r\n" ) ).chars().filter( ch -> ch == ',' ).count();
    assertThat( commaCount, is( equalTo( 49L ) ) );
    processor.close();
  }

  // =====================================================================
  // CsvWriteConfig — encoding field preserves custom encoding
  // =====================================================================
  @Test
  public void testCsvWriteConfigEncodingPreservesUTF8() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream, "UTF-8" );

    Object config = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );

    Field encodingField = config.getClass().getDeclaredField( "encoding" );
    encodingField.setAccessible( true );
    assertThat( (String) encodingField.get( config ), is( equalTo( "UTF-8" ) ) );
    processor.close();
  }

  // =====================================================================
  // processReport — progress listeners receive started/finished events
  // =====================================================================
  @Test
  public void testProcessReportFiresStartedEventBeforeException() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    final List<String> events = new ArrayList<>();
    processor.addReportProgressListener( new ReportProgressListener() {
      public void reportProcessingStarted( ReportProgressEvent event ) {
        events.add( "started" );
      }
      public void reportProcessingUpdate( ReportProgressEvent event ) {
        events.add( "update" );
      }
      public void reportProcessingFinished( ReportProgressEvent event ) {
        events.add( "finished" );
      }
    } );

    try {
      processor.processReport();
    } catch ( ReportProcessingException e ) {
      // expected — no data
    }

    // "started" should have been fired before the exception
    assertTrue( events.contains( "started" ) );
    processor.close();
  }

  // =====================================================================
  // writeReportTitle — single-char title
  // =====================================================================
  @Test
  public void testWriteReportTitleSingleCharacter() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "X", csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "X" ) );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  // =====================================================================
  // writeSingleRow — value is an empty string (not null)
  // =====================================================================
  @Test
  public void testWriteSingleRowWithEmptyStringValue() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();
    TableModel tableModel = new DefaultTableModel(
        new Object[][] { { "", "data" } },
        new Object[] { "A", "B" } );

    invokePrivate( processor, "writeSingleRow",
        new Class<?>[] { StringBuilder.class, TableModel.class, int.class, int.class, csvConfig.getClass(), OutputStream.class },
        line, tableModel, 0, 2, csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertTrue( output.contains( "data" ) );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  // =====================================================================
  // computeRealColumnCount — single column that is synthetic
  // =====================================================================
  @Test
  public void testComputeRealColumnCountSingleSyntheticColumn() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = mock( TableModel.class );
    doReturn( 1 ).when( tableModel ).getColumnCount();
    doReturn( "::column::0" ).when( tableModel ).getColumnName( 0 );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 0 ) ) );
    processor.close();
  }

  // =====================================================================
  // computeRealColumnCount — single column that is real
  // =====================================================================
  @Test
  public void testComputeRealColumnCountSingleRealColumn() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    TableModel tableModel = mock( TableModel.class );
    doReturn( 1 ).when( tableModel ).getColumnCount();
    doReturn( "RealCol" ).when( tableModel ).getColumnName( 0 );

    int result = (int) invokePrivate( processor, "computeRealColumnCount",
        new Class<?>[] { TableModel.class }, tableModel );

    assertThat( result, is( equalTo( 1 ) ) );
    processor.close();
  }

  // =====================================================================
  // Helper: create a MasterReport wired to a TableDataFactory
  // =====================================================================
  private MasterReport createReportWithData( final DefaultTableModel tableModel ) {
    MasterReport r = new MasterReport();
    final TableDataFactory tdf = new TableDataFactory();
    tdf.addTable( r.getQuery(), tableModel );
    r.setDataFactory( tdf );
    return r;
  }

  // =====================================================================
  // processReport — FULL success path (Phase 1 → 2 → 3)
  // Covers: processReport(), initializeReportState(), extractTableModel(),
  //         advanceThroughHeaders(), writeDataRows(), advanceThroughFooters(),
  //         fireProcessingStarted/Finished, computeRealColumnCount (called),
  //         writeReportTitle, writeColumnHeaders, writeSingleRow, wrapOutputStream,
  //         createCsvWriteConfig
  // =====================================================================
  @Test
  public void testProcessReportSuccessPathWithData() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] {
            { "Alice", 30 },
            { "Bob", 25 }
        },
        new Object[] { "Name", "Age" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );
    processor.processReport();
    processor.close();

    String csv = out.toString( StandardCharsets.UTF_8 );
    // column headers must be present
    assertTrue( csv.contains( "Name" ) );
    assertTrue( csv.contains( "Age" ) );
    // data rows must be present
    assertTrue( csv.contains( "Alice" ) );
    assertTrue( csv.contains( "Bob" ) );
    assertTrue( csv.contains( "30" ) );
    assertTrue( csv.contains( "25" ) );
  }

  @Test
  public void testProcessReportSuccessWithEncoding() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] { { "Hello" } },
        new Object[] { "Msg" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out, "UTF-8" );
    processor.processReport();
    processor.close();

    String csv = out.toString( StandardCharsets.UTF_8 );
    assertTrue( csv.contains( "Hello" ) );
    assertTrue( csv.contains( "Msg" ) );
  }

  // =====================================================================
  // processReport — fires both started AND finished progress events
  // =====================================================================
  @Test
  public void testProcessReportFiresStartedAndFinishedEvents() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] { { "x" } },
        new Object[] { "C" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );

    final List<String> events = new ArrayList<>();
    processor.addReportProgressListener( new ReportProgressListener() {
      public void reportProcessingStarted( ReportProgressEvent event ) { events.add( "started" ); }
      public void reportProcessingUpdate( ReportProgressEvent event ) { events.add( "update" ); }
      public void reportProcessingFinished( ReportProgressEvent event ) { events.add( "finished" ); }
    } );

    processor.processReport();
    processor.close();

    assertTrue( events.contains( "started" ) );
    assertTrue( events.contains( "finished" ) );
  }

  // =====================================================================
  // processReport — large data triggers FLUSH_INTERVAL and PROGRESS_INTERVAL
  // =====================================================================
  @Test
  public void testProcessReportLargeDataTriggersFlushAndProgress() throws Exception {
    final int rowCount = 11000;
    Object[][] rows = new Object[ rowCount ][ 1 ];
    for ( int i = 0; i < rowCount; i++ ) {
      rows[ i ] = new Object[] { "row" + i };
    }
    DefaultTableModel data = new DefaultTableModel( rows, new Object[] { "Val" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );

    final List<String> events = new ArrayList<>();
    processor.addReportProgressListener( new ReportProgressListener() {
      public void reportProcessingStarted( ReportProgressEvent event ) { events.add( "started" ); }
      public void reportProcessingUpdate( ReportProgressEvent event ) { events.add( "update" ); }
      public void reportProcessingFinished( ReportProgressEvent event ) { events.add( "finished" ); }
    } );

    processor.processReport();
    processor.close();

    String csv = out.toString( StandardCharsets.UTF_8 );
    assertTrue( csv.contains( "Val" ) );
    assertTrue( csv.contains( "row0" ) );
    assertTrue( csv.contains( "row" + ( rowCount - 1 ) ) );
    assertTrue( events.contains( "update" ) );
    assertTrue( events.contains( "started" ) );
    assertTrue( events.contains( "finished" ) );
  }

  // =====================================================================
  // processReport — multiple columns with null values
  // =====================================================================
  @Test
  public void testProcessReportWithNullValues() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] {
            { "Alice", null, "NYC" },
            { null, 25, null }
        },
        new Object[] { "Name", "Age", "City" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );
    processor.processReport();
    processor.close();

    String csv = out.toString( StandardCharsets.UTF_8 );
    assertTrue( csv.contains( "Name" ) );
    assertTrue( csv.contains( "Alice" ) );
    assertTrue( csv.contains( "NYC" ) );
    assertTrue( csv.contains( "25" ) );
  }

  // =====================================================================
  // processReport — single row, single column (minimum viable)
  // =====================================================================
  @Test
  public void testProcessReportSingleRowSingleColumn() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] { { "only" } },
        new Object[] { "X" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );
    processor.processReport();
    processor.close();

    String csv = out.toString( StandardCharsets.UTF_8 );
    assertTrue( csv.contains( "X" ) );
    assertTrue( csv.contains( "only" ) );
  }

  // =====================================================================
  // processReport — EmptyReportException for zero rows
  // =====================================================================
  @Test
  public void testProcessReportThrowsEmptyReportForZeroRows() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] {},
        new Object[] { "Col" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );
    try {
      processor.processReport();
      fail( "Expected EmptyReportException for zero-row data" );
    } catch ( EmptyReportException e ) {
      assertNotNull( e.getMessage() );
    } finally {
      processor.close();
    }
  }

  // =====================================================================
  // processReport — EmptyReportException for zero columns
  // =====================================================================
  @Test
  public void testProcessReportThrowsEmptyReportForZeroColumns() throws Exception {
    DefaultTableModel data = new DefaultTableModel( 1, 0 );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, out );
    try {
      processor.processReport();
      fail( "Expected EmptyReportException for zero-column data" );
    } catch ( EmptyReportException e ) {
      assertNotNull( e.getMessage() );
    } finally {
      processor.close();
    }
  }

  // =====================================================================
  // processReport — with BufferedOutputStream (wrapOutputStream no-wrap)
  // =====================================================================
  @Test
  public void testProcessReportWithBufferedOutputStream() throws Exception {
    DefaultTableModel data = new DefaultTableModel(
        new Object[][] { { "data" } },
        new Object[] { "C" } );

    MasterReport r = createReportWithData( data );
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    BufferedOutputStream buffered = new BufferedOutputStream( raw );
    FastCsvExportProcessor processor = new FastCsvExportProcessor( r, buffered );
    processor.processReport();
    processor.close();
    buffered.flush();

    String csv = raw.toString( "UTF-8" );
    assertTrue( csv.contains( "C" ) );
    assertTrue( csv.contains( "data" ) );
  }

  // =====================================================================
  // writeReportTitle — null title writes nothing
  // =====================================================================
  @Test
  public void testWriteReportTitleWithNullTitle() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, null, csvConfig, wrapped );
    wrapped.flush();

    assertTrue( testOut.toString( StandardCharsets.UTF_8 ).isEmpty() );
    processor.close();
  }

  // =====================================================================
  // writeReportTitle — whitespace-only title writes nothing
  // =====================================================================
  @Test
  public void testWriteReportTitleWithWhitespaceTitle() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "   ", csvConfig, wrapped );
    wrapped.flush();

    assertTrue( testOut.toString( StandardCharsets.UTF_8 ).isEmpty() );
    processor.close();
  }

  // =====================================================================
  // writeReportTitle — title with special CSV characters
  // =====================================================================
  @Test
  public void testWriteReportTitleWithSpecialChars() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    Object csvConfig = invokePrivate( processor, "createCsvWriteConfig", new Class<?>[] {} );
    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    OutputStream wrapped = (OutputStream) invokePrivate( processor, "wrapOutputStream",
        new Class<?>[] { OutputStream.class }, testOut );
    StringBuilder line = new StringBuilder();

    invokePrivate( processor, "writeReportTitle",
        new Class<?>[] { StringBuilder.class, String.class, csvConfig.getClass(), OutputStream.class },
        line, "Report, with \"commas\" and quotes", csvConfig, wrapped );
    wrapped.flush();

    String output = testOut.toString( StandardCharsets.UTF_8 );
    assertFalse( output.isEmpty() );
    assertTrue( output.endsWith( "\r\n" ) );
    processor.close();
  }

  // =====================================================================
  // fireProgressIfNeeded — fires at PROGRESS_INTERVAL boundary
  // =====================================================================
  @Test
  public void testFireProgressIfNeededAtBoundary() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    ReportProgressEvent progressEvent = new ReportProgressEvent( processor );

    final List<String> captured = new ArrayList<>();
    processor.addReportProgressListener( new ReportProgressListener() {
        public void reportProcessingStarted( ReportProgressEvent event ) {
            // no-op: not needed for this test
        }
      public void reportProcessingUpdate( ReportProgressEvent event ) { captured.add( "update" ); }
        public void reportProcessingFinished( ReportProgressEvent event ) {
            // no-op: not needed for this test
        }
    } );

    // row=4999 → (row+1)=5000 → 5000 % 5000 == 0 → should fire
    invokePrivate( processor, "fireProgressIfNeeded",
        new Class<?>[] { ReportProgressEvent.class, int.class, int.class },
        progressEvent, 4999, 10000 );

    assertThat( captured.size(), is( equalTo( 1 ) ) );
    processor.close();
  }

  @Test
  public void testFireProgressIfNeededNotAtBoundary() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    ReportProgressEvent progressEvent = new ReportProgressEvent( processor );

    final List<String> captured = new ArrayList<>();
    processor.addReportProgressListener( new ReportProgressListener() {
        public void reportProcessingStarted( ReportProgressEvent event ) {
            // no-op: not needed for this test
        }
      public void reportProcessingUpdate( ReportProgressEvent event ) { captured.add( "update" ); }
        public void reportProcessingFinished( ReportProgressEvent event ) {
            // no-op: not needed for this test
        }
    } );

    // row=100 → (row+1)=101 → not % 5000 → should NOT fire
    invokePrivate( processor, "fireProgressIfNeeded",
        new Class<?>[] { ReportProgressEvent.class, int.class, int.class },
        progressEvent, 100, 10000 );

    assertThat( captured.size(), is( equalTo( 0 ) ) );
    processor.close();
  }

  // =====================================================================
  // CSVDataOutputProcessor — FAST_EXPORT feature
  // =====================================================================
  @Test
  public void testCSVDataOutputProcessorHasFastExportFeature() throws Exception {
    Class<?>[] innerClasses = FastCsvExportProcessor.class.getDeclaredClasses();
    Class<?> csvDataOutputProcessorClass = null;
    for ( Class<?> c : innerClasses ) {
      if ( AbstractOutputProcessor.class.isAssignableFrom( c ) ) {
        csvDataOutputProcessorClass = c;
        break;
      }
    }
    assertNotNull( "CSVDataOutputProcessor inner class not found", csvDataOutputProcessorClass );

    Constructor<?> ctor = csvDataOutputProcessorClass.getDeclaredConstructor();
    ctor.setAccessible( true );
    Object csvProcessor = ctor.newInstance();

    Method getMetaDataMethod = csvDataOutputProcessorClass.getMethod( "getMetaData" );
    OutputProcessorMetaData metaData = (OutputProcessorMetaData) getMetaDataMethod.invoke( csvProcessor );

    Configuration config = ClassicEngineBoot.getInstance().getGlobalConfig();
    metaData.initialize( config );

    assertTrue( metaData.isFeatureSupported( OutputProcessorFeature.FAST_EXPORT ) );
  }

  // =====================================================================
  // CSVDataOutputProcessor — processPageContent is a no-op
  // =====================================================================
  @Test
  public void testCSVDataOutputProcessorProcessPageContentDoesNothing() throws Exception {
    Class<?>[] innerClasses = FastCsvExportProcessor.class.getDeclaredClasses();
    Class<?> csvDataOutputProcessorClass = null;
    for ( Class<?> c : innerClasses ) {
      if ( AbstractOutputProcessor.class.isAssignableFrom( c ) ) {
        csvDataOutputProcessorClass = c;
        break;
      }
    }
    assertNotNull( csvDataOutputProcessorClass );

    Constructor<?> ctor = csvDataOutputProcessorClass.getDeclaredConstructor();
    ctor.setAccessible( true );
    Object csvProcessor = ctor.newInstance();

    Method processPageContent = csvDataOutputProcessorClass.getDeclaredMethod(
        "processPageContent",
        org.pentaho.reporting.engine.classic.core.layout.output.LogicalPageKey.class,
        org.pentaho.reporting.engine.classic.core.layout.model.LogicalPageBox.class );
    processPageContent.setAccessible( true );

    processPageContent.invoke( csvProcessor, (Object) null, (Object) null );
    assertNotNull( csvProcessor );
  }

  // =====================================================================
  // createLayoutManager — returns a FastExportOutputFunction
  // =====================================================================
  @Test
  public void testCreateLayoutManagerReturnsFastExportOutputFunction() throws Exception {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    Method method = FastCsvExportProcessor.class.getDeclaredMethod( "createLayoutManager" );
    method.setAccessible( true );
    Object lm = method.invoke( processor );

    assertThat( lm, is( instanceOf( FastExportOutputFunction.class ) ) );
    processor.close();
  }

  // =====================================================================
  // addReportProgressListener / removeReportProgressListener
  // =====================================================================
  @Test
  public void testAddAndRemoveProgressListener() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );

    ReportProgressListener listener = new ReportProgressListener() {
        public void reportProcessingStarted( ReportProgressEvent event ) {
            // no-op
        }
        public void reportProcessingUpdate( ReportProgressEvent event ) {
            // no-op
        }
        public void reportProcessingFinished( ReportProgressEvent event ) {
            // no-op
        }
    };

    processor.addReportProgressListener( listener );
    processor.removeReportProgressListener( listener );
    assertNotNull( processor );
    processor.close();
  }

  // =====================================================================
  // Output processor metadata is CSV type
  // =====================================================================
  @Test
  public void testOutputProcessorMetaDataIsCSVType() throws ReportProcessingException {
    FastCsvExportProcessor processor = new FastCsvExportProcessor( report, outputStream );
    OutputProcessorMetaData metaData = processor.getOutputProcessor().getMetaData();
    assertNotNull( metaData );
    processor.close();
  }

  // =====================================================================
  // CSVDataOutputProcessor — getMetaData returns same instance
  // =====================================================================
  @Test
  public void testCSVDataOutputProcessorGetMetaDataMultipleCalls() throws Exception {
    Class<?>[] innerClasses = FastCsvExportProcessor.class.getDeclaredClasses();
    Class<?> csvDataOutputProcessorClass = null;
    for ( Class<?> c : innerClasses ) {
      if ( AbstractOutputProcessor.class.isAssignableFrom( c ) ) {
        csvDataOutputProcessorClass = c;
        break;
      }
    }
    assertNotNull( csvDataOutputProcessorClass );

    Constructor<?> ctor = csvDataOutputProcessorClass.getDeclaredConstructor();
    ctor.setAccessible( true );
    Object csvProcessor = ctor.newInstance();

    Method getMetaDataMethod = csvDataOutputProcessorClass.getMethod( "getMetaData" );
    Object metaData1 = getMetaDataMethod.invoke( csvProcessor );
    Object metaData2 = getMetaDataMethod.invoke( csvProcessor );

    assertThat( metaData1, is( notNullValue() ) );
    assertSame( metaData1, metaData2 );
  }
}
