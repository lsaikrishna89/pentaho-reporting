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


package org.pentaho.reporting.engine.classic.demo.ancient.demo.world;

import java.net.URL;
import javax.swing.JComponent;

import org.pentaho.reporting.engine.classic.core.ClassicEngineBoot;
import org.pentaho.reporting.engine.classic.core.MasterReport;
import org.pentaho.reporting.engine.classic.core.TableDataFactory;
import org.pentaho.reporting.engine.classic.core.modules.gui.base.PreviewDialog;
import org.pentaho.reporting.engine.classic.demo.util.AbstractXmlDemoHandler;
import org.pentaho.reporting.engine.classic.demo.util.ReportDefinitionException;
import org.pentaho.reporting.libraries.base.util.ObjectUtilities;

/**
 * Creation-Date: 28.08.2005, 20:13:43
 *
 * @author Thomas Morgner
 */
public class CountryReportSecurityXMLDemoHandler extends AbstractXmlDemoHandler
{
  private CountryDataTableModel data;

  public CountryReportSecurityXMLDemoHandler()
  {
    data = new CountryDataTableModel();
  }

  public String getDemoName()
  {
    return "Country Report Demo (PDF Security)";
  }

  public MasterReport createReport() throws ReportDefinitionException
  {
    final MasterReport report = parseReport();
    report.setDataFactory(new TableDataFactory
        ("default", data));
    return report;
  }

  public URL getDemoDescriptionSource()
  {
    return ObjectUtilities.getResourceRelative("country-report-security.html",
        CountryReportSecurityXMLDemoHandler.class);
  }

  public JComponent getPresentationComponent()
  {
    return createDefaultTable(data);
  }

  public URL getReportDefinitionSource()
  {
    return ObjectUtilities.getResourceRelative("country-report-security.xml",
        CountryReportSecurityXMLDemoHandler.class);
  }


  /**
   * Entry point for running the demo application...
   *
   * @param args ignored.
   */
  public static void main(final String[] args) throws ReportDefinitionException
  {
    // initialize JFreeReport
    ClassicEngineBoot.getInstance().start();

    final CountryReportSecurityXMLDemoHandler handler = new CountryReportSecurityXMLDemoHandler();
    PreviewDialog dialog = new PreviewDialog();
    dialog.pack();
    dialog.setVisible(true);
//    final SimpleDemoFrame frame = new SimpleDemoFrame(handler);
//    frame.init();
//    frame.pack();
//    RefineryUtilities.centerFrameOnScreen(frame);
//    frame.setVisible(true);
//    PdfReportUtil.createPDF(handler.createReport(), "/tmp/report.pdf");
  }
}
