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


package org.pentaho.reporting.designer.extensions.pentaho.drilldown;

import org.pentaho.reporting.designer.core.AbstractReportDesignerUiPlugin;

public class DrillDownUiPlugin extends AbstractReportDesignerUiPlugin {
  public DrillDownUiPlugin() {
  }

  public String[] getOverlaySources() {
    return new String[] { "org/pentaho/reporting/designer/extensions/pentaho/drilldown/ui-overlay.xul" };
  }

}
