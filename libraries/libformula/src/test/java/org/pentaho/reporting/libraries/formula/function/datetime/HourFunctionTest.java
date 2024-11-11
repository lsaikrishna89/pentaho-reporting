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


package org.pentaho.reporting.libraries.formula.function.datetime;

import org.pentaho.reporting.libraries.formula.FormulaTestBase;

import java.math.BigDecimal;

/**
 * @author Cedric Pronzato
 */
public class HourFunctionTest extends FormulaTestBase {
  public Object[][] createDataTest() {
    return new Object[][]
      {
        { "HOUR(5/24)", new BigDecimal( 5 ) },
        { "HOUR(5/24-1/(24*60*60))", new BigDecimal( 4 ) },
        { "HOUR(\"14:00\")", new BigDecimal( 14 ) },
      };
  }

  public void testDefault() throws Exception {
    runDefaultTest();
  }


}
