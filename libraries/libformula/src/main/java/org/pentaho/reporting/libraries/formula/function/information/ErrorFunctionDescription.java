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


package org.pentaho.reporting.libraries.formula.function.information;

import org.pentaho.reporting.libraries.formula.function.AbstractFunctionDescription;
import org.pentaho.reporting.libraries.formula.function.FunctionCategory;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.TextType;

public class ErrorFunctionDescription extends AbstractFunctionDescription {
  public ErrorFunctionDescription() {
    super( "ERROR", "org.pentaho.reporting.libraries.formula.function.information.Error-Function" );
  }

  public Type getValueType() {
    return AnyType.TYPE;
  }

  public FunctionCategory getCategory() {
    return InformationFunctionCategory.CATEGORY;
  }

  public int getParameterCount() {
    return 2;
  }

  public Type getParameterType( final int position ) {
    return TextType.TYPE;
  }

  public boolean isParameterMandatory( final int position ) {
    return position == 0;
  }
}
