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

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.Sequence;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.NumberType;

import java.math.BigDecimal;

/**
 * This function counts the numbers in the list of NumberSequences provided. Only numbers in references are counted; all
 * other types are ignored. Errors are not propagated.
 *
 * @author Cedric Pronzato
 */
public class CountFunction implements Function {

  public CountFunction() {
  }

  public String getCanonicalName() {
    return "COUNT";
  }

  public TypeValuePair evaluate( final FormulaContext context,
                                 final ParameterCallback parameters ) throws EvaluationException {
    final int parameterCount = parameters.getParameterCount();

    if ( parameterCount == 0 ) {
      throw EvaluationException.getInstance( LibFormulaErrorValue.ERROR_ARGUMENTS_VALUE );
    }

    int count = 0;

    for ( int paramIdx = 0; paramIdx < parameterCount; paramIdx++ ) {
      try {
        final Type type = parameters.getType( paramIdx );
        final Object value = parameters.getValue( paramIdx );
        final Sequence sequence = context.getTypeRegistry().convertToNumberSequence( type, value, true );

        while ( sequence.hasNext() ) {
          sequence.next();
          count++;
        }
      } catch ( EvaluationException e ) {
        // This is in case of an error value in a scalar argument, we must ignore this error in number sequences
        // todo: maybe it has to be done for other type of exceptions.
      }
    }

    return new TypeValuePair( NumberType.GENERIC_NUMBER, new BigDecimal( count ) );
  }
}
