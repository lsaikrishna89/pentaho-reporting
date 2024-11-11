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


package org.pentaho.reporting.engine.classic.core.util.beans;

/**
 * A class that handles the conversion of {@link Integer} attributes to and from their {@link String} representation.
 *
 * @author Thomas Morgner
 */
public class StringValueConverter implements ValueConverter {

  /**
   * Creates a new value converter.
   */
  public StringValueConverter() {
    super();
  }

  /**
   * Converts the attribute to a string.
   *
   * @param o
   *          the attribute ({@link Integer} expected).
   * @return A string representing the {@link Integer} value.
   */
  public String toAttributeValue( final Object o ) throws BeanException {
    if ( o == null ) {
      throw new NullPointerException();
    }
    if ( o instanceof String ) {
      return o.toString();
    }
    throw new BeanException( "Failed to convert object of type " + o.getClass() + ": Not a String." );
  }

  /**
   * Converts a string to a {@link Integer}.
   *
   * @param s
   *          the string.
   * @return a {@link Integer}.
   */
  public Object toPropertyValue( final String s ) {
    if ( s == null ) {
      throw new NullPointerException();
    }
    return s;
  }
}
