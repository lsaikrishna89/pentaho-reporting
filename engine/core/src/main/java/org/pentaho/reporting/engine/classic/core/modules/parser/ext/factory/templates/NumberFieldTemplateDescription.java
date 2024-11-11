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


package org.pentaho.reporting.engine.classic.core.modules.parser.ext.factory.templates;

import org.pentaho.reporting.engine.classic.core.filter.templates.NumberFieldTemplate;

/**
 * A number field template description.
 *
 * @author Thomas Morgner
 */
public class NumberFieldTemplateDescription extends AbstractTemplateDescription {
  /**
   * Creates a new template description.
   *
   * @param name
   *          the name.
   */
  public NumberFieldTemplateDescription( final String name ) {
    super( name, NumberFieldTemplate.class, true );
  }

}
