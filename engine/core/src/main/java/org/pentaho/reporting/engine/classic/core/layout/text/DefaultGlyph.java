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


package org.pentaho.reporting.engine.classic.core.layout.text;

import org.pentaho.reporting.libraries.fonts.text.Spacing;

/**
 * Creation-Date: 03.04.2007, 16:41:38
 *
 * @author Thomas Morgner
 */
public final class DefaultGlyph implements Glyph {
  private static final int[] EMPTY_EXTRA_CHARS = new int[0];

  private int codepoint;
  private int breakWeight;
  private int classification;
  private Spacing spacing;
  private int width;
  private int height;
  private int baseLine;
  private int kerning;
  private int[] extraChars;

  public DefaultGlyph( final int codepoint, final int breakWeight, final int classification, final Spacing spacing,
      final int width, final int height, final int baseLine, final int kerning, final int[] extraChars ) {

    // Log.debug ("Glyph: -" + ((char) (0xffff & codepoint)) + "- [" + baseLine + ", " + height + "]");
    if ( spacing == null ) {
      this.spacing = Spacing.EMPTY_SPACING;
    } else {
      this.spacing = spacing;
    }
    if ( extraChars == null ) {
      this.extraChars = DefaultGlyph.EMPTY_EXTRA_CHARS;
    } else if ( extraChars.length == 0 ) {
      this.extraChars = DefaultGlyph.EMPTY_EXTRA_CHARS;
    } else {
      this.extraChars = (int[]) extraChars.clone();
    }

    this.baseLine = baseLine;
    this.codepoint = codepoint;
    this.breakWeight = breakWeight;
    this.width = width;
    this.height = height;
    this.classification = classification;
    this.kerning = kerning;
  }

  public int getClassification() {
    return classification;
  }

  public int[] getExtraChars() {
    if ( extraChars.length == 0 ) {
      return DefaultGlyph.EMPTY_EXTRA_CHARS;
    }
    return (int[]) extraChars.clone();
  }

  public int getBaseLine() {
    return baseLine;
  }

  public int getCodepoint() {
    return codepoint;
  }

  public int getBreakWeight() {
    return breakWeight;
  }

  public Spacing getSpacing() {
    return spacing;
  }

  public int getWidth() {
    return width;
  }

  public int getHeight() {
    return height;
  }

  public int getKerning() {
    return kerning;
  }

  public boolean equals( final Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    final DefaultGlyph glyph = (DefaultGlyph) o;

    if ( breakWeight != glyph.breakWeight ) {
      return false;
    }
    if ( codepoint != glyph.codepoint ) {
      return false;
    }
    if ( height != glyph.height ) {
      return false;
    }
    if ( kerning != glyph.kerning ) {
      return false;
    }
    if ( width != glyph.width ) {
      return false;
    }
    if ( !spacing.equals( glyph.spacing ) ) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    int result = codepoint;
    result = 29 * result + breakWeight;
    result = 29 * result + spacing.hashCode();
    result = 29 * result + width;
    result = 29 * result + height;
    result = 29 * result + kerning;
    return result;
  }

  public String toString() {
    return getClass().getName() + "={codepoint='" + ( (char) ( codepoint & 0xffff ) ) + ", extra-chars="
        + extraChars.length + '}';
  }
}
