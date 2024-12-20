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


package org.pentaho.reporting.engine.classic.core.modules.misc.tablemodel;

import javax.swing.table.AbstractTableModel;

/**
 * <code>TableModel</code> used by the <code>CSVTableModelProducer</code> class. It has a feature which generates the
 * column name if it is not know.
 *
 * @author Mimil
 * @see this.getColumnName()
 */
public class CSVTableModel extends AbstractTableModel {
  private static final Object[][] EMPTY_DATA = new Object[0][0];
  private static final String[] EMPTY_NAMES = new String[0];

  private String[] columnNames;
  private int maxColumnCount;
  private Object[][] data;

  public CSVTableModel() {
    this.columnNames = EMPTY_NAMES;
    this.data = EMPTY_DATA;
  }

  public Object[][] getData() {
    return (Object[][]) data.clone();
  }

  public void setData( final Object[][] data ) {
    this.data = (Object[][]) data.clone();
  }

  public String[] getColumnNames() {
    return (String[]) columnNames.clone();
  }

  public void setColumnNames( final String[] columnNames ) {
    this.columnNames = (String[]) columnNames.clone();
  }

  /**
   * Counts columns of this <code>TableModel</code>.
   *
   * @return the column count
   */
  public int getColumnCount() {
    if ( this.columnNames != null ) {
      return columnNames.length;
    }

    return this.maxColumnCount;
  }

  /**
   * Counts rows of this <code>TableModel</code>.
   *
   * @return the row count
   */
  public int getRowCount() {
    return this.data.length;
  }

  /**
   * Gets the Object at specified row and column positions.
   *
   * @param rowIndex
   *          row index
   * @param columnIndex
   *          column index
   * @return The requested Object
   */
  public Object getValueAt( final int rowIndex, final int columnIndex ) {
    final Object[] line = this.data[rowIndex];

    if ( line.length < columnIndex ) {
      return null;
    } else {
      return line[columnIndex];
    }
  }

  /**
   * Sets the maximum column count if it is bigger than the current one.
   *
   * @param maxColumnCount
   */
  public void setMaxColumnCount( final int maxColumnCount ) {
    if ( this.maxColumnCount < maxColumnCount ) {
      this.maxColumnCount = maxColumnCount;
    }
  }

  public int getMaxColumnCount() {
    return maxColumnCount;
  }

  /**
   * Return the column name at a specified position.
   *
   * @param column
   *          column index
   * @return the column name
   */
  public String getColumnName( final int column ) {
    if ( this.columnNames != null ) {
      return this.columnNames[column];
    } else {
      if ( column >= this.maxColumnCount ) {
        throw new IllegalArgumentException( "Column (" + column + ") does not exist" ); //$NON-NLS-1$ //$NON-NLS-2$
      } else {
        return "COLUMN_" + column; //$NON-NLS-1$
      }
    }
  }
}
