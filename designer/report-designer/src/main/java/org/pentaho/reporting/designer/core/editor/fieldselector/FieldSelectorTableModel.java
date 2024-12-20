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


package org.pentaho.reporting.designer.core.editor.fieldselector;

import org.pentaho.reporting.designer.core.Messages;
import org.pentaho.reporting.designer.core.editor.structuretree.ReportFieldNode;

import javax.swing.table.AbstractTableModel;

/**
 * Todo: Document Me
 *
 * @author Thomas Morgner
 */
public class FieldSelectorTableModel extends AbstractTableModel {
  public static final ReportFieldNode[] EMPTY_NODES = new ReportFieldNode[ 0 ];
  private ReportFieldNode[] columns;

  public FieldSelectorTableModel() {
    columns = EMPTY_NODES;
  }

  public void setDataSchema( final ReportFieldNode[] columnNames ) {
    if ( columnNames == null ) {
      throw new NullPointerException();
    }
    this.columns = columnNames.clone();
    fireTableDataChanged();
  }

  /**
   * Returns the number of rows in the model. A <code>JTable</code> uses this method to determine how many rows it
   * should display.  This method should be quick, as it is called frequently during rendering.
   *
   * @return the number of rows in the model
   * @see #getColumnCount
   */
  public int getRowCount() {
    return columns.length;
  }

  /**
   * Returns the number of columns in the model. A <code>JTable</code> uses this method to determine how many columns it
   * should create and display by default.
   *
   * @return the number of columns in the model
   * @see #getRowCount
   */
  public int getColumnCount() {
    return 1;
  }

  /**
   * Returns a default name for the column using spreadsheet conventions: A, B, C, ... Z, AA, AB, etc.  If
   * <code>column</code> cannot be found, returns an empty string.
   *
   * @param column the column being queried
   * @return a string containing the default name of <code>column</code>
   */
  public String getColumnName( final int column ) {
    switch( column ) {
      case 0:
        return Messages.getString( "FieldSelectorTableModel.Field" );
      default:
        throw new IndexOutOfBoundsException();
    }
  }

  public Class getColumnClass( final int column ) {
    switch( column ) {
      case 0:
        return ReportFieldNode.class;
      default:
        throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Returns the value for the cell at <code>columnIndex</code> and <code>rowIndex</code>.
   *
   * @param rowIndex    the row whose value is to be queried
   * @param columnIndex the column whose value is to be queried
   * @return the value Object at the specified cell
   */
  public Object getValueAt( final int rowIndex, final int columnIndex ) {
    if ( columnIndex == 0 ) {
      return columns[ rowIndex ];
    }
    throw new IndexOutOfBoundsException();
  }

  public String getFieldName( final int selectedRow ) {
    return columns[ selectedRow ].getFieldName();
  }
}
