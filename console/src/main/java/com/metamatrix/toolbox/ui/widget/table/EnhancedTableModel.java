/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.table;

// System imports
import java.util.Vector;

import javax.swing.table.TableModel;

/**
This is the type of TableModel required by a TableWidget.
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public interface EnhancedTableModel extends TableModel {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    Vector getDataVector();

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param columnIndex The column's index
    @return True if any cell in the specified column is editable
    @since Golden Gate
    */
    boolean isColumnEditable(int columnIndex);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if any cell within the model is editable.
    @since Golden Gate
    */
    boolean isEditable();

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param rowIndex The cell's row index
    @return True if any cell in the specified row is editable
    @since Golden Gate
    */
    boolean isRowEditable(int rowIndex);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the specified cell to the specified editability.
    @param rowIndex     The cell's row index
    @param columnIndex  The cell's column index
    @param isEditable   True if the cell is to be set to editable
    @since Golden Gate
    */
    void setCellEditable(int rowIndex, int columnIndex, boolean isCellEditable);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the specified column to the specified editability.
    @param columnIndex The column's index
    @param isEditable  True if the column's cells are to be set to editable
    @since Golden Gate
    */
    void setColumnEditable(int columnIndex, boolean isEditable);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    void setColumnIdentifiers(Vector columnNames);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the model to the specified editability.
    @param isEditable True if the cells are to be set to editable
    @since Golden Gate
    */
    void setEditable(boolean isEditable);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the specified row to the specified editability.
    @param rowIndex     The row's index
    @param isEditable   True if the row's cells are to be set to editable
    @since Golden Gate
    */
    void setRowEditable(int rowIndex, boolean isEditable);
}
