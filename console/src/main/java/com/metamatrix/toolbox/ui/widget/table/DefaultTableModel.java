/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

/**
This is the default implementation of EnhancedTableModel used by a TableWidget when the TableModel is not passed in the
constructor.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTableModel extends javax.swing.table.DefaultTableModel
implements EnhancedTableModel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final int COLUMN_EDITABILITY = 0;
    private static final int CELL_EXCEPTIONS    = 1;

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private List colEditStatusList;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel() {
        super();
        initializeDefaultTableModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel(final Vector columnNames) {
        super(columnNames, 0);
        initializeDefaultTableModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel(final int rowCount, final int columnCount) {
        super(rowCount, columnCount);
        initializeDefaultTableModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel(final Vector columnNames, final int rowCount) {
        super(columnNames, rowCount);
        initializeDefaultTableModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel(final Vector data, final Vector columnNames) {
        super(data, columnNames);
        initializeDefaultTableModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableModel(final Object[][] data, final Object[] columnNames) {
        super(data, columnNames);
        initializeDefaultTableModel();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to add an entry to the column edit status list.
    @since 2.0
    */
    public void addColumn(final Object columnName, final Vector columnData) {
        super.addColumn(columnName, columnData);
        createColumnEditStatusList();
        addColumnEditStatus();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds an entry to the column edit status list.
    @since 2.0
    */
    protected void addColumnEditStatus() {
        // Create a 2-element column edit status entry.  The first entry contains the overall edit status of the column (initially
        // editable), and the second entry is a list of cell indexes that are exceptions to the overall status.
        final ArrayList colEditStatus = new ArrayList(2);
        colEditStatus.add(Boolean.TRUE);
        colEditStatus.add(new HashSet());
        colEditStatusList.add(colEditStatus);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates the column edit status list if necessary.
    @since 2.0
    */
    protected void createColumnEditStatusList() {
        if (colEditStatusList == null) {
            colEditStatusList = new ArrayList();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates column edit status list.
    @since 2.0
    */
    protected void initializeDefaultTableModel() {
        createColumnEditStatusList();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param rowIndex     The cell's row index
    @param columnIndex  The cell's column index
    @return True if the cell at the specified row and column index is editable.
    @since 2.0
    */
    public boolean isCellEditable(final int rowIndex, final int columnIndex) {
        final List colEditStatus = (List)colEditStatusList.get(columnIndex);
        final boolean isColEditable = ((Boolean)colEditStatus.get(COLUMN_EDITABILITY)).booleanValue();
        final boolean isCellException = ((Set)colEditStatus.get(CELL_EXCEPTIONS)).contains(new Integer(rowIndex));
        if ((isColEditable  &&  !isCellException)  ||
            (!isColEditable  &&  isCellException)) {
            return true;
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param columnIndex The column's index
    @return True if any cell in the specified column is editable
    @since 2.0
    */
    public boolean isColumnEditable(final int columnIndex) {
        final List colEditStatus = (List)colEditStatusList.get(columnIndex);
        if (((Boolean)colEditStatus.get(COLUMN_EDITABILITY)).booleanValue()  ||
            ((Set)colEditStatus.get(CELL_EXCEPTIONS)).size() > 0) {
            return true;
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if any cell within the model is editable.
    @since 2.0
    */
    public boolean isEditable() {
        for (int ndx = colEditStatusList.size();  --ndx >= 0;) {
            if (isColumnEditable(ndx)) {
                return true;
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param rowIndex The cell's row index
    @return True if any cell in the specified row is editable
    @since 2.0
    */
    public boolean isRowEditable(final int rowIndex) {
        for (int ndx = colEditStatusList.size();  --ndx >= 0;) {
            if (isCellEditable(rowIndex, ndx)) {
                return true;
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the specified cell to the specified editability.
    @param rowIndex     The cell's row index
    @param columnIndex  The cell's column index
    @param isEditable   True if the cell is to be set to editable
    @since 2.0
    */
    public void setCellEditable(final int rowIndex, final int columnIndex, final boolean isCellEditable) {
        final List colEditStatus = (List)colEditStatusList.get(columnIndex);
        final boolean isColEditable = ((Boolean)colEditStatus.get(COLUMN_EDITABILITY)).booleanValue();
        final Set cellEditStatusSet = (Set)colEditStatus.get(CELL_EXCEPTIONS);
        if (isColEditable == isCellEditable) {
            cellEditStatusSet.remove(new Integer(rowIndex));
        } else {
            cellEditStatusSet.add(new Integer(rowIndex));
            // To save memory, reverse column status if all cells are exceptions
            if (cellEditStatusSet.size() == getRowCount()) {
                setColumnEditable(columnIndex, !isColEditable);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the specified column to the specified editability.
    @param columnIndex The column's index
    @param isEditable  True if the column's cells are to be set to editable
    @since 2.0
    */
    public void setColumnEditable(final int columnIndex, final boolean isEditable) {
        final List colEditStatus = (List)colEditStatusList.get(columnIndex);
        colEditStatus.set(COLUMN_EDITABILITY, new Boolean(isEditable));
        ((Set)colEditStatus.get(CELL_EXCEPTIONS)).clear();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected void setColumnEditStatusList(final Vector columnNames) {
        if (columnNames != null) {
            for (int count = columnNames.size() - colEditStatusList.size();  --count >= 0;) {
                addColumnEditStatus();
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to add entries to the column edit status list if the specified number of columnNames is greater than the current
    number of column edit status entries.
    @since 2.0
    */
    public void setColumnIdentifiers(final Vector columnNames) {
        createColumnEditStatusList();
        colEditStatusList.clear();
        super.setColumnIdentifiers(columnNames);
        setColumnEditStatusList(columnNames);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force firing of deletion event before insertion event.
    @since 2.0
    */
    public void setDataVector(final Vector data, final Vector columnNames) {
        createColumnEditStatusList();
        colEditStatusList.clear();
        if (getDataVector() != null) {
            //setNumRows(0); Obsolete as of Java 2 platform v1.3. Please use setRowCount instead.
            setRowCount(0);
        }
        super.setDataVector(data, columnNames);
        setColumnEditStatusList(columnNames);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the model to the specified editability.
    @param isEditable True if the cells are to be set to editable
    @since 2.0
    */
    public void setEditable(final boolean isEditable) {
        for (int ndx = colEditStatusList.size();  --ndx >= 0;) {
            setColumnEditable(ndx, isEditable);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets all cells within the specified row to the specified editability.
    @param rowIndex     The row's index
    @param isEditable   True if the row's cells are to be set to editable
    @since 2.0
    */
    public void setRowEditable(final int rowIndex, final boolean isEditable) {
        for (int ndx = colEditStatusList.size();  --ndx >= 0;) {
            setCellEditable(rowIndex, ndx, isEditable);
        }
    }
    
    public void setValueAt(final Object value, final int rowIndex, final int columnIndex) {
        super.setValueAt(value, rowIndex, columnIndex);
    }
}
