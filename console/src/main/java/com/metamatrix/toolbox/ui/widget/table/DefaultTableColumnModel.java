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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.swing.event.TableColumnModelEvent;
import javax.swing.table.TableColumn;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTableColumnModel extends javax.swing.table.DefaultTableColumnModel
implements EnhancedTableColumnModel {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private List cols = new ArrayList();
    private List hiddenCols = null;
    private List sortedCols = null;
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addColumn(final TableColumn column) {
        addColumn(column, getColumnCount());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addColumn(final TableColumn column, final int index) {
        if (!(column instanceof EnhancedTableColumn)) {
            throw new IllegalArgumentException("Column parameter must be an instance of EnhancedTableColumn");
        }
        tableColumns.add(index, column);
        cols.add(index, column);
        column.addPropertyChangeListener(this);
        recalcWidthCache();
        fireColumnAdded(new TableColumnModelEvent(this, 0, index));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addColumnSortListener(final TableColumnSortListener listener) {
        listenerList.add(TableColumnSortListener.class, listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void fireColumnSorted() {
        final Object[] listeners = listenerList.getListenerList();
        for (int ndx = listeners.length - 2 ;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == TableColumnSortListener.class) {
                ((TableColumnSortListener)listeners[ndx + 1]).columnSorted();
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getColumnIndex(final TableColumn column) {
        return tableColumns.indexOf(column);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getHiddenAndShownColumnCount() {
        return cols.size();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getHiddenAndShownColumns() {
        return Collections.unmodifiableList(cols);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getHiddenColumnCount() {
        if (hiddenCols == null) {
            return 0;
        }
        return hiddenCols.size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getHiddenColumns() {
        if (hiddenCols == null) {
            return null;
        }
        return Collections.unmodifiableList(new ArrayList(hiddenCols));
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableColumn getHiddenOrShownColumn(final int index) {
        return (TableColumn)cols.get(index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getHiddenOrShownColumnIndex(final Object columnID) {
        final Iterator iterator = cols.iterator();
        for (int ndx = 0;  iterator.hasNext();  ++ndx) {
            if (((TableColumn)iterator.next()).getIdentifier().equals(columnID)) {
                return ndx;
            }
        }
        return -1;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getSortedColumnCount() {
        if (sortedCols == null) {
            return 0;
        }
        return sortedCols.size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getSortedColumns() {
        if (sortedCols == null) {
            return null;
        }
        return Collections.unmodifiableList(new ArrayList(sortedCols));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isColumnHidden(final TableColumn column) {
        if (hiddenCols == null) {
            return false;
        }
        return hiddenCols.contains(column);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void moveColumn(final int fromIndex, final int toIndex) {
        cols.add(getHiddenOrShownColumnIndex(getColumn(toIndex).getIdentifier()),
                 cols.remove(getHiddenOrShownColumnIndex(getColumn(fromIndex).getIdentifier())));
        super.moveColumn(fromIndex, toIndex);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void moveHiddenOrShownColumn(final int fromIndex, final int toIndex) {
        if (fromIndex == toIndex) {
            return;
        }
        if (!isColumnHidden((TableColumn)cols.get(fromIndex))) {
            int toNdx = toIndex;
            while (toNdx < cols.size()  &&  isColumnHidden((TableColumn)cols.get(toNdx))) {
                ++toNdx;
            }
            if (toNdx == cols.size()) {
                super.moveColumn(getColumnIndex(getHiddenOrShownColumn(fromIndex).getIdentifier()), getColumnCount());
            } else {
                super.moveColumn(getColumnIndex(getHiddenOrShownColumn(fromIndex).getIdentifier()),
                                 getColumnIndex(getHiddenOrShownColumn(toNdx).getIdentifier()));
            }
        }
        cols.add(toIndex, cols.remove(fromIndex));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeColumn(final TableColumn column) {
        super.removeColumn(column);
        cols.remove(column);
        if (hiddenCols != null) {
            hiddenCols.remove(column);
            if (hiddenCols.size() == 0) {
                hiddenCols = null;
            }
        }
        if (sortedCols != null) {
            sortedCols.remove(column);
            if (sortedCols.size() == 0) {
                sortedCols = null;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeColumnSortListener(final TableColumnSortListener listener) {
        listenerList.remove(TableColumnSortListener.class, listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnHidden(final TableColumn column, final boolean isHidden) {
        if (isHidden) {
            // Create hidden column list if necessary
            if (hiddenCols == null) {
                hiddenCols = new ArrayList();
            }
            // Return if column already hidden
            if (hiddenCols.contains(column)) {
                return;
            }
            // Save column in hidden column list
            hiddenCols.add(column);
            // Remove column from model
            super.removeColumn(column);
        } else {
            // Return if no hidden columns
            if (hiddenCols == null) {
                return;
            }
            // Remove column from hidden column list
            if (!hiddenCols.remove(column)) {
                return;
            }
            // Re-add column to model
            Object col = null;
            final ListIterator iterator = cols.listIterator(cols.indexOf(column) + 1);
            while (iterator.hasNext()) {
                if (!hiddenCols.contains(iterator.next())) {
                    col = iterator.previous();
                    break;
                }
            }
            if (col == null) {
                super.addColumn(column);
            } else {
                final int ndx = tableColumns.indexOf(col);
                tableColumns.add(ndx, column);
                column.addPropertyChangeListener(this);
                recalcWidthCache();
                fireColumnAdded(new TableColumnModelEvent(this, 0, ndx));
            }
            // Allow hidden column list to be garbage collected if empty
            if (hiddenCols.size() == 0) {
                hiddenCols = null;
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnNotSorted(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        setColumnSortStatus(column, false, column.isSortedAscending(), isCurrentColumnSortOrderKept);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedAscending(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        setColumnSortStatus(column, true, true, isCurrentColumnSortOrderKept);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedDescending(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        setColumnSortStatus(column, true, false, isCurrentColumnSortOrderKept);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void setColumnSortStatus(final EnhancedTableColumn column, final boolean isSorted, final boolean isSortedAscending,
                                       final boolean isCurrentColumnSortOrderKept) {
        setColumnSortStatus(column, isSorted, isSortedAscending, isCurrentColumnSortOrderKept, true);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Added fireSortEvent flag so that bulk operations can fire only one Sort Event to listeners 
     * @since 3.1
     */
    protected void setColumnSortStatus(final EnhancedTableColumn column, final boolean isSorted, final boolean isSortedAscending,
                                       final boolean isCurrentColumnSortOrderKept, boolean fireSortEvent) {

        final int oldSortPriority = column.getSortPriority();
        final boolean oldIsSorted = column.isSorted();
        final boolean oldIsSortedAscending = column.isSortedAscending();
        if (!isCurrentColumnSortOrderKept) {
            setColumnsNotSorted();
        }
        if (isSorted) {
            if (isSortedAscending) {
                column.setSortedAscending();
            } else {
                column.setSortedDescending();
            }
            if (!oldIsSorted  ||  !isCurrentColumnSortOrderKept) {
                if (sortedCols == null) {
                    sortedCols = new ArrayList();
                }
                sortedCols.add(column);
                column.setSortPriority(sortedCols.size());
                if (fireSortEvent) {
                    fireColumnSorted();
                }
            } else if (oldIsSortedAscending != isSortedAscending) {
                if (fireSortEvent) {
                    fireColumnSorted();
                }
            }
        } else {
            column.setNotSorted();
            if (oldIsSorted &&  isCurrentColumnSortOrderKept) {
                sortedCols.remove(column);
                if (sortedCols.size() == 0) {
                    sortedCols = null;
                } else {
                    // Descrement sort priorities of columns with priorities greater than this column's
                    final Iterator iterator = sortedCols.iterator();
                    EnhancedTableColumn col;
                    int priority;
                    while (iterator.hasNext()) {
                        col = (EnhancedTableColumn)iterator.next();
                        priority = col.getSortPriority();
                        if (priority > oldSortPriority) {
                            col.setSortPriority(priority - 1);
                        }
                    }
                }
            }
            if ( oldIsSorted && fireSortEvent ) {
                fireColumnSorted();
            }
                
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnsHidden(final boolean isHidden) {
        if (isHidden) {
            while (getColumnCount() > 0) {
                setColumnHidden(getColumn(0), true);
            }
        } else {
            while (hiddenCols != null) {
                setColumnHidden((TableColumn)hiddenCols.get(0), false);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnsNotSorted() {
        if (sortedCols == null) {
            return;
        }
        final Iterator iterator = sortedCols.iterator();
        while (iterator.hasNext()) {
            ((EnhancedTableColumn)iterator.next()).setNotSorted();
        }
        sortedCols = null;
    }
    
	/**
     * Obtain the TableColumn from this class given the index returned from
     * convertColumnToModelIndex.  Indexes the "cols" vector in this class, 
     * which may contain hidden columns, instead of the "tableColumns" vector 
     * in the superclass.
     * @since 3.1
	 * @see javax.swing.table.TableColumnModel#getColumn(int)
	 */
	public TableColumn getColumnFromModelIndex(int modelIndex) {
		return (TableColumn) cols.get(modelIndex);
	}

}
