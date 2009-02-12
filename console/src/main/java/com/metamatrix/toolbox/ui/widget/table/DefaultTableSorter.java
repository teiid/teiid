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
import java.util.Iterator;
import java.util.List;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class DefaultTableSorter
implements TableSorter {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static final DefaultTableSorter INSTANCE = new DefaultTableSorter();

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public static DefaultTableSorter getInstance() {
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public int getInsertionIndex(final List rowData, /*final int modelRowIndex,*/ final List rows, final List sortedRowMap, final List sortedColumns,
                                 final TableComparator comparator, final int maximumRowIndex) {
        if (sortedColumns == null) {
            return maximumRowIndex;
        }
        int minRowNdx = 0;
        int maxRowNdx = maximumRowIndex;
        int midRowNdx = maxRowNdx / 2;
        List sortedRow;
        Iterator iterator;
        EnhancedTableColumn col = null;
        int modelColNdx;
        while (minRowNdx < maxRowNdx)
        {
            int sortedRowIndex = ((Integer) sortedRowMap.get(midRowNdx)).intValue();
            sortedRow = (List)rows.get(sortedRowIndex);
            iterator = sortedColumns.iterator();
            int comparison = 0;
            while (iterator.hasNext()) {
                col = (EnhancedTableColumn)iterator.next();
                modelColNdx = col.getModelIndex();
                comparison = comparator.compare(rowData.get(modelColNdx), sortedRow.get(modelColNdx), modelColNdx /*, modelRowIndex, sortedRowIndex*/);
                if (comparison != 0) {
                    break;
                }
            }
            if ((col.isSortedAscending()  &&  comparison < 0)  ||  (col.isSortedDescending()  &&  comparison > 0)) {
                maxRowNdx = midRowNdx;
            } else {
                minRowNdx = midRowNdx + 1;
            }
            midRowNdx = minRowNdx + (maxRowNdx - minRowNdx) / 2;
        }
        return midRowNdx;
    }
}
