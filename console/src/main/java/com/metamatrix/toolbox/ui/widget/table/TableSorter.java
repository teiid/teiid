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
import java.util.List;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public interface TableSorter {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * determine where the specified row of the table belongs given the specified parameters
     * @param rowDataVector the Vector of table row data currently being examined.
     * @param the row index of this row in the real TableModel, which will not be modified.
     * @param rows a List of table row Vectors that consists of the entire table.
     * @param sortedRowMap the current ordered list of model row index values
     * @since Golden Gate
     */
    int getInsertionIndex(List rowDataVector, 
                          //int modelRowIndex, 
                          List rows, 
                          List sortedRowMap, 
                          List sortedColumns, 
                          TableComparator comparator,
                          int maximumRowIndex);
}
