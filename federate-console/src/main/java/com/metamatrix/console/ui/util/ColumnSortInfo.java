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

package com.metamatrix.console.ui.util;

import java.util.Iterator;

import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class ColumnSortInfo {
	public static ColumnSortInfo[] getTableColumnSortInfo(TableWidget table) {
	    ColumnSortInfo[] csi;
	    int numSortColumns = table.getSortedColumnCount();
	    csi = new ColumnSortInfo[numSortColumns];
		java.util.List /*<EnhancedTableColumn>*/ sortedColumns =
				table.getSortedColumns();
	    if (numSortColumns == 1) {
	        EnhancedTableColumn col = (EnhancedTableColumn)sortedColumns.get(0);
	        String colName = (String)col.getIdentifier();
	        boolean ascending = col.isSortedAscending();
	        csi[0] = new ColumnSortInfo(colName, ascending);
	    } else if (numSortColumns > 1) {
	        Iterator it = sortedColumns.iterator();
	        while (it.hasNext()) {
	            EnhancedTableColumn col = (EnhancedTableColumn)it.next();
	     		int loc = col.getSortPriority() - 1;
	     		String colName = (String)col.getIdentifier();
	     		boolean ascending = col.isSortedAscending();
	     		csi[loc] = new ColumnSortInfo(colName, ascending);
	        }       
	    }
	    return csi;
	}
	
	public static void setColumnSortOrder(ColumnSortInfo[] csi, 
			TableWidget table) {
		boolean firstSortCol = true;
    	for (int i = 0; i < csi.length; i++) {
        	EnhancedTableColumn col = null;
        	//If the column does not exist in the table, this will throw an
        	//exception.  That's okay.
        	try {
            	col = (EnhancedTableColumn)table.getColumn(csi[i].getColHeader());
        	} catch (Exception ex) {
//System.err.println("exception occured:");
//ex.printStackTrace();
        	}
        	if (col != null) {
            	boolean addToExistingSortColumns = (!firstSortCol);
         		if (csi[i].isAscending()) {
//System.err.println("before call to setColumnSortedAscending(), col = " + 
// col.getIdentifier() + ", addToExistingSortColumns = " + addToExistingSortColumns +
// ", table contents:");
//System.err.println(StaticTableUtilities.tableContents(table));	         	    
					table.setColumnSortedAscending(col, addToExistingSortColumns);
//System.err.println("after call, table contents:");
//System.err.println(StaticTableUtilities.tableContents(table));					
				} else {
         	    	table.setColumnSortedDescending(col, addToExistingSortColumns);
         		}
         		firstSortCol = false;
        	}
        }
	}
	 
    private String colHeader;
    private boolean ascending;
    
    public ColumnSortInfo(String hdr, boolean asc) {
        super();
        colHeader = hdr;
        ascending = asc;
    }
    
    public String getColHeader() {
        return colHeader;
    }
    
    public boolean isAscending() {
        return ascending;
    }
}
