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

package com.metamatrix.console.util;

import java.util.Iterator;
import java.util.Vector;

import javax.swing.JTable;
import javax.swing.ListSelectionModel;

public class StaticTableUtilities {

    /**
     * Effectively save the currently selected rows in a table, by saving the element in each 
     * selected row
     * for a given column.  This is an index column whose elements must uniquely identify the
     * row.
     *
     * @param table     The JTable being operated on
     * @param indexCol  A table column whose elements will uniquely identify a row of the table
     * @return Object[] The array of items in column indexCol of each selected row
     */
    public static Object[] saveSelections(JTable table, int indexCol) {
        int numRows = table.getModel().getRowCount();
        ListSelectionModel lsm = table.getSelectionModel();
        Vector v = new Vector();
        for (int i = 0; i < numRows; i++) {
            if (lsm.isSelectedIndex(i)) {
                v.add(table.getModel().getValueAt(i, indexCol));
            }
        }
        Object[] sel = new Object[v.size()];
        Iterator it = v.iterator();
        int i = 0;
        while (it.hasNext()) {
            sel[i] = it.next();
            i++;
        }
        return sel;
    }

    /**
     * Corresponding method to saveSelections().  All rows in the table whose object at
     * indexCol matches any item in selections will be selected.
     *
     * @param table  The JTable being operated on
     * @param indexCol The column to look in JTable for a match against selections
     * @param selections The set of objects whose rows are to be selected
     */
    public static void restoreSelections(JTable table, int indexCol, Object[] selections) {
        //We will go through the table row by row.  For each row, if the item at indexCol 
        //matches any item in selections, then we will select that row.
        table.clearSelection();
        int numRows = table.getModel().getRowCount();
        for (int i = 0; i < numRows; i++) {
            Object current = table.getModel().getValueAt(i, indexCol);
            boolean matchFound = false;
            int j = 0;
            while ((!matchFound) && (j < selections.length)) {
                if (selections[j].equals(current)) {
                    matchFound = true;
                } else {
                    j++;
                }
            }
            if (matchFound) {
                table.getSelectionModel().addSelectionInterval(i, i);
            }
        }
    }
    
    public static int getColumnNumForTableColumn(JTable table, 
    		String columnName) {
		int numColumns = table.getColumnCount();
		int matchCol = -1;
		int col = 0;
		while ((matchCol < 0) && (col < numColumns)) {
			String curColumnName = table.getColumnName(col);
			if (curColumnName.equals(columnName)) {
			    matchCol = col;
			} else {
			    col++;
			}
		}
		return matchCol;
    }
    
    public static String tableContents(JTable table) {
        int numColumns = table.getColumnCount();
        int numRows = table.getRowCount();
        StringBuffer buf = new StringBuffer(10 * numRows * numColumns);
        buf.append("columns:");
        for (int j = 0; j < numColumns; j++) {
            if (j > 0) {
                buf.append(',');
            }
            String colName = table.getColumnName(j);
            buf.append(" " + colName);
        }
		for (int i = 0; i < numRows; i++) {
		    buf.append('\n');
		    buf.append("row " + i + ":");
		    for (int j = 0; j < numColumns; j++) {
                if (j > 0) {
                    buf.append(',');
                }
		        Object value = table.getModel().getValueAt(i, j);
		        if (value == null) {
		            buf.append(" <null>");
		        } else {
		            buf.append(" " + value.toString());
		        }
		   	}
		}
		String str = buf.toString();
		return str;
    }        
}
