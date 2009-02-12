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

package com.metamatrix.console.ui.views.queries;

import java.util.Iterator;
import java.util.Vector;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public class QueryTableModel extends DefaultTableModel {
    public final static int NUM_COLUMNS = 4;
	public final static int REQUEST_ID_COL = 0;
    public final static String REQUEST_ID_HDR =
            ConsolePlugin.Util.getString("QueryTableModel.Request_ID"); //$NON-NLS-1$
	public final static int USER_COL = 1;
    public final static String USER_HDR =
            ConsolePlugin.Util.getString("QueryTableModel.User_Name"); //$NON-NLS-1$
	public final static int SESSION_ID_COL = 2;
    public final static String SESSION_HDR =
            ConsolePlugin.Util.getString("QueryTableModel.Session_ID"); //$NON-NLS-1$
    public final static int CONNECTOR_BINDING_COL = 3;
	public final static String CONNECTOR_BINDING_HDR =
            ConsolePlugin.Util.getString("QueryTableModel.Connector_Binding"); //$NON-NLS-1$
    public final static String TIME_SUBMITTED_HDR = 
            ConsolePlugin.Util.getString("QueryTableModel.Submitted"); //$NON-NLS-1$
	public final static String TRANSACTION_ID_HDR =
            ConsolePlugin.Util.getString("QueryTableModel.Transaction_ID"); //$NON-NLS-1$

	private int[] colOrder = {0, 1, 2, 3};

	//Must be in the same order as above:
	public static final String[] COLUMN_NAMES = new String[] {
            REQUEST_ID_HDR,
            USER_HDR,
            SESSION_HDR,
            CONNECTOR_BINDING_HDR};

	public QueryTableModel() {
		super(StaticUtilities.arrayToVector(COLUMN_NAMES), 0);
	}

	public int getRequestIdColumn() {
		int col = -1;
		for (int i = 0; i < colOrder.length; i++) {
			if (colOrder[i] == REQUEST_ID_COL) {
				col = i;
			}
		}
		return col;
	}

	public void setRows(Vector data) {
        removeRows();
		Iterator it = data.iterator();
        while (it.hasNext()) {
            Vector vec = (Vector)it.next();
            super.addRow(vec);
        }
    }

    private void removeRows() {
        int numRows = getRowCount();
        for (int i = numRows - 1; i >= 0; i--) {
            super.removeRow(i);
        }
    }
}
