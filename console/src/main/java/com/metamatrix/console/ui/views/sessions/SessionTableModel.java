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

package com.metamatrix.console.ui.views.sessions;

import java.util.Date;
import java.util.Iterator;
import java.util.Vector;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public class SessionTableModel extends DefaultTableModel {

	public static final int USER_NAME_COLUMN_NUM = 0;
	public static final int APPLICATION_COLUMN_NUM = 1;
	public static final int SESSION_ID_COLUMN_NUM = 2;
	public static final int LOGGED_IN_COLUMN_NUM = 3;
	public static final int VDB_NAME_COLUMN_NUM = 4;
	public static final int VDB_VERSION_COLUMN_NUM = 5;
	public static final int LAST_PING_TIME = 6;
	public static final int STATE_COLUMN_NUM = 7;

	//Note-- positions must match USER_NAME_COLUMN_NUM, etc., below.
	public final static String LOGGED_IN_AT = ConsolePlugin.Util.getString("SessionTableModel.Logged_In_At_1"); //$NON-NLS-1$
	public static final String[] COLUMN_NAMES =
		new String[] {
			ConsolePlugin.Util.getString("SessionTableModel.User_Name_2"), //$NON-NLS-1$
			ConsolePlugin.Util.getString("SessionTableModel.Application_3"), //$NON-NLS-1$
			ConsolePlugin.Util.getString("SessionTableModel.ID_4"), //$NON-NLS-1$
			LOGGED_IN_AT,
			ConsolePlugin.Util.getString("SessionTableModel.VDB_Name_5"), //$NON-NLS-1$
			ConsolePlugin.Util.getString("SessionTableModel.VDB_Ver._6"), //$NON-NLS-1$
			ConsolePlugin.Util.getString("SessionTableModel.Last_Ping_Time_8"), //$NON-NLS-1$
			ConsolePlugin.Util.getString("SessionTableModel.State_9") }; //$NON-NLS-1$

	public static final int COLUMN_COUNT = COLUMN_NAMES.length;

	public SessionTableModel() {
		super(StaticUtilities.arrayToVector(COLUMN_NAMES));
	}

	public void setDataVector(Object[][] data) {
		setDataVector(StaticUtilities.doubleArrayToVector(data));
	}

	public void setDataVector(Vector data) {
		//BWP 01/27/04  Apparent difference between JDK 1.3.x and JDK 1.4.x on how
		//superclass's setDataVector() method works, so not calling it.  Symptom
		//was: not seeing any rows in sessions table.  Replacing with code to 
		//set row count to 0 then add new rows individually.
		
		//super.setDataVector(data, getColumnNamesAsVector());
		setRowCount(0);
		Iterator it = data.iterator();
		while (it.hasNext()) {
 			Vector dataVec = (Vector)it.next();
 			addRow(dataVec);
		}
	}

	public Class getColumnClass(int columnIndex) {
		Class cls;
		if (columnIndex == SESSION_ID_COLUMN_NUM) {
			cls = Long.class;
		} else if (columnIndex == VDB_VERSION_COLUMN_NUM) {
			cls = String.class;
		} else if (columnIndex == LOGGED_IN_COLUMN_NUM) {
			cls = Date.class;
		} else {
			cls = super.getColumnClass(columnIndex);
		}
		return cls;
	}
}
