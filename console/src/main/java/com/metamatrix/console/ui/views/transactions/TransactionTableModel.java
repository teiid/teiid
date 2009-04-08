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

package com.metamatrix.console.ui.views.transactions;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import com.metamatrix.admin.api.objects.Transaction;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
/**
 * Extension to DefaultSortableTableModel to model the Transactions tab table.
 */
public class TransactionTableModel extends DefaultTableModel {
	
	public static final int TXN_ID_INDEX = 0;
	public static final int SESSION_ID_INDEX = 1;
	public static final int XID_INDEX = 3;
    //
    //  Static column information
    //
    public final static Class<?>[] COLUMN_CLASS = new Class[]{
    	String.class,
    	String.class,
    	String.class,
    	Object.class,
    	String.class,
    	Date.class
    	
    };
    public final static String[] COLUMN_HEADERS = new String[]{
    	"Transaction ID",
    	"Session ID",
    	"Scope",
    	"Xid",
    	"Status",
    	"Start Time",
    };
    
//
// Constructors
//
    public TransactionTableModel() {
        super();

        // Set columns for model
        super.setColumnIdentifiers(COLUMN_HEADERS);

        // Set to 0 rows
        super.setNumRows(0);
    }

//
// Overridden methods
//
    public Class getColumnClass(int columnIndex) {
    	return COLUMN_CLASS[columnIndex];
    }

//
// Processing methods
//
    /**
     * Method to repopulate the table model based on a Collection of
     * ServerTransaction objects.
     */
    public void resetFromTransactionsList(Collection<Transaction> txs) {
        int numRows = txs.size();
        
        // This panel needs to be removed, any transaction information provided
        // should be on the queries panel.
        
        //Store list's contents in a matrix-- one row per transaction.
        Object[][] data = new Object[numRows][COLUMN_HEADERS.length];
        Iterator<Transaction> it = txs.iterator();
        for (int row = 0; it.hasNext(); row++) {
        	Transaction txn = it.next();
            data[row][0] = txn.getIdentifier();
            data[row][1] = txn.getAssociatedSession();
            data[row][2] = txn.getScope();
            data[row][3] = txn.getXid() != null?new MMXid(txn.getXid()):null;
            data[row][4] = txn.getStatus();
            data[row][5] = txn.getCreated();
        }
        //Set model's data to this matrix.
        this.setRowCount(numRows);
        this.setDataVector(data, COLUMN_HEADERS);
        this.setEditable(false);
    }

}
