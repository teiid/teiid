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

package com.metamatrix.console.ui.views.transactions;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.server.transaction.ServerTransaction;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.common.xa.TransactionID;
/**
 * Extension to DefaultSortableTableModel to model the Transactions tab table.
 */
public class TransactionTableModel extends DefaultTableModel {
    //
    //  Static column information
    //
    public final static int NUM_COLUMNS = 9;
    public final static int TRANSACTION_ID_COL = 0;
    public final static int SESSION_ID_COL = 1;
    public final static int STATUS_COL = 2;
    public final static int START_TIME_COL = 3;
    public final static int END_TIME_COL = 4;
    public final static int CONNECTOR_ID_COL = 5;
    public final static int PROCESSOR_ID_COL = 6;
    public final static int DATABASE_COL = 7;
    public final static int REQUEST_ID_COL = 8;
    public final static String TRANSACTION_ID_HDR = "Transaction ID";
    public final static String SESSION_ID_HDR = "Session ID";
    public final static String STATUS_HDR = "Status";
    public final static String START_TIME_HDR = "Start Time";
    public final static String END_TIME_HDR = "End Time";
    public final static String CONNECTOR_ID_HDR = "Connector ID";
    public final static String PROCESSOR_ID_HDR = "Processor ID";
    public final static String DATABASE_HDR = "Model";
    public final static String REQUEST_ID_HDR = "Request ID";

    private Map /*<Long (transaction num.) to TransactionID>*/ transMap;
        //Transaction-number-to-transaction-ID map of the model's current
        //contents.  This map is accurate only if the model is populated
        //exclusively through use of the resetFromTransactionsList() method.
    private String[] colHdrs;

//
// Constructors
//
    public TransactionTableModel() {
        super();

        // Set columns for model
        colHdrs = new String[NUM_COLUMNS];
        colHdrs[TRANSACTION_ID_COL] = TRANSACTION_ID_HDR;
        colHdrs[SESSION_ID_COL] = SESSION_ID_HDR;
        colHdrs[STATUS_COL] = STATUS_HDR;
        colHdrs[START_TIME_COL] = START_TIME_HDR;
        colHdrs[END_TIME_COL] = END_TIME_HDR;
        colHdrs[CONNECTOR_ID_COL] = CONNECTOR_ID_HDR;
        colHdrs[PROCESSOR_ID_COL] = PROCESSOR_ID_HDR;
        colHdrs[DATABASE_COL] = DATABASE_HDR;
        colHdrs[REQUEST_ID_COL] = REQUEST_ID_HDR;
        super.setColumnIdentifiers(colHdrs);

        // Set to 0 rows
        super.setNumRows(0);
    }

//
// Overridden methods
//
    public Class getColumnClass(int columnIndex) {
        // Return appropriate column class based on the column index
        Class cls;
        if ((columnIndex == SESSION_ID_COL) || (columnIndex == PROCESSOR_ID_COL)
                || (columnIndex == REQUEST_ID_COL) || (columnIndex ==
                TRANSACTION_ID_COL)) {
            cls = Long.class;
        } else if ((columnIndex == START_TIME_COL) || (columnIndex == END_TIME_COL)) {
            cls = Date.class;
        } else {
            cls = String.class;
        }
        return cls;
    }

//
// Processing methods
//
    /**
     * Method to repopulate the table model based on a Collection of
     * ServerTransaction objects.
     */
    public void resetFromTransactionsList(Collection /*<ServerTransaction>*/ tx) {
        transMap = new HashMap();
        int numRows = tx.size();

        //Store list's contents in a matrix-- one row per transaction.
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        Object[][] data = new Object[numRows][NUM_COLUMNS];
        Iterator it = tx.iterator();
        for (int row = 0; it.hasNext(); row++) {
            ServerTransaction st = (ServerTransaction)it.next();

            //getDisplayString() for TransactionID is guaranteed to return a
            //numeric string.
            data[row][TRANSACTION_ID_COL] = new Long(st.getTransactionID().getID());
            transMap.put(data[row][TRANSACTION_ID_COL], st.getTransactionID());
            data[row][SESSION_ID_COL] = st.getSessionToken().getSessionIDValue();
            data[row][STATUS_COL] = st.getStatusString();

            //Form new Date objects for begin and end time, rather than using
            //the objects returned by ServerTransaction.  This is so we can
            //ensure that we have objects whose toString() method displays both
            //date and time.  toString() for java.sql.Date displays date only.
            Date beginTime = st.getBeginTime();
            if (beginTime == null) {
                data[row][START_TIME_COL] = null;
            } else {
                data[row][START_TIME_COL] = formatter.format(new Date(beginTime.getTime()));
            }
            Date endTime = st.getEndTime();
            if (endTime == null) {
                data[row][END_TIME_COL] = null;
            } else {
                data[row][END_TIME_COL] = formatter.format(new Date(endTime.getTime()));
            }
            data[row][CONNECTOR_ID_COL] = null;
            data[row][PROCESSOR_ID_COL] = null;            
            data[row][DATABASE_COL] = st.getDatabase();
//            if (st.getRequestID() == null) {
//                data[row][REQUEST_ID_COL] = null;
//            } else {
                data[row][REQUEST_ID_COL] = new Long(st.getRequestID());
//            }
        }
        //Set model's data to this matrix.
        this.setNumRows(0);
        this.setDataVector(data, colHdrs);
    }

    /**
     * Method to return a TransactionID contained within the model, given its
     * transaction number.  This method is accurate only if the model has been
     * populated exclusively through use of the resetFromTransactionsList()
     * method.
     */
    public TransactionID transactionIDForTransactionNum(Long transactionNum) {
        //transMap is a transaction-number-to-transaction-ID map for the model's
        //current contents.  See if it contains the given transaction number.
        TransactionID id = null;
        if (transMap != null) {
            id = (TransactionID)transMap.get(transactionNum);
        }
        return id;
    }
}
