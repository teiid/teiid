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

package com.metamatrix.console.models;

import java.util.Collection;

import com.metamatrix.api.exception.ComponentNotFoundException;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.xa.TransactionID;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.transactions.TransactionTableModel;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.RuntimeExternalException;

import com.metamatrix.server.admin.api.TransactionAdminAPI;

/**
 * Extension of TimedManager to manage the Transactions tab.  It creates and
 * maintains the tab's table model, but has no reference to the tab itself.
 */
public class TransactionManager extends TimedManager {
    private TransactionAdminAPI transAPI;
    private TransactionTableModel tableModel;

// Constructors and initialization methods

    public TransactionManager(ConnectionInfo connection) {
        super(connection);
        super.init();
        transAPI = ModelManager.getTransactionAPI(getConnection());
        //Create the table model, initially empty.
        tableModel = new TransactionTableModel();
    }

    public TransactionTableModel getTableModel() {
        return tableModel;
    }

// Overridden methods

    public void refresh() {
        refreshTableModel();
    }

// Processing methods

    /**
     * Method to refresh the table model with current data.
     */
    public void refreshTableModel() {
        super.refresh(false);
        //Make API call to get all transactions
        Collection /*<ServerTransaction>*/ tx = null;
        try {
            tx = transAPI.getAllTransactions();
        } catch (ComponentNotFoundException ex) {
            //Exception should not occur.  Throw to caller as a
            //RuntimeExternalException.
            LogManager.logError(LogContexts.TRANSACTIONS, ex,
                    "Error retrieving list of transactions.  Transaction Service may not be running."); //$NON-NLS-1$
            throw new RuntimeExternalException(ex);
        } catch (Exception ex) {
            //Exception should not occur.  Throw to caller as a
            //RuntimeExternalException.
            LogManager.logError(LogContexts.TRANSACTIONS, ex,
                    "Error retrieving list of transactions."); //$NON-NLS-1$
            throw new RuntimeExternalException(ex);
        }
        //Repopulate the table model from the list of transactions returned.
        tableModel.resetFromTransactionsList(tx);
    }

    public TransactionID transactionIDForTransactionNum(Long transactionNum) {
        return tableModel.transactionIDForTransactionNum(transactionNum);
    }
}

