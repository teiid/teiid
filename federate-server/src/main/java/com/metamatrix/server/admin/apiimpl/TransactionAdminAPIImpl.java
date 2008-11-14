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

package com.metamatrix.server.admin.apiimpl;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;

import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.xa.TransactionID;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.platform.admin.apiimpl.AdminAPIHelper;
import com.metamatrix.platform.admin.apiimpl.SubSystemAdminAPIImpl;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.server.admin.api.TransactionAdminAPI;

public class TransactionAdminAPIImpl extends SubSystemAdminAPIImpl implements TransactionAdminAPI {

    //Transaction service proxy
    private static TransactionAdminAPI transactionAdminAPI;

    /**
     * ctor
     */
    private TransactionAdminAPIImpl() throws MetaMatrixComponentException {
    }
    
    public synchronized static TransactionAdminAPI getInstance() throws MetaMatrixComponentException {
        if (transactionAdminAPI == null) {
            transactionAdminAPI = new TransactionAdminAPIImpl();
        }
        return transactionAdminAPI;
    }

    /**
     * Return all transactions that are in the system.
     *
     * @return a collection of <code>ServerTransaction</code> objects.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     * @throws RemoteException if there is a communication exception.
     */
    public synchronized Collection getAllTransactions()
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        // TODO: This will eventually be replaced
        return Collections.EMPTY_LIST;
    }


    /**
     * Terminate a transactions.
     * If status == STATUS_ACTIVE or STATUS_MARKED_ROLLBACK, rollback transaction.
     * Else, set status to STATUS_ROLLEDBACK.
     *
     * @param transactionID ID of the transaction to be rolledback.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws com.metamatrix.common.xa.InvalidTransactionIDException if the Transaction does not exist.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     * @throws RemoteException if there is a communication exception.
     */
    public synchronized void terminateTransaction(TransactionID transactionID)
        throws AuthorizationException, InvalidSessionException, XATransactionException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(callerToken, AdminRoles.RoleName.ADMIN_PRODUCT, "TransactionAdminAPIImpl.terminateTransaction(" + transactionID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        // TODO: This will eventually be replaced
//        transAdmin.terminateTransaction(transactionID);
    }

    /**
     * Terminate a collection of transactions.
     * If status == STATUS_ACTIVE or STATUS_MARKED_ROLLBACK, rollback transaction.
     * Else, set status to STATUS_ROLLEDBACK.
     *
     * @param transactionIDs a collection of transaction IDs indentifying those transactions to rollback.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws com.metamatrix.common.xa.InvalidTransactionIDException if the Transaction does not exist.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     * @throws RemoteException if there is a communication exception.
     */
    //public void terminateTransactions(List transactionIDs)
        //throws AuthorizationException, InvalidSessionException, InvalidTransactionIDException, MetaMatrixComponentException, RemoteException {

        // Validate caller's session
        //SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        // AdminAPIHelper.checkForRequiredRole(callerToken, UserRoles.RoleName.ADMIN_METAMATRIX);

        //transAdmin.terminateTransactions(transactionIDs);
    //}

    /**
     * Terminate all transactions for the user session.
     * If status == STATUS_ACTIVE or STATUS_MARKED_ROLLBACK, rollback transaction.
     * Else, set status to STATUS_ROLLEDBACK.
     *
     * @param userSessionID the primary identifier for the user account.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     * @throws RemoteException if there is a communication exception.
     */
    public synchronized void terminateAllTransactions(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MultipleException, MetaMatrixComponentException {

        // Validate caller's session
        SessionToken callerToken = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(callerToken, AdminRoles.RoleName.ADMIN_PRODUCT, "TransactionAdminAPIImpl.terminateAllTransactions(" + userSessionID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        // TODO: This will eventually be replaced
//        transAdmin.terminateTransactionsForSession(callerToken);
    }

}

