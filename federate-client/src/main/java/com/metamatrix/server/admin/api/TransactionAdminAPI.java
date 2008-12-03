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

package com.metamatrix.server.admin.api;

import java.util.Collection;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.xa.TransactionID;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.platform.admin.api.SubSystemAdminAPI;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public interface TransactionAdminAPI extends SubSystemAdminAPI {

    /**
     * Return all transactions that are in the system.
     *
     * @return a collection of <code>ServerTransaction</code> objects.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    Collection getAllTransactions()
        throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException;

    /**
     * Terminate a transaction.
     * If status == STATUS_ACTIVE or STATUS_MARKED_ROLLBACK, rollback transaction.
     * Else, set status to STATUS_ROLLEDBACK.
     *
     * @param transactionID ID of the transaction to be rolledback.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws com.metamatrix.common.xa.InvalidTransactionIDException if the Transaction does not exist.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    void terminateTransaction(TransactionID transactionID)
        throws AuthorizationException, InvalidSessionException, XATransactionException, MetaMatrixComponentException ;

    /**
     * Terminate all transactions for the user session.
     * If status == STATUS_ACTIVE or STATUS_MARKED_ROLLBACK, rollback transaction.
     * Else, set status to STATUS_ROLLEDBACK.
     *
     * @param userSessionID the primary identifier for the user account.
	 * @throws AuthorizationException if caller is not authorized to perform this method.
	 * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    void terminateAllTransactions(MetaMatrixSessionID userSessionID)
        throws AuthorizationException, InvalidSessionException, MultipleException, MetaMatrixComponentException ;

}

