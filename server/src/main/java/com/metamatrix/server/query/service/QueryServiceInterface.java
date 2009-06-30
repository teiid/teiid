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

package com.metamatrix.server.query.service;

import java.util.Collection;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Transaction;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceInterface;

public interface QueryServiceInterface extends ServiceInterface, CacheAdmin {
    public static final String SERVICE_NAME = "QueryService"; //$NON-NLS-1$
    
    //=========================================================================
    // Methods to clear cache associated with the prepared statements
    //=========================================================================
    public void clearCache(SessionToken sessionToken)
        throws ComponentNotFoundException;

    //=========================================================================
    // Methods to get queries and query status
    //=========================================================================
    
    public Collection getAllQueries();

    //=========================================================================
    // Methods to cancel running queries and cursors associated with them
    //=========================================================================
    public void cancelQueries(SessionToken sessionToken, boolean shouldRollback)
        throws InvalidRequestIDException, MetaMatrixComponentException;
        
    public void cancelQuery(RequestID requestID, boolean shouldRollback)
        throws InvalidRequestIDException, MetaMatrixComponentException;
    
    public void cancelQuery(AtomicRequestID ari)
    throws InvalidRequestIDException, MetaMatrixComponentException;   
    
    Collection<Transaction> getTransactions();
    
    void terminateTransaction(Xid transactionId) throws AdminException;
    
    void terminateTransaction(String transactionId, String sessionId) throws AdminException;

}
