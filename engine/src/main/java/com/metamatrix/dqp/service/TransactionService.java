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

/*
 */
package com.metamatrix.dqp.service;

import java.util.Collection;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Transaction;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.dqp.internal.transaction.TransactionProvider.XAConnectionSource;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

/**
 */
public interface TransactionService extends ApplicationService {
	public static final String TRANSACTIONS_ENABLED = "xa.enabled"; //$NON-NLS-1$
    public static final String MAX_TIMEOUT = "xa.max_timeout"; //$NON-NLS-1$
    public static final String TXN_STORE_DIR = "xa.txnstore_dir"; //$NON-NLS-1$
    public static final String TXN_STATUS_PORT = "xa.txnstatus_port"; //$NON-NLS-1$
    public static final String TXN_ENABLE_RECOVERY = "xa.enable_recovery"; //$NON-NLS-1$
    
    
    public static final String PROCESSNAME = DQPEmbeddedProperties.PROCESSNAME;
    
    public static final String DEFAULT_TXN_MGR_LOG_DIR = "txnlog"; //$NON-NLS-1$
    public static final String DEFAULT_TXN_TIMEOUT = "120"; //$NON-NLS-1$ //2 mins
    public static final String DEFAULT_TXN_STATUS_PORT = "0"; //$NON-NLS-1$

    // processor level methods
    TransactionContext start(TransactionContext context) throws XATransactionException, SystemException;

    TransactionContext commit(TransactionContext context) throws XATransactionException, SystemException;

    TransactionContext rollback(TransactionContext context) throws XATransactionException, SystemException;

    TransactionContext getOrCreateTransactionContext(String threadId);

    // local transaction
    TransactionContext begin(String threadId) throws XATransactionException, SystemException;

    void commit(String threadId) throws XATransactionException, SystemException;

    void rollback(String threadId) throws XATransactionException, SystemException;

    // connector worker
    TransactionContext delist(TransactionContext context,
                              XAResource resource,
                              int flags) throws XATransactionException;

    TransactionContext enlist(TransactionContext context,
                              XAResource resource) throws XATransactionException;
    
    void cancelTransactions(String threadId, boolean requestOnly) throws InvalidTransactionException, SystemException;

    
    // recovery
    void registerRecoverySource(String name, XAConnectionSource resource);
    
    void removeRecoverySource(String name);  
    
    int prepare(final String threadId,
            MMXid xid) throws XATransactionException;

	void commit(final String threadId,
	                   MMXid xid,
	                   boolean onePhase) throws XATransactionException;
	
	void rollback(final String threadId,
	                     MMXid xid) throws XATransactionException;
	
	Xid[] recover(int flag) throws XATransactionException;
	
	void forget(final String threadId,
	            MMXid xid) throws XATransactionException;
	
	void start(final String threadId,
	           MMXid xid,
	           int flags,
	           int timeout) throws XATransactionException;
	
	void end(final String threadId,
	         MMXid xid,
	         int flags) throws XATransactionException;
        
    Collection<Transaction> getTransactions();
    
    void terminateTransaction(Xid transactionId) throws AdminException;
    
    void terminateTransaction(String transactionId, String sessionId) throws AdminException;
}
