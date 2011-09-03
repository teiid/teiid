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
package org.teiid.dqp.service;

import java.util.Collection;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Transaction;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;


/**
 */
public interface TransactionService {
    
    // processor level methods
    void begin(TransactionContext context) throws XATransactionException;

    void commit(TransactionContext context) throws XATransactionException;

    void rollback(TransactionContext context) throws XATransactionException;

    TransactionContext getOrCreateTransactionContext(String threadId);
    
    void suspend(TransactionContext context) throws XATransactionException;
    
    void resume(TransactionContext context) throws XATransactionException;

    // local transaction methods
    TransactionContext begin(String threadId) throws XATransactionException;

    void commit(String threadId) throws XATransactionException;

    void rollback(String threadId) throws XATransactionException;

    void cancelTransactions(String threadId, boolean requestOnly) throws XATransactionException;

    // global transaction methods
    int prepare(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException;

	void commit(final String threadId, XidImpl xid, boolean onePhase, boolean singleTM) throws XATransactionException;
	
	void rollback(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException;
	
	Xid[] recover(int flag, boolean singleTM) throws XATransactionException;
	
	void forget(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException;
	
	void start(final String threadId, XidImpl xid, int flags, int timeout, boolean singleTM) throws XATransactionException;
	
	void end(final String threadId, XidImpl xid, int flags, boolean singleTM) throws XATransactionException;
        
	// management methods
    Collection<Transaction> getTransactions();
    
    void terminateTransaction(String transactionId) throws AdminException;
}
