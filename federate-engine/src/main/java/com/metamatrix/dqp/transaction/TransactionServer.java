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

package com.metamatrix.dqp.transaction;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import com.metamatrix.common.xa.TransactionContext;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.data.xa.api.XAConnector;

public interface TransactionServer {
    
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
    void registerRecoverySource(String name, XAConnector resource);
    
    void removeRecoverySource(String name);            
}
