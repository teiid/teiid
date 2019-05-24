/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 */
package org.teiid.dqp.service;

import java.util.Collection;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.TransactionMetadata;
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
    Collection<TransactionMetadata> getTransactions();

    void terminateTransaction(String threadId) throws AdminException;
}
