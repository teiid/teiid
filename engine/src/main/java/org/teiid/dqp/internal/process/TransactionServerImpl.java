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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.query.QueryPlugin;
import org.teiid.resource.api.XAImporter;

/**
 * Note that the begin methods do not leave the transaction associated with the
 * calling thread.  This is by design and requires explicit resumes for association.
 */
public class TransactionServerImpl implements TransactionService {

    protected static class TransactionMapping {

        // (connection -> transaction for global and local)
        private Map<String, TransactionContext> threadToTransactionContext = new HashMap<String, TransactionContext>();
        // (MMXid -> global transactions keyed)
        private Map<Xid, TransactionContext> xidToTransactionContext = new HashMap<Xid, TransactionContext>();

        public synchronized TransactionContext getOrCreateTransactionContext(String threadId) {
            TransactionContext tc = threadToTransactionContext.get(threadId);

            if (tc == null) {
                tc = new TransactionContext();
                tc.setThreadId(threadId);
                threadToTransactionContext.put(threadId, tc);
            }

            return tc;
        }

        public synchronized TransactionContext getTransactionContext(String threadId) {
            return threadToTransactionContext.get(threadId);
        }

        public synchronized TransactionContext getTransactionContext(XidImpl xid) {
            return xidToTransactionContext.get(xid);
        }

        public synchronized TransactionContext removeTransactionContext(String threadId) {
            return threadToTransactionContext.remove(threadId);
        }

        public synchronized void removeTransactionContext(TransactionContext tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.remove(tc.getXid());
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.remove(tc.getThreadId());
            }
        }

        public synchronized void addTransactionContext(TransactionContext tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.put(tc.getXid(), tc);
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.put(tc.getThreadId(), tc);
            }
        }
    }

    protected TransactionMapping transactions = new TransactionMapping();

    protected TransactionManager transactionManager;
    private boolean detectTransactions;
    private XAImporter xaImporter;

    public void setDetectTransactions(boolean detectTransactions) {
        this.detectTransactions = detectTransactions;
    }

    public boolean isDetectTransactions() {
        return detectTransactions;
    }

    public void setXaImporter(XAImporter xaImporter) {
        this.xaImporter = xaImporter;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Global Transaction
     */
    public int prepare(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, false);
        if (!tc.getSuspendedBy().isEmpty()) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30505, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30505, xid));
        }

        // In the container this pass though
        if (singleTM) {
            return XAResource.XA_RDONLY;
        }

        try {
            return this.xaImporter.prepare(tc.getXid());
        } catch (XAException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30506, e);
        }
    }

    /**
     * Global Transaction
     */
    public void commit(final String threadId, XidImpl xid, boolean onePhase, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, false);
        try {
            if (singleTM || (onePhase && XAResource.XA_RDONLY == prepare(threadId, xid, singleTM))) {
                return; //nothing to do
            }
            //TODO: we have no way of knowing for sure if we can safely use the onephase optimization
            this.xaImporter.commit(tc.getXid(), false);
        } catch (XAException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30507, e);
        } finally {
            this.transactions.removeTransactionContext(tc);
        }
    }

    /**
     * Global Transaction
     */
    public void rollback(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, false);
        try {
            // In the case of single TM, the container directly roll backs the sources.
            if (!singleTM) {
                this.xaImporter.rollback(tc.getXid());
            }
        } catch (XAException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30508, e);
        } finally {
            this.transactions.removeTransactionContext(tc);
        }
    }

    /**
     * Global Transaction
     */
    public Xid[] recover(int flag, boolean singleTM) throws XATransactionException {
        // In case of single TM, container knows this list.
        if (singleTM) {
            return new Xid[0];
        }

        try {
            return this.xaImporter.recover(flag);
        } catch (XAException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30509, e);
        }
    }

    /**
     * Global Transaction
     */
    public void forget(final String threadId, XidImpl xid, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, false);
        try {
            if (singleTM) {
                return;
            }
            this.xaImporter.forget(xid);
        } catch (XAException err) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30510, err);
        } finally {
            this.transactions.removeTransactionContext(tc);
        }
    }

    /**
     * Global Transaction
     */
    public void start(final String threadId, final XidImpl xid, int flags, int timeout, boolean singleTM) throws XATransactionException {

        TransactionContext tc = null;

        switch (flags) {
            case XAResource.TMNOFLAGS: {
                try {
                    checkXAState(threadId, xid, false, false);
                    tc = transactions.getOrCreateTransactionContext(threadId);
                    if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
                         throw new XATransactionException(QueryPlugin.Event.TEIID30517, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
                    }
                    tc.setXid(xid);
                    if (singleTM) {
                        tc.setTransaction(transactionManager.getTransaction());
                        if (tc.getTransaction() == null) {
                            //the current code currently does not handle the case of embedded connections where
                            //someone is manually initiating txns - that is there is no thread bound txn.
                            //in theory we could inflow the txn and then change all of the methods to check singleTM off of the context
                            throw new XATransactionException(QueryPlugin.Event.TEIID30590, XAException.XAER_INVAL, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30590));
                        }
                    } else {
                        Transaction txn = xaImporter.importTransaction(transactionManager, xid, timeout);
                        tc.setTransaction(txn);
                    }
                    tc.setTransactionType(TransactionContext.Scope.GLOBAL);
                } catch (XAException e) {
                     throw new XATransactionException(QueryPlugin.Event.TEIID30512, XAException.XAER_INVAL, e);
                } catch (SystemException e) {
                     throw new XATransactionException(QueryPlugin.Event.TEIID30512, XAException.XAER_INVAL, e);
                }
                break;
            }
            case XAResource.TMJOIN:
            case XAResource.TMRESUME: {
                tc = checkXAState(threadId, xid, true, false);
                TransactionContext threadContext = transactions.getOrCreateTransactionContext(threadId);
                if (threadContext.getTransactionType() != TransactionContext.Scope.NONE) {
                     throw new XATransactionException(QueryPlugin.Event.TEIID30517, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
                }

                if (flags == XAResource.TMRESUME && !tc.getSuspendedBy().remove(threadId)) {
                     throw new XATransactionException(QueryPlugin.Event.TEIID30518, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30518, new Object[] {xid, threadId}));
                }
                break;
            }
            default:
                 throw new XATransactionException(QueryPlugin.Event.TEIID30519, XAException.XAER_INVAL, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30519));
        }

        tc.setThreadId(threadId);
        transactions.addTransactionContext(tc);
    }

    /**
     * Global Transaction
     */
    public void end(final String threadId, XidImpl xid, int flags, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, true);
        try {
            switch (flags) {
                case XAResource.TMSUSPEND: {
                    tc.getSuspendedBy().add(threadId);
                    break;
                }
                case XAResource.TMSUCCESS: {
                    //TODO: should close all statements
                    break;
                }
                case XAResource.TMFAIL: {
                    cancelTransactions(threadId, false);
                    break;
                }
                default:
                     throw new XATransactionException(QueryPlugin.Event.TEIID30520, XAException.XAER_INVAL, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30520));
            }
        } finally {
            tc.setThreadId(null);
            transactions.removeTransactionContext(threadId);
        }
    }

    private TransactionContext checkXAState(final String threadId, final XidImpl xid, boolean transactionExpected, boolean threadBound) throws XATransactionException {
        TransactionContext tc = transactions.getTransactionContext(xid);

        if (transactionExpected && tc == null) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30521, XAException.XAER_NOTA, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30521, xid));
        } else if (!transactionExpected) {
            if (tc != null) {
                 throw new XATransactionException(QueryPlugin.Event.TEIID30522, XAException.XAER_DUPID, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30522, new Object[] {xid}));
            }
            if (!threadBound) {
                tc = transactions.getOrCreateTransactionContext(threadId);
                if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
                     throw new XATransactionException(QueryPlugin.Event.TEIID30517, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
                }
            }
            return null;
        }

        if (threadBound) {
            if (!threadId.equals(tc.getThreadId())) {
                 throw new XATransactionException(QueryPlugin.Event.TEIID30524, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30524, xid));
            }
        } else if (tc.getThreadId() != null) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30525, XAException.XAER_PROTO, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30525, xid));
        }

        return tc;
    }

    private TransactionContext checkLocalTransactionState(String threadId, boolean transactionExpected)
        throws XATransactionException {

        final TransactionContext tc = transactions.getOrCreateTransactionContext(threadId);

        //TODO: this check is only really needed in local mode
        if (!transactionExpected && detectTransactions) {
            try {
                Transaction tx = transactionManager.getTransaction();
                if (tx != null && tx != tc.getTransaction()) {
                    throw new XATransactionException(QueryPlugin.Event.TEIID30517, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
                }
            } catch (SystemException e) {
            } catch (IllegalStateException e) {
            }
        }

        try {
            if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
                if (tc.getTransactionType() != TransactionContext.Scope.LOCAL || !transactionExpected) {
                    throw new XATransactionException(QueryPlugin.Event.TEIID30517, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
                }
                transactionManager.resume(tc.getTransaction());
            } else if (transactionExpected) {
                throw new InvalidTransactionException(QueryPlugin.Util.getString("TransactionServer.no_transaction", threadId)); //$NON-NLS-1$
            }
        } catch (InvalidTransactionException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30526, e);
        } catch (SystemException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30527, e);
        }
        return tc;
    }

    private void beginDirect(TransactionContext tc) throws XATransactionException {
        try {
            transactionManager.begin();
            Transaction tx = transactionManager.suspend();
            tc.setTransaction(tx);
            tc.setCreationTime(System.currentTimeMillis());
        } catch (javax.transaction.NotSupportedException err) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30528, err);
        } catch (SystemException err) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30528, err);
        }
    }

    private void commitDirect(TransactionContext context)
            throws XATransactionException {
        try {
            transactionManager.commit();
        } catch (SecurityException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30530, e);
        } catch (RollbackException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30530, e);
        } catch (HeuristicMixedException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30530, e);
        } catch (HeuristicRollbackException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30530, e);
        } catch (SystemException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30530, e);
        } finally {
            transactions.removeTransactionContext(context);
        }
    }

    private void rollbackDirect(TransactionContext tc)
            throws XATransactionException {
        try {
            this.transactionManager.rollback();
        } catch (SecurityException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30535, e);
        } catch (SystemException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30535, e);
        } catch (IllegalStateException e) {
            throw new XATransactionException(QueryPlugin.Event.TEIID30535, e);
        } finally {
            transactions.removeTransactionContext(tc);
        }
    }

    public void suspend(TransactionContext context) throws XATransactionException {
        try {
            this.transactionManager.suspend();
        } catch (SystemException e) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30537, e);
        }
    }

    public void resume(TransactionContext context) throws XATransactionException {
        try {
            //if we're already associated, just return
            if (this.transactionManager.getTransaction() == context.getTransaction()) {
                return;
            }
        } catch (SystemException e) {
        }
        try {
            this.transactionManager.resume(context.getTransaction());
        } catch (IllegalStateException e) {
            throw new XATransactionException(QueryPlugin.Event.TEIID30538, e);
        } catch (InvalidTransactionException e) {
            throw new XATransactionException(QueryPlugin.Event.TEIID30538, e);
        } catch (SystemException e) {
            throw new XATransactionException(QueryPlugin.Event.TEIID30538, e);
        }
    }

    /**
     * Local Transaction
     */
    public TransactionContext begin(String threadId) throws XATransactionException {
        TransactionContext tc = checkLocalTransactionState(threadId, false);
        beginDirect(tc);
        tc.setTransactionType(TransactionContext.Scope.LOCAL);
        return tc;
    }

    /**
     * Local Transaction
     */
    public void commit(String threadId) throws XATransactionException {
        TransactionContext tc = checkLocalTransactionState(threadId, true);
        commitDirect(tc);
    }

    /**
     * Local Transaction
     */
    public void rollback(String threadId) throws XATransactionException {
        TransactionContext tc = checkLocalTransactionState(threadId, true);
        rollbackDirect(tc);
    }

    public TransactionContext getOrCreateTransactionContext(final String threadId) {
        TransactionContext tc = transactions.getOrCreateTransactionContext(threadId);
        if (detectTransactions) {
            try {
                Transaction tx = transactionManager.getTransaction();
                if (tx != null && tx != tc.getTransaction()) {
                    tx.registerSynchronization(new Synchronization() {

                        @Override
                        public void beforeCompletion() {
                        }

                        @Override
                        public void afterCompletion(int status) {
                            transactions.removeTransactionContext(threadId);
                        }
                    });
                    tc.setTransaction(tx);
                    tc.setTransactionType(Scope.INHERITED);
                }
                //TODO: it may be appropriate to throw an up-front exception
            } catch (SystemException e) {
            } catch (IllegalStateException e) {
            } catch (RollbackException e) {
            }
        }
        return tc;
    }

    /**
     * Request level transaction
     */
    public void begin(TransactionContext context) throws XATransactionException{
        if (context.getTransactionType() != TransactionContext.Scope.NONE) {
             throw new XATransactionException(QueryPlugin.Event.TEIID30517, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30517));
        }
        beginDirect(context);
        context.setTransactionType(TransactionContext.Scope.REQUEST);
        this.transactions.addTransactionContext(context); //it may have been removed if this is a block level operation
    }

    /**
     * Request level transaction
     */
    public void commit(TransactionContext context) throws XATransactionException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.REQUEST);
        try {
            commitDirect(context);
        } finally {
            context.setTransaction(null);
            context.setTransactionType(Scope.NONE);
        }
    }

    /**
     * Request level transaction
     */
    public void rollback(TransactionContext context) throws XATransactionException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.REQUEST);
        try {
            rollbackDirect(context);
        } finally {
            context.setTransaction(null);
            context.setTransactionType(Scope.NONE);
        }
    }

    public void cancelTransactions(String threadId, boolean requestOnly) throws XATransactionException {
        TransactionContext tc = requestOnly?transactions.getTransactionContext(threadId):transactions.removeTransactionContext(threadId);

        if (tc == null || tc.getTransactionType() == TransactionContext.Scope.NONE
                || tc.getTransactionType() == TransactionContext.Scope.INHERITED
                || (requestOnly && tc.getTransactionType() != TransactionContext.Scope.REQUEST)) {
            return;
        }

        try {
            Transaction t = tc.getTransaction();
            if (t != null) {
                t.setRollbackOnly();
            }
        } catch (SystemException e) {
            throw new XATransactionException(QueryPlugin.Event.TEIID30541, e);
        }
    }

    @Override
    public Collection<TransactionMetadata> getTransactions() {
        Set<TransactionContext> txnSet = Collections.newSetFromMap(new IdentityHashMap<TransactionContext, Boolean>());
        synchronized (this.transactions) {
            txnSet.addAll(this.transactions.threadToTransactionContext.values());
            txnSet.addAll(this.transactions.xidToTransactionContext.values());
        }
        Collection<TransactionMetadata> result = new ArrayList<TransactionMetadata>(txnSet.size());
        for (TransactionContext transactionContext : txnSet) {
            if (transactionContext.getTransactionType() == Scope.NONE) {
                continue;
            }
            TransactionMetadata txnImpl = new TransactionMetadata();
            txnImpl.setAssociatedSession(transactionContext.getThreadId());
            txnImpl.setCreatedTime(transactionContext.getCreationTime());
            txnImpl.setScope(transactionContext.getTransactionType().toString());
            txnImpl.setId(transactionContext.getTransactionId());
            result.add(txnImpl);
        }
        return result;
    }

    @Override
    public void terminateTransaction(String threadId) throws AdminException {
        if (threadId == null) {
            return;
        }
        try {
            cancelTransactions(threadId, false);
        } catch (XATransactionException e) {
             throw new AdminProcessingException(QueryPlugin.Event.TEIID30542, e);
        }
    }

}
