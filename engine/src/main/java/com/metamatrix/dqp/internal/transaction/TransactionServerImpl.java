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

package com.metamatrix.dqp.internal.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.teiid.connector.xa.api.TransactionContext;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.transaction.TransactionProvider.XAConnectionSource;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.transaction.XAServer;

public class TransactionServerImpl implements
                                    TransactionServer, XAServer {

    private static class TransactionMapping {

        // (connection -> transaction for global and local)
        private Map threadToTransactionContext = new HashMap();
        // (MMXid -> global transactions keyed)
        private Map xidToTransactionContext = new HashMap();

        public synchronized TransactionContextImpl getOrCreateTransactionContext(String threadId) {
            TransactionContextImpl tc = (TransactionContextImpl)threadToTransactionContext.get(threadId);

            if (tc == null) {
                tc = new TransactionContextImpl();
                tc.setThreadId(threadId);
                threadToTransactionContext.put(threadId, tc);
            }

            return tc;
        }
        
        public synchronized TransactionContextImpl getTransactionContext(String threadId) {
            return (TransactionContextImpl)threadToTransactionContext.get(threadId);
        }

        public synchronized TransactionContextImpl getTransactionContext(MMXid xid) {
            return (TransactionContextImpl)xidToTransactionContext.get(xid);
        }

        public synchronized TransactionContextImpl removeTransactionContext(String threadId) {
            return (TransactionContextImpl)threadToTransactionContext.remove(threadId);
        }

        public synchronized void removeTransactionContext(TransactionContextImpl tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.remove(tc.getXid());
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.remove(tc.getThreadId());
            }
        }
        
        public synchronized void removeTransactionContext(MMXid xid) {
            this.xidToTransactionContext.remove(xid);
        }

        public synchronized void addTransactionContext(TransactionContextImpl tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.put(tc.getXid(), tc);
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.put(tc.getThreadId(), tc);
            }
        }
    }

    private TransactionMapping transactions = new TransactionMapping();
    
    private TransactionProvider provider;

    public TransactionServerImpl() {
    }

    public void init(TransactionProvider theProvider) throws XATransactionException {
        this.provider = theProvider;
    }

    public synchronized void shutdown(boolean force) {
        this.provider.shutdown();
    }

    @SuppressWarnings("finally")
	public int prepare(final String threadId,
                       MMXid xid) throws XATransactionException {
        TransactionContextImpl impl = checkXAState(threadId, xid, true, false);
        if (!impl.getSuspendedBy().isEmpty()) {
            throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.suspended_exist", xid)); //$NON-NLS-1$
        }
        try {
        	getTransactionManager().resume(impl.getTransaction());
        	try {
        		endAssociations(impl);
        	} finally {
        		return this.provider.getXATerminator().prepare(xid);
        	}
        } catch (XAException err) {
            throw new XATransactionException(err);
        } catch (SystemException err) {
        	throw new XATransactionException(err);
		} catch (InvalidTransactionException err) {
			throw new XATransactionException(err);
		} catch (IllegalStateException err) {
			throw new XATransactionException(err);
		} finally {
			try {
				getTransactionManager().suspend();
			} catch (SystemException err) {
				throw new XATransactionException(err);
			}
		}
    }
    
    private void endAssociations(TransactionContextImpl impl) throws XATransactionException, SystemException {
        Transaction tx = getTransactionManager().getTransaction();
        Assertion.isNotNull(tx);
        for (Iterator i = impl.getXAResources().iterator(); i.hasNext();) {
        	XAResource resource = (XAResource)i.next();
        	if (!tx.delistResource(resource, XAResource.TMSUCCESS)) {
                throw new XATransactionException(DQPPlugin.Util.getString("TransactionServer.failed_to_delist")); //$NON-NLS-1$
            }
        }
        impl.getXAResources().clear();
    }

    public void commit(final String threadId,
                       MMXid xid,
                       boolean onePhase) throws XATransactionException {

        TransactionContextImpl tc = checkXAState(threadId, xid, true, false);        
        try {
            if (onePhase && prepare(threadId, xid) == XAResource.XA_RDONLY) {
                return;
            }
            // TODO: for one phase, MM needs to check if there are multiple resources involved
            // or single, in its txn, if it has single it can use onePhase.
            // Also, Arjuna has bug JBTM-457, where on single phase commit, they do not call the synchronization
            // when they fix the bug, we can re-write next couple lines differently to make use of the
            // optimization of onePhase.
            this.provider.getXATerminator().commit(xid, false);
        } catch (XAException err) {
            throw new XATransactionException(err);
        } finally {
            this.transactions.removeTransactionContext(tc);
        }
    }

    public void rollback(final String threadId,
                         MMXid xid) throws XATransactionException {
        TransactionContextImpl tc = checkXAState(threadId, xid, true, false);
        try {
            this.provider.getXATerminator().rollback(xid);
        } catch (XAException err) {
            throw new XATransactionException(err);
        } finally {
            this.transactions.removeTransactionContext(tc);
        }
    }

    public Xid[] recover(int flag) throws XATransactionException {
        try {
            return this.provider.getXATerminator().recover(flag);
        } catch (XAException err) {
            throw new XATransactionException(err);
        }
    }

    public void forget(final String threadId,
                       MMXid xid) throws XATransactionException {
        try {
            this.provider.getXATerminator().forget(xid);
        } catch (XAException err) {
            throw new XATransactionException(err);
        } finally {
            this.transactions.removeTransactionContext(xid);
        }
    }

    public void start(final String threadId,
                      final MMXid xid,
                      int flags,
                      int timeout) throws XATransactionException {
        
        TransactionContextImpl tc = null;

        switch (flags) {
            case XAResource.TMNOFLAGS: {
                checkXAState(threadId, xid, false, false);
                tc = transactions.getOrCreateTransactionContext(threadId);
                if (tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_NONE) {
                    throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
                }
                Transaction tx;
                try {
                    tx = this.provider.importTransaction(xid, timeout);
                } catch (XAException err) {
                    throw new XATransactionException(err);
                } 
                
                try {
                    tx.registerSynchronization(new Synchronization() {

                        public void afterCompletion(int arg0) {
                            transactions.removeTransactionContext(xid);
                        }

                        public void beforeCompletion() {
                        }});
                } catch (RollbackException err) {
                    throw new XATransactionException(err, XAException.XA_RBROLLBACK);
                } catch (SystemException err) {
                    throw new XATransactionException(err, XAException.XAER_RMERR);
                }
                tc.setTransaction(tx, provider.getTransactionID(tx));
                tc.setTransactionTimeout(timeout);
                tc.setXid(xid);
                tc.setTransactionType(TransactionContext.Scope.TRANSACTION_GLOBAL);
                break;
            }
            case XAResource.TMJOIN:
            case XAResource.TMRESUME: {
                tc = checkXAState(threadId, xid, true, false);
                TransactionContextImpl threadContext = transactions.getOrCreateTransactionContext(threadId);
                if (threadContext.getTransactionType() != TransactionContext.Scope.TRANSACTION_NONE) {
                    throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
                }
                
                if (flags == XAResource.TMRESUME && !tc.getSuspendedBy().remove(threadId)) {
                    throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.resume_failed", new Object[] {xid, threadId})); //$NON-NLS-1$
                }
                break;
            }
            default:
                throw new XATransactionException(XAException.XAER_INVAL, DQPPlugin.Util.getString("TransactionServer.unknown_flags")); //$NON-NLS-1$
        }

        tc.setThreadId(threadId);
        transactions.addTransactionContext(tc);
    }

    public void end(final String threadId,
                    MMXid xid,
                    int flags) throws XATransactionException {
        TransactionContextImpl tc = checkXAState(threadId, xid, true, true);
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
                    try {
                        tc.getTransaction().setRollbackOnly();
                    } catch (SystemException err) {
                        throw new XATransactionException(err, XAException.XAER_RMERR);
                    }
                    throw new XATransactionException(new XAException(XAException.XA_RBROLLBACK));
                }
                default:
                    throw new XATransactionException(XAException.XAER_INVAL, DQPPlugin.Util.getString("TransactionServer.unknown_flags")); //$NON-NLS-1$
            }
        } finally {
            tc.setThreadId(null);
            transactions.removeTransactionContext(threadId);
        }
    }

    private TransactionContextImpl checkXAState(final String threadId,
                                                    final MMXid xid,
                                                    boolean transactionExpected, boolean threadBound) throws XATransactionException {
        TransactionContextImpl tc = transactions.getTransactionContext(xid);
        
        if (transactionExpected && tc == null) {
            throw new XATransactionException(XAException.XAER_INVAL, DQPPlugin.Util.getString("TransactionServer.no_global_transaction", xid)); //$NON-NLS-1$
        } else if (!transactionExpected) {
            if (tc != null) {
                throw new XATransactionException(XAException.XAER_DUPID, DQPPlugin.Util.getString("TransactionServer.existing_global_transaction", new Object[] {xid})); //$NON-NLS-1$
            }
            if (!threadBound) {
                tc = transactions.getOrCreateTransactionContext(threadId);
                if (tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_NONE) {
                    throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.existing_transaction", new Object[] {xid, threadId})); //$NON-NLS-1$
                }
            }
            return null;
        } 
        
        if (threadBound) {
            if (!threadId.equals(tc.getThreadId())) {
                throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.wrong_transaction", xid)); //$NON-NLS-1$
            }
        } else if (tc.getThreadId() != null) {
            throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.concurrent_transaction", xid)); //$NON-NLS-1$
        }
        
        return tc;
    }

    private TransactionContextImpl checkLocalTransactionState(String threadId,
                                               boolean transactionExpected) throws NotSupportedException,
                                                                           SystemException,
                                                                           InvalidTransactionException {

        final TransactionContextImpl tc = transactions.getOrCreateTransactionContext(threadId);

        final TransactionManager tm = getTransactionManager();

        if (tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_NONE) {
            if (tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_LOCAL) {
                throw new NotSupportedException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
            }
            if (!transactionExpected) {
                throw new NotSupportedException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
            }
            tm.resume(tc.getTransaction());
        } else if (transactionExpected) {
            throw new InvalidTransactionException(DQPPlugin.Util.getString("TransactionServer.no_transaction", threadId)); //$NON-NLS-1$
        }

        return tc;
    }

    public TransactionContext begin(String threadId) throws XATransactionException, SystemException {
        try {
            TransactionContextImpl tc = checkLocalTransactionState(threadId, false);
            final TransactionManager tm = getTransactionManager();
            tm.begin();
            Transaction tx = tm.suspend();
            tc.setTransaction(tx, provider.getTransactionID(tx));
            tc.setTransactionType(TransactionContext.Scope.TRANSACTION_LOCAL);
            return tc;
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } catch (NotSupportedException err) {
            throw new XATransactionException(err);
        } 
    }

    public void commit(String threadId) throws XATransactionException, SystemException {
        TransactionContextImpl tc;
        try {
            tc = checkLocalTransactionState(threadId, true);
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } catch (NotSupportedException err) {
            throw new XATransactionException(err);
        } 
        final TransactionManager tm = getTransactionManager();
        try {
        	try {
        		endAssociations(tc);
        	} finally {
        		tm.commit();
        	}
        } catch (IllegalStateException err) {
            throw new XATransactionException(err);
        } catch (RollbackException err) {
            throw new XATransactionException(err);
        } catch (HeuristicMixedException err) {
            throw new XATransactionException(err);
        } catch (HeuristicRollbackException err) {
            throw new XATransactionException(err);
        } finally {
            transactions.removeTransactionContext(tc);
        }
        
    }

    public void rollback(String threadId) throws XATransactionException, SystemException {
        TransactionContextImpl tc;
        try {
            tc = checkLocalTransactionState(threadId, true);
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } catch (NotSupportedException err) {
            throw new XATransactionException(err);
        } 
        final TransactionManager tm = getTransactionManager();
        
        try {
            tm.rollback();
        } catch (IllegalStateException err) {
            throw new XATransactionException(err);
        } finally {
            transactions.removeTransactionContext(tc);
        }
    }

    private TransactionManager getTransactionManager() {
        return provider.getTransactionManager();
    }

    public TransactionContext getOrCreateTransactionContext(String threadId) {
        return transactions.getOrCreateTransactionContext(threadId);
    }

    // request level transaction
    public TransactionContext start(TransactionContext context) throws XATransactionException, SystemException {
        TransactionManager tm = getTransactionManager();

        TransactionContextImpl tc = (TransactionContextImpl)context;

        try {
            if (tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_NONE) {
                throw new XATransactionException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
            }
            tm.begin();
            Transaction tx = tm.suspend();
            
            tc.setTransaction(tx, provider.getTransactionID(tx));
            tc.setTransactionType(TransactionContext.Scope.TRANSACTION_REQUEST);
            return tc;
        } catch (NotSupportedException e) {
            throw new XATransactionException(e);
        }
    }

    public TransactionContext commit(TransactionContext context) throws XATransactionException, SystemException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.TRANSACTION_REQUEST);
        TransactionContextImpl tc = (TransactionContextImpl)context;
        
        //commit may be called multiple times by the processworker, if this is a subsequent call, then the current
        //context will not be active
        TransactionContextImpl currentContext = transactions.getTransactionContext(tc.getThreadId());
        if (currentContext == null || currentContext.getTransactionType() == TransactionContext.Scope.TRANSACTION_NONE) {
            return currentContext;
        }
        TransactionManager tm = getTransactionManager();
        
        try {
            tm.resume(context.getTransaction());
            try {
            	endAssociations(tc);
            } finally {
            	tm.commit();
            }
        } catch (InvalidTransactionException e) {
            throw new XATransactionException(e);
        } catch (RollbackException e) {
            throw new XATransactionException(e);
        } catch (HeuristicMixedException e) {
            throw new XATransactionException(e);
        } catch (HeuristicRollbackException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext(tc);
        }
        return context;
    }

    public TransactionContext rollback(TransactionContext context) throws XATransactionException, SystemException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.TRANSACTION_REQUEST);
        TransactionManager tm = getTransactionManager();
        try {
            tm.resume(context.getTransaction());
        	tm.rollback();
        } catch (InvalidTransactionException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext((TransactionContextImpl)context);
        }        
        return context;
    }

    public TransactionContext delist(TransactionContext context,
                                     XAResource resource,
                                     int flags) throws XATransactionException {
        TransactionManager tm = getTransactionManager();
        TransactionContextImpl tc = (TransactionContextImpl)context;
        
        try {
            Transaction tx = tm.getTransaction();
            if (!tx.equals(context.getTransaction())) {
                throw new XATransactionException(context.getTransaction() + " != " + tx); //$NON-NLS-1$
            }
    
            // intermediate suspend/success is not necessary because we hold the connector connection
            // for the duration of the transaction. However, we want to suspend because 
            // ConnectorWorker thread needs to be disassociated.
        } catch (SystemException err) {
            throw new XATransactionException(err);
        } catch (IllegalStateException err) {
            throw new XATransactionException(err);
        } finally {
            try {
                tm.suspend();
            } catch (SystemException err) {
                throw new XATransactionException(err);
            }
        }
        return tc;
    }

    public TransactionContext enlist(TransactionContext context,
                                     XAResource resource) throws XATransactionException {
        TransactionManager tm = getTransactionManager();
        TransactionContextImpl tc = (TransactionContextImpl)context;
        
        try {
            if (tc.getTransactionTimeout() > 0) {
                if (tc.getTransactionTimeout() != resource.getTransactionTimeout()) {
                    resource.setTransactionTimeout(tc.getTransactionTimeout());
                }
            }
            Transaction tx = tm.getTransaction();
            if (tx == null) {
                tm.resume(context.getTransaction());
            } else {
                if (!tx.equals(context.getTransaction())) {
                    throw new XATransactionException(context.getTransaction() + " != " + tx); //$NON-NLS-1$
                }
            }

            if (!context.getTransaction().enlistResource(resource)) {
				context.getTransaction().delistResource(resource, XAResource.TMFAIL);
                throw new XATransactionException(DQPPlugin.Util.getString("TransactionServer.failed_to_enlist")); //$NON-NLS-1$
            }
            tc.addXAResource(resource);
        } catch (SystemException err) {
            throw new XATransactionException(err);
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } catch (RollbackException err) {
            throw new XATransactionException(err);
        } catch (IllegalStateException err) {
            throw new XATransactionException(err);
        } catch (XAException err) {
            throw new XATransactionException(err);
        }

        return context;
    }

    /** 
     * @throws IllegalStateException 
     * @throws InvalidTransactionException 
     * @see com.metamatrix.dqp.transaction.TransactionServer#cancelTransactions(java.lang.String)
     */
    public void cancelTransactions(String threadId, boolean requestOnly) throws InvalidTransactionException, SystemException {
        TransactionContextImpl tc = transactions.getTransactionContext(threadId);
        
        if (tc == null || tc.getTransactionType() == TransactionContext.Scope.TRANSACTION_NONE) {
            return;
        }
        
        if (requestOnly && tc.getTransactionType() != TransactionContext.Scope.TRANSACTION_REQUEST) {
            return;
        }
        
        TransactionManager tm = getTransactionManager();
        
        try {
            tm.resume(tc.getTransaction());
            tm.setRollbackOnly();
        } finally {
            tm.suspend();
            transactions.removeTransactionContext(tc);
        }
    }    
    
    public synchronized void registerRecoverySource(String name, XAConnectionSource resource) {
        this.provider.registerRecoverySource(name, resource);
    }

    public synchronized void removeRecoverySource(String name) {
        this.provider.removeRecoverySource(name);
    }    
    
}
