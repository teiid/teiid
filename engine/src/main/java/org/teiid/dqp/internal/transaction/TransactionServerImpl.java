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

package org.teiid.dqp.internal.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import javax.resource.NotSupportedException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.impl.TransactionMetadata;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.TransactionContext;
import com.metamatrix.dqp.service.TransactionService;

public class TransactionServerImpl implements TransactionService {

    private static class TransactionMapping {

        // (connection -> transaction for global and local)
        private Map<String, TransactionContext> threadToTransactionContext = new HashMap<String, TransactionContext>();
        // (MMXid -> global transactions keyed) 
        private Map<Integer, TransactionContext> xidToTransactionContext = new HashMap<Integer, TransactionContext>();
        
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

        public synchronized TransactionContext getTransactionContext(MMXid xid) {
            return xidToTransactionContext.get(xid.hashCode());
        }
        
        public synchronized TransactionContext getTransactionContext(int hash) {
            return xidToTransactionContext.get(hash);
        }
        
        public synchronized TransactionContext removeTransactionContext(String threadId) {
            return threadToTransactionContext.remove(threadId);
        }

        public synchronized void removeTransactionContext(TransactionContext tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.remove(tc.getXid().hashCode());
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.remove(tc.getThreadId());
            }
        }
        
        public synchronized void addTransactionContext(TransactionContext tc) {
            if (tc.getXid() != null) {
                this.xidToTransactionContext.put(tc.getXid().hashCode(), tc);
            }
            if (tc.getThreadId() != null) {
                this.threadToTransactionContext.put(tc.getThreadId(), tc);
            }
        }
    }

    private TransactionMapping transactions = new TransactionMapping();
    
    private TransactionProvider provider;
    private String processName = "embedded"; //$NON-NLS-1$
    private XidFactory xidFactory;

    public TransactionServerImpl() {
    }

    public void setProcessName(String processName) {
		this.processName = processName;
	}
    
    public void setTransactionProvider(TransactionProvider theProvider) {
        this.provider = theProvider;
    }

    /**
     * Global Transaction 
     */
	public int prepare(final String threadId, MMXid xid, boolean singleTM) throws XATransactionException {
        TransactionContext tc = checkXAState(threadId, xid, true, false);
        if (!tc.getSuspendedBy().isEmpty()) {
            throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.suspended_exist", xid)); //$NON-NLS-1$
        }		
        
        if (tc.shouldRollback()) {
        	throw new XATransactionException(XAException.XAER_RMERR, DQPPlugin.Util.getString("TransactionServer.rollback_set", xid));
        }
        
        // In the container this pass though
        if (singleTM) {        	    	
	    	return XAResource.XA_RDONLY;
        }
        
        try {
        	return this.provider.getXATerminator().prepare(tc.getXid());
        } catch (XAException e) {
            throw new XATransactionException(e);
        }
    }
    
    /**
     * Global Transaction 
     */    
    public void commit(final String threadId, MMXid xid, boolean onePhase, boolean singleTM) throws XATransactionException {
    	TransactionContext tc = checkXAState(threadId, xid, true, false);  
    	try {
    		// In the case of single TM, the container directly commits the sources.
        	if (!singleTM) {
        		// In the case of onePhase containers call commit directly. If Teiid is also one phase let it pass through,
        		// otherwise force the prepare.
        		boolean singlePhase = tc.isOnePhase();
        		if (onePhase && !singlePhase) {
        			prepare(threadId, xid, singleTM);
        		}
        		this.provider.getXATerminator().commit(tc.getXid(), singlePhase);
        	}
    	} catch (XAException e) {
            throw new XATransactionException(e);
        } finally {
    		this.transactions.removeTransactionContext(tc);
    	}
    }
    
    /**
     * Global Transaction 
     */
    public void rollback(final String threadId, MMXid xid, boolean singleTM) throws XATransactionException {
    	TransactionContext tc = checkXAState(threadId, xid, true, false);  
    	try {
    		// In the case of single TM, the container directly roll backs the sources.
        	if (!singleTM) {
        		this.provider.getXATerminator().rollback(tc.getXid());
        	}
    	} catch (XAException e) {
            throw new XATransactionException(e);
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
			return this.provider.getXATerminator().recover(flag);
		} catch (XAException e) {
			throw new XATransactionException(e);
		}
    }

    /**
     * Global Transaction 
     */    
    public void forget(final String threadId, MMXid xid, boolean singleTM) throws XATransactionException {
    	TransactionContext tc = checkXAState(threadId, xid, true, false); 
        try {
        	if (singleTM) {
        		return;
        	}
            this.provider.getXATerminator().forget(xid);
        } catch (XAException err) {
            throw new XATransactionException(err);
        } finally {
        	this.transactions.removeTransactionContext(tc);
        }
    }

    /**
     * Global Transaction 
     */
    public void start(final String threadId, final MMXid xid, int flags, int timeout, boolean singleTM) throws XATransactionException {
        
        TransactionContext tc = null;

        switch (flags) {
            case XAResource.TMNOFLAGS: {
                try {
					checkXAState(threadId, xid, false, false);
					tc = transactions.getOrCreateTransactionContext(threadId);
					if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
					    throw new XATransactionException(XAException.XAER_PROTO, DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
					}
					tc.setTransactionTimeout(timeout);
					tc.setXid(xid);
					tc.setTransactionType(TransactionContext.Scope.GLOBAL);
				} catch (NotSupportedException e) {
					throw new XATransactionException(XAException.XAER_INVAL, e.getMessage()); //$NON-NLS-1$
				}
                break;
            }
            case XAResource.TMJOIN:
            case XAResource.TMRESUME: {
                tc = checkXAState(threadId, xid, true, false);
                TransactionContext threadContext = transactions.getOrCreateTransactionContext(threadId);
                if (threadContext.getTransactionType() != TransactionContext.Scope.NONE) {
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

    /**
     * Global Transaction 
     */    
    public void end(final String threadId, MMXid xid, int flags, boolean singleTM) throws XATransactionException {
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
                	tc.setRollbackOnly();
                    break;
                }
                default:
                    throw new XATransactionException(XAException.XAER_INVAL, DQPPlugin.Util.getString("TransactionServer.unknown_flags")); //$NON-NLS-1$
            }
        } finally {
            tc.setThreadId(null);
            transactions.removeTransactionContext(threadId);
        }
    }

    private TransactionContext checkXAState(final String threadId, final MMXid xid, boolean transactionExpected, boolean threadBound) throws XATransactionException {
        TransactionContext tc = transactions.getTransactionContext(xid);
        
        if (transactionExpected && tc == null) {
            throw new XATransactionException(XAException.XAER_NOTA, DQPPlugin.Util.getString("TransactionServer.no_global_transaction", xid)); //$NON-NLS-1$
        } else if (!transactionExpected) {
            if (tc != null) {
                throw new XATransactionException(XAException.XAER_DUPID, DQPPlugin.Util.getString("TransactionServer.existing_global_transaction", new Object[] {xid})); //$NON-NLS-1$
            }
            if (!threadBound) {
                tc = transactions.getOrCreateTransactionContext(threadId);
                if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
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

    private TransactionContext checkLocalTransactionState(String threadId, boolean transactionExpected) 
    	throws InvalidTransactionException {

        final TransactionContext tc = transactions.getOrCreateTransactionContext(threadId);

        if (tc.getTransactionType() != TransactionContext.Scope.NONE) {
            if (tc.getTransactionType() != TransactionContext.Scope.LOCAL) {
                throw new InvalidTransactionException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
            }
            if (!transactionExpected) {
                throw new InvalidTransactionException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
            }
        } else if (transactionExpected) {
            throw new InvalidTransactionException(DQPPlugin.Util.getString("TransactionServer.no_transaction", threadId)); //$NON-NLS-1$
        }

        return tc;
    }

    /**
     * Local Transaction 
     */
    public TransactionContext begin(String threadId) throws XATransactionException {
        try {
            TransactionContext tc = checkLocalTransactionState(threadId, false);
            tc.setXid(this.xidFactory.createXid());
            tc.setCreationTime(System.currentTimeMillis());
            tc.setTransactionType(TransactionContext.Scope.LOCAL);
            return tc;
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        }
    }

    /**
     * Local Transaction 
     */    
    public void commit(String threadId) throws XATransactionException {
        TransactionContext tc;
        try {
            tc = checkLocalTransactionState(threadId, true);
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } 

        if (tc.shouldRollback()) {
        	rollback(threadId);
        }
        try {
        	if (tc.isInTransaction()) {
            	directCommit(tc);
        	}
        } catch (XAException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext(tc);
        }
    }

    /**
     * Local Transaction 
     */    
    public void rollback(String threadId) throws XATransactionException {
        TransactionContext tc;
        try {
            tc = checkLocalTransactionState(threadId, true);
        } catch (InvalidTransactionException err) {
            throw new XATransactionException(err);
        } 
        
        try {
        	if (tc.isInTransaction()) {
        		this.provider.getXATerminator().rollback(tc.getXid());
        	}
        } catch (XAException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext(tc);
        }
    }


    public TransactionContext getOrCreateTransactionContext(String threadId) {
        return transactions.getOrCreateTransactionContext(threadId);
    }

    /**
     * Request level transaction
     */
    public TransactionContext start(TransactionContext context) throws XATransactionException{
        if (context.getTransactionType() != TransactionContext.Scope.NONE) {
            throw new XATransactionException(DQPPlugin.Util.getString("TransactionServer.existing_transaction")); //$NON-NLS-1$
        }
        context.setXid(this.xidFactory.createXid());
        context.setCreationTime(System.currentTimeMillis());
        context.setTransactionType(TransactionContext.Scope.REQUEST);
        return context;
    }

    /**
     * Request level transaction
     */    
    public TransactionContext commit(TransactionContext context) throws XATransactionException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.REQUEST);
        
        //commit may be called multiple times by the processworker, if this is a subsequent call, then the current
        //context will not be active
        TransactionContext tc = transactions.getTransactionContext(context.getThreadId());
        if (tc == null || tc.getTransactionType() == TransactionContext.Scope.NONE) {
            return tc;
        }
        
        Assertion.assertTrue(!tc.shouldRollback());
        
        try {
        	if (tc.isInTransaction()) {
            	directCommit(tc);
        	}
        } catch (XAException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext(context);
        }
        return context;
    }

	private void directCommit(TransactionContext tc) throws XAException {
		boolean commit = true;
		boolean onePhase = tc.isOnePhase();
		if (!onePhase) {
			int prepare = this.provider.getXATerminator().prepare(tc.getXid());
			commit = (prepare == XAResource.XA_OK);
		}
		
		if (commit) {
			this.provider.getXATerminator().commit(tc.getXid(), onePhase);
		}
	}

    /**
     * Request level transaction
     */    
    public TransactionContext rollback(TransactionContext context) throws XATransactionException {
        Assertion.assertTrue(context.getTransactionType() == TransactionContext.Scope.REQUEST);
        try {
	        this.provider.getXATerminator().rollback(context.getXid());
        } catch (XAException e) {
            throw new XATransactionException(e);
        } finally {
            transactions.removeTransactionContext(context);
        }
        return context;      
    }

    /**
     * Request level transaction
     */
    public void cancelTransactions(String threadId, boolean requestOnly) throws XATransactionException {
        TransactionContext tc = transactions.getTransactionContext(threadId);
        
        if (tc == null || tc.getTransactionType() == TransactionContext.Scope.NONE) {
            return;
        }
        
        if (requestOnly && tc.getTransactionType() != TransactionContext.Scope.REQUEST) {
            return;
        }
        
        tc.setRollbackOnly();
        
        if (requestOnly) {
        	rollback(tc);
        }
        else {
        	rollback(threadId);
        }
    }

	@Override
	public Collection<org.teiid.adminapi.Transaction> getTransactions() {
		Set<TransactionContext> txnSet = Collections.newSetFromMap(new IdentityHashMap<TransactionContext, Boolean>());
		synchronized (this.transactions) {
			txnSet.addAll(this.transactions.threadToTransactionContext.values());
			txnSet.addAll(this.transactions.xidToTransactionContext.values());
		}
		Collection<org.teiid.adminapi.Transaction> result = new ArrayList<org.teiid.adminapi.Transaction>(txnSet.size());
		for (TransactionContext TransactionContext : txnSet) {
			if (TransactionContext.getXid() == null) {
				continue;
			}
			TransactionMetadata txnImpl = new TransactionMetadata();
			txnImpl.setAssociatedSession(Long.parseLong(TransactionContext.getThreadId()));
			txnImpl.setCreatedTime(TransactionContext.getCreationTime());
			txnImpl.setScope(TransactionContext.getTransactionType().toString());
			txnImpl.setXid(TransactionContext.getXid().toString());
			result.add(txnImpl);
		}
		return result;
	}
	
	@Override
	public void terminateTransaction(String xid) throws AdminException {
		if (xid == null) {
			return;
		}
		TransactionContext context = this.transactions.getTransactionContext(xid.hashCode());
		context.setRollbackOnly();
		
		try {
			if (context.getTransactionType() == TransactionContext.Scope.REQUEST ) {
				rollback(context);
			}
			
			if (context.getTransactionType() == TransactionContext.Scope.LOCAL ) {
				rollback(context.getThreadId());
			}
			
			if (context.getTransactionType() == TransactionContext.Scope.GLOBAL ) {
				throw new AdminProcessingException("Can not terminate global transactions!");
			}			
		} catch (XATransactionException e) {
			throw new AdminProcessingException(e.getMessage());
		}
	}
	
	public void setXidFactory(XidFactory f) {
		this.xidFactory = f;
	}
}
