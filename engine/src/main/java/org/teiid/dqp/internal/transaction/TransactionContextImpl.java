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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.teiid.connector.xa.api.TransactionContext;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;

class TransactionContextImpl implements
                              TransactionContext, Externalizable {

    private String threadId;
    private MMXid xid;
    private String txnID;
    private Transaction transaction;
    private Scope transactionType = Scope.NONE;
    private Set<String> suspendedBy = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private int transactionTimeout = -1;
    private Set<XAResource> xaResources = Collections.newSetFromMap(new ConcurrentHashMap<XAResource, Boolean>());
    private long creationTime;
    
    public TransactionContextImpl() {}
    
    public boolean isInTransaction() {
        return getTransaction() != null;
    }

    /**
     * @param transaction
     *            The transaction to set.
     */
    void setTransaction(Transaction transaction, String id) {
        this.transaction = transaction;
        this.txnID = id;
        this.creationTime = System.currentTimeMillis();
    }
    
    long getCreationTime() {
		return creationTime;
	}
    
    /**
     * @return Returns the transaction.
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * @return Returns the txnID.
     */
    public String getTxnID() {
        return this.txnID;
    }

    void setTransactionType(Scope transactionType) {
        this.transactionType = transactionType;
    }

    public Scope getTransactionType() {
        return transactionType;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        this.buildString(sb);
        return sb.toString();
    }

    private void buildString(StringBuffer sb) {
        sb.append("TxnContext: ").append(this.txnID); //$NON-NLS-1$
    }

    void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    String getThreadId() {
        return threadId;
    }

    public MMXid getXid() {
        return this.xid;
    }

    void setXid(MMXid xid) {
        this.xid = xid;
    }

    Set<String> getSuspendedBy() {
        return this.suspendedBy;
    }

    /** 
     * @see org.teiid.connector.xa.api.TransactionContext#getTransactionTimeout()
     */
    int getTransactionTimeout() {
        return this.transactionTimeout;
    }
    
    void setTransactionTimeout(int transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }
    
    void addXAResource(XAResource resource) {
    	this.xaResources.add(resource);
    }
    
    Set<XAResource> getXAResources() {
    	return xaResources;
    }

    /** 
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException,
                                            ClassNotFoundException {
        
    }

    /** 
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        if (this.transaction != null) {
            throw new MetaMatrixRuntimeException(DQPPlugin.Util.getString("TransactionContextImpl.remote_not_supported")); //$NON-NLS-1$
        }
    }

}