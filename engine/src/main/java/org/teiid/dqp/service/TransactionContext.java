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

package org.teiid.dqp.service;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;

import org.teiid.core.TeiidRuntimeException;

public class TransactionContext implements Serializable, Cloneable{

    private static final long serialVersionUID = -8689401273499649058L;

    public enum Scope {
        GLOBAL,
        LOCAL,
        NONE,
        REQUEST,
        INHERITED
    }

    private String threadId;
    private Scope transactionType = Scope.NONE;
    private long creationTime;
    private Transaction transaction;
    private Set<String> suspendedBy = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private int isolationLevel;
    private Xid xid;

    public int getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long time) {
        this.creationTime = time;
    }

    public void setTransactionType(Scope transactionType) {
        this.transactionType = transactionType;
    }

    public Scope getTransactionType() {
        return transactionType;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public String getThreadId() {
        return threadId;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public String toString() {
        return threadId + " " + transactionType + " ID:" + getTransactionId(); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public String getTransactionId() {
        if (this.transaction != null) {
            return this.transaction.toString();
        } else if (this.getXid() != null) {
            return this.getXid().toString();
        }
        return "NONE"; //$NON-NLS-1$
    }

    public Set<String> getSuspendedBy() {
        return this.suspendedBy;
    }

    @Override
    public TransactionContext clone() {
        try {
            return (TransactionContext) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    /**
     * set a transaction context.
     *
     * @param xid transaction context.
     */
    public void setXid(Xid xid)
    {
       this.xid = xid;
    }

    /**
     * @return an Xid object carrying a transaction context,
     * if any.
     */
    public Xid getXid()
    {
       return this.xid;
    }

}