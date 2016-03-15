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

package org.teiid.dqp.service;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.resource.spi.work.ExecutionContext;
import javax.transaction.Transaction;

public class TransactionContext extends ExecutionContext implements Serializable{

	private static final long serialVersionUID = -8689401273499649058L;

	public enum Scope {
		GLOBAL,
		LOCAL,
		NONE,
		REQUEST
	}
	
    private String threadId;
    private Scope transactionType = Scope.NONE;
    private long creationTime;
    private Transaction transaction;
    private Set<String> suspendedBy = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private int isolationLevel;

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

}