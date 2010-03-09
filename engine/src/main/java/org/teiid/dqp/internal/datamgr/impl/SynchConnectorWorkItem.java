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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.resource.spi.work.WorkEvent;
import javax.transaction.xa.Xid;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.service.TransactionContext;

public class SynchConnectorWorkItem extends ConnectorWorkItem {

	private static class TransactionLock {
		Semaphore lock = new Semaphore(1, true);
		int pendingCount;
	}

	private static Map<Xid, TransactionLock> TRANSACTION_LOCKS = new HashMap<Xid, TransactionLock>();

	private TransactionLock lock;

	SynchConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver) throws ConnectorException  {
		super(message, manager, resultsReceiver);
		
		// since container makes sure that there is no current work registered under current transaction it is
		// required that lock must be acquired before we schedule the work.
		try {
			acquireTransactionLock();
		} catch (InterruptedException e) {
			interrupted(e);
		} 			
	}
	
	@Override
	public void run() {
		while (!this.isDoneProcessing()) { //process until closed
			super.run();
		}
	}

	@Override
	protected void pauseProcessing() {
		try {
			while (isIdle()) {
				this.wait();
			}
		} catch (InterruptedException e) {
			interrupted(e);
		}
	}

	private void interrupted(InterruptedException e) {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, this.id +" Interrupted, proceeding to close"); //$NON-NLS-1$
		this.requestCancel();
	}

	@Override
	protected void resumeProcessing() {
		this.notify();
	}
	
	private void acquireTransactionLock() throws InterruptedException {		
		if (!this.requestMsg.isTransactional()) {
			return;
		}
		TransactionContext tc = this.requestMsg.getTransactionContext();		
		Xid key = tc.getXid();

		TransactionLock existing = null;
		synchronized (TRANSACTION_LOCKS) {
			existing = TRANSACTION_LOCKS.get(key);
			if (existing == null) {
				existing = new TransactionLock();
				TRANSACTION_LOCKS.put(key, existing);
			}
			existing.pendingCount++;
			tc.incrementPartcipatingSourceCount(requestMsg.getConnectorName());
		}
		existing.lock.acquire();
		this.lock = existing;
		LogManager.logTrace("got the connector lock on =", key);
	}

	private void releaseTxnLock() {
		if (!this.requestMsg.isTransactional() || this.lock == null) {
			return;
		}
		TransactionContext tc = this.requestMsg.getTransactionContext();
		synchronized (TRANSACTION_LOCKS) {
			lock.pendingCount--;
			if (lock.pendingCount == 0) {
				Xid key = tc.getXid();
				TRANSACTION_LOCKS.remove(key);
				LogManager.logTrace("released the connector lock on =", key);
			}
		}
		lock.lock.release();
		this.lock = null;
	}
	
    @Override
    protected boolean dataNotAvailable(long delay) {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {
				"AtomicRequest", id, "On connector", manager.getName(), " threw a DataNotAvailableException, but will be ignored since this is a Synch Connector." }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    	return true;
    }

    
	@Override
	public void workCompleted(WorkEvent event) {
		try {
			super.workCompleted(event);
		} finally {
			releaseTxnLock();
		}
	}

	@Override
	public void workRejected(WorkEvent event) {
		try {
			super.workRejected(event);
		} finally {
			releaseTxnLock();
		}
	}
}
