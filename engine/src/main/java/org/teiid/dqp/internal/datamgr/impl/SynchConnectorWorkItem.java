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

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.util.LogConstants;

public class SynchConnectorWorkItem extends ConnectorWorkItem {

	private static class TransactionLock {
		Semaphore lock = new Semaphore(1, true);
		int pendingCount;
	}

	private static Map<String, TransactionLock> TRANSACTION_LOCKS = new HashMap<String, TransactionLock>();

	private TransactionLock lock;

	SynchConnectorWorkItem(AtomicRequestMessage message,
			ConnectorManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver) {
		super(message, manager, resultsReceiver);
	}
	
	@Override
	public void run() {
		while (!this.isDoneProcessing()) { //process until closed
			try {
				acquireTransactionLock();
			} catch (InterruptedException e) {
				interrupted(e);
			} 
			try {
				super.run();
			} finally {
				releaseTxnLock();
			}
		}
	}

	@Override
	protected void pauseProcessing() {
		releaseTxnLock();
		try {
			while (isIdle()) {
				this.wait();
			}
			acquireTransactionLock();
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
		if (!this.isTransactional) {
			return;
		}
		String key = requestMsg.getTransactionContext().getTxnID();

		TransactionLock existing = null;
		synchronized (TRANSACTION_LOCKS) {
			existing = TRANSACTION_LOCKS.get(key);
			if (existing == null) {
				existing = new TransactionLock();
				TRANSACTION_LOCKS.put(key, existing);
			}
			existing.pendingCount++;
		}
		existing.lock.acquire();
		this.lock = existing;
	}

	private void releaseTxnLock() {
		if (!this.isTransactional || this.lock == null) {
			return;
		}
		synchronized (TRANSACTION_LOCKS) {
			lock.pendingCount--;
			if (lock.pendingCount == 0) {
				String key = requestMsg.getTransactionContext().getTxnID();
				TRANSACTION_LOCKS.remove(key);
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

}
