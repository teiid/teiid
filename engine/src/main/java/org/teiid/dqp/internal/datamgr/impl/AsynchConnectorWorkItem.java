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

import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkManager;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;

public class AsynchConnectorWorkItem extends ConnectorWorkItem {
    private WorkManager workManager;            
	
    AsynchConnectorWorkItem(AtomicRequestMessage message, ConnectorManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver, WorkManager wm) throws ConnectorException {
    	super(message, manager, resultsReceiver);
    	this.workManager = wm;
    }
    
    @Override
    protected boolean dataNotAvailable(long delay) {
    	try {
			this.manager.scheduleTask(workManager, this, delay);
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e.getCause());
		}
    	return false;
    }
    
	@Override
    protected void resumeProcessing() {
    	try {
			this.manager.reenqueueRequest(workManager, this);
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e.getCause());
		}
    }

	@Override
	public void workCompleted(WorkEvent arg0) {
		if (this.lastBatch) {
			manager.removeState(this.id);
			sendClose();
		}
	}	
}