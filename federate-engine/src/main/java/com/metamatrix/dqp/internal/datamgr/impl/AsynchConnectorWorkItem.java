/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.impl;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.data.api.AsynchQueryCommandExecution;
import com.metamatrix.data.api.AsynchQueryExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;

public class AsynchConnectorWorkItem extends ConnectorWorkItem {
                
    AsynchConnectorWorkItem(AtomicRequestMessage message, ConnectorRequestStateManager manager, ResultsReceiver<AtomicResultsMessage> resultsReceiver) {
    	super(message, manager, resultsReceiver);
    	Assertion.assertTrue(!this.isTransactional, "Asynch work items are not suitable for transactions"); //$NON-NLS-1$
    }
    
    @Override
    protected void handleBatch(Batch batch) throws ConnectorException,
    		CommunicationException {
    	if(batch.getRowCount() == 0 && !batch.isLast() && (execution instanceof AsynchQueryExecution || execution instanceof AsynchQueryCommandExecution)) {                
        	long delay = 0;
        	if (execution instanceof AsynchQueryExecution) {
        		delay = ((AsynchQueryExecution)execution).getPollInterval();
        	} else {
        		delay = ((AsynchQueryCommandExecution)execution).getPollInterval();        		
        	}
			this.manager.scheduleTask(this, delay);
        } else {
        	super.handleBatch(batch);
        }
    }

	@Override
    protected void resumeProcessing() {
    	this.manager.reenqueueRequest(this);
    }

}