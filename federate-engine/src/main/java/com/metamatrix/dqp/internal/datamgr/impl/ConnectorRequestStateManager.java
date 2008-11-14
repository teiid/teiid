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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.internal.datamgr.ConnectorPropertyNames;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.util.LogConstants;

public class ConnectorRequestStateManager {
	
	/* service object always up-to-date */
    private ConnectorManager connectorManager;

    /* state specific to a lifecycle of the connector */
    Connector connector;
    MetadataService metadataService;
    int maxResultRows;
    boolean exceptionOnMaxRows = true;
    TransactionService transactionService;
    private WorkerPool pool;
    private boolean pooledResource;

    /* synchronized Collection for tracking request state objects */
    /** AtomicRequestID --> ConnectorRequestState */
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();

    // Lazily created and used for asynch query execution
    private Timer timer;
       
    /**
     * ConnectorStateManager
     * Default ctor.
     */
    public ConnectorRequestStateManager(Connector connector, ConnectorManager connectorManager, WorkerPool pool, TransactionService transactionSrevice, MetadataService metadataService) {
        this.connector = connector;
        this.connectorManager = connectorManager;
        this.pool = pool;
        
        String maxResultRowsString = connectorManager.getEnvironment().getApplicationProperties().getProperty(ConnectorPropertyNames.MAX_RESULT_ROWS);
        if ( maxResultRowsString != null && maxResultRowsString.trim().length() > 0 ) {
            try {
                maxResultRows = Integer.parseInt(maxResultRowsString);
            } catch (NumberFormatException e) {
                Object[] params = new Object[]{ConnectorPropertyNames.MAX_RESULT_ROWS};
                String msg = DQPPlugin.Util.getString("ConnectorManagerImpl.Couldn__t_parse_property", params); //$NON-NLS-1$
                LogManager.logWarning(LogConstants.CTX_CONNECTOR, e, msg);
            }
        }
        this.exceptionOnMaxRows = PropertiesUtils.getBooleanProperty(connectorManager.getEnvironment().getApplicationProperties(), ConnectorPropertyNames.EXCEPTION_ON_MAX_ROWS, false);
        this.pooledResource = PropertiesUtils.getBooleanProperty(connectorManager.getEnvironment().getApplicationProperties(), ConnectorPropertyNames.POOLED_RESOURCE, true);
        this.transactionService = transactionSrevice;
        this.metadataService = metadataService;
    }

    public void executeRequest(AtomicRequestMessage message, ResultsReceiver<AtomicResultsMessage> resultsReciever) {
    	enqueueRequest(createState(message, resultsReciever));
    }
    
    public void closeRequest(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestClose();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    public void cancelRequest(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestCancel();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }
    
    public void requestMore(AtomicRequestID requestId) {
    	ConnectorWorkItem workItem = getState(requestId);
    	if (workItem == null) {
    		return; //already closed
    	}
        ClassLoader contextloader = Thread.currentThread().getContextClassLoader();
        try {
        	Thread.currentThread().setContextClassLoader(this.connector.getClass().getClassLoader());
    	    workItem.requestMore();
        } finally {
        	Thread.currentThread().setContextClassLoader(contextloader);
        }
    }

    /**
     * Method to add a state object to be tracked by this manager.
     * @param requestMsg
     * @throws ConnectorException
     */
    ConnectorWorkItem createState(AtomicRequestMessage requestMsg, ResultsReceiver<AtomicResultsMessage> resultsReceiver) {
        AtomicRequestID atomicRequestId = requestMsg.getAtomicRequestID();
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {atomicRequestId, "Create State"}); //$NON-NLS-1$
    	
    	ConnectorWorkItem item = null;
    	if (pooledResource) {
    		item = new SynchConnectorWorkItem(requestMsg, this, resultsReceiver);
    	} else {
    		item = new AsynchConnectorWorkItem(requestMsg, this, resultsReceiver);
    	}

        Assertion.isNull(requestStates.put(atomicRequestId, item), "State already existed"); //$NON-NLS-1$
        requestMsg.markProcessingStart();
        return item;
    }

    ConnectorWorkItem getState(AtomicRequestID requestId) {
        return requestStates.get(requestId);
    }

    void reenqueueRequest(AsynchConnectorWorkItem state) {
    	enqueueRequest(state);
    }
    
    private void enqueueRequest(ConnectorWorkItem state) {
        this.pool.execute(state);
    }

    /**
     * Remove the state associated with
     * the given <code>RequestID</code>.
     */
    public void removeState(AtomicRequestID id) {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {id, "Remove State"}); //$NON-NLS-1$
        requestStates.remove(id);    
    }

    int size() {
        return requestStates.size();
    }

    /**
     * Schedule a task to be executed after the specified delay (in milliseconds) 
     * @param task The task to execute
     * @param delay The delay to wait (in ms) before executing the task
     * @since 4.3.3
     */
    public void scheduleTask(final AsynchConnectorWorkItem state, long delay) {
        synchronized(this) {
            if(this.timer == null) {
                this.timer = new Timer("AsynchRequestThread", true); //$NON-NLS-1$
            }
        }
        
        this.timer.schedule(new TimerTask() {
			@Override
			public void run() {
				state.requestMore();
			}}, delay);
    }
    
    /**
     * Shutdown all long-lived state, such as the timer thread.
     * 
     * @since 4.3.3
     */
    public synchronized void shutdown() {
    	for (ConnectorWorkItem workItem : this.requestStates.values()) {
			workItem.resultsReceiver.exceptionOccurred(new ConnectorException("shut down"));
		}
        if(this.timer != null) {
            this.timer.cancel();
            this.timer = null;
        }        
    } 
    
    void logSRCCommand(AtomicRequestMessage requestMsg, ExecutionContext context, short cmdStatus, int finalRowCnt) {
        connectorManager.logSRCCommand(requestMsg, context, cmdStatus, finalRowCnt);
    }
    
}
