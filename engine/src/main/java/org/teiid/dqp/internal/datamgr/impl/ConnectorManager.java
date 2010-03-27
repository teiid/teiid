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

/*
 * Date: Aug 25, 2003
 * Time: 3:53:37 PM
 */
package org.teiid.dqp.internal.datamgr.impl;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.WrappedConnection;
import org.teiid.connector.metadata.runtime.Datatype;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.logging.api.CommandLogMessage;
import org.teiid.logging.api.CommandLogMessage.Event;
import org.teiid.security.SecurityHelper;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Scope;
import com.metamatrix.query.sql.lang.Command;

/**
 * The <code>ConnectorManager</code> manages a {@link org.teiid.connector.basic.BasicConnector Connector}
 * and its associated workers' state.
 */
@ManagementObject(isRuntime=true, componentType=@ManagementComponent(type="teiid",subtype="connectormanager"), properties=ManagementProperties.EXPLICIT)
public class ConnectorManager  {
	
	public enum ConnectorStatus {
		NOT_INITIALIZED, INIT_FAILED, OPEN, DATA_SOURCE_UNAVAILABLE, CLOSED, UNABLE_TO_CHECK;
	}
	
	public static final int DEFAULT_MAX_THREADS = 20;
	private String connectorName;
	    
    private SecurityHelper securityHelper;
    
    private volatile ConnectorStatus state = ConnectorStatus.NOT_INITIALIZED;

    //services acquired in start
    private BufferService bufferService;
    
    // known requests
    private ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem> requestStates = new ConcurrentHashMap<AtomicRequestID, ConnectorWorkItem>();
	
	private SourceCapabilities cachedCapabilities;

    public ConnectorManager(String name) {
    	this(name, DEFAULT_MAX_THREADS, null);
    }
	
    public ConnectorManager(String name, int maxThreads, SecurityHelper securityHelper) {
    	if (name == null) {
    		throw new IllegalArgumentException("Connector name can not be null"); //$NON-NLS-1$
    	}
    	if (maxThreads <= 0) {
    		maxThreads = DEFAULT_MAX_THREADS;
    	}
    	this.connectorName = name;
    	this.securityHelper = securityHelper;
    }
    
    SecurityHelper getSecurityHelper() {
		return securityHelper;
	}
    
    public String getName() {
        return this.connectorName;
    }	
	    
    public MetadataStore getMetadata(String modelName, Map<String, Datatype> datatypes, Properties importProperties) throws ConnectorException {
    	
    	MetadataFactory factory = new MetadataFactory(modelName, datatypes, importProperties);
		
		WrappedConnection conn = null;
    	try {
    		checkStatus();
	    	conn = (WrappedConnection)getConnector().getConnection();
	    	conn.getConnectorMetadata(factory);
    	} finally {
    		if (conn != null) {
    			conn.close();
    		}
    	}		
    	return factory.getMetadataStore();
	}    
    
    
    public SourceCapabilities getCapabilities() throws ConnectorException {
    	if (cachedCapabilities != null) {
    		return cachedCapabilities;
    	}
        Connection conn = null;
        try {
        	checkStatus();
        	Connector connector = getConnector();
        	ConnectorCapabilities caps = connector.getCapabilities();
            boolean global = true;
            if (caps == null) {
            	conn = connector.getConnection();
            	caps = conn.getCapabilities();
            	global = false;
            }
            
            BasicSourceCapabilities resultCaps = CapabilitiesConverter.convertCapabilities(caps, getName(), connector.getConnectorEnvironment().isXaCapable());
            if (global) {
            	resultCaps.setScope(Scope.SCOPE_GLOBAL);
            	cachedCapabilities = resultCaps;
            } else {
            	resultCaps.setScope(Scope.SCOPE_PER_USER);
            }
            return resultCaps;
        } finally {
        	if ( conn != null ) {
                conn.close();
            }
        }
    }
    
    public ConnectorWork executeRequest(AtomicRequestMessage message) throws ConnectorException {
        // Set the connector ID to be used; if not already set. 
    	checkStatus();
    	AtomicRequestID atomicRequestId = message.getAtomicRequestID();
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {atomicRequestId, "Create State"}); //$NON-NLS-1$

    	ConnectorWorkItem item = new ConnectorWorkItem(message, this);
        Assertion.isNull(requestStates.put(atomicRequestId, item), "State already existed"); //$NON-NLS-1$
        return item;
    }
    
    ConnectorWork getState(AtomicRequestID requestId) {
        return requestStates.get(requestId);
    }
    
    /**
     * Remove the state associated with
     * the given <code>RequestID</code>.
     */
    void removeState(AtomicRequestID id) {
    	LogManager.logDetail(LogConstants.CTX_CONNECTOR, new Object[] {id, "Remove State"}); //$NON-NLS-1$
        requestStates.remove(id);    
    }

    int size() {
        return requestStates.size();
    }
    
    public void setBufferService(BufferService service) {
    	this.bufferService = service;
    }
    
    /**
     * initialize this <code>ConnectorManager</code>.
     */
    public synchronized void start() {
    	if (this.state != ConnectorStatus.NOT_INITIALIZED) {
    		return;
    	}
    	this.state = ConnectorStatus.INIT_FAILED;
        
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, DQPPlugin.Util.getString("ConnectorManagerImpl.Initializing_connector", connectorName)); //$NON-NLS-1$

    	this.state = ConnectorStatus.OPEN;
    }
    
    /**
     * Stop this connector.
     */
    public void stop() {        
        synchronized (this) {
        	if (this.state == ConnectorStatus.CLOSED) {
        		return;
        	}
            this.state= ConnectorStatus.CLOSED;
		}
        
        //ensure that all requests receive a response
        for (ConnectorWork workItem : this.requestStates.values()) {
    		workItem.cancel();
		}
    }

    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, an empty Collection is returned.
     */
   /* @ManagementProperty(description="Get Runtime workmanager statistics", use={ViewUse.STATISTIC}, readOnly=true)
    public WorkerPoolStatisticsMetadata getWorkManagerStatistics() {
        return workManager.getStats();
    }*/

    /**
     * Add begin point to transaction monitoring table.
     * @param qr Request that contains the MetaMatrix command information in the transaction.
     */
    void logSRCCommand(AtomicRequestMessage qr, ExecutionContext context, Event cmdStatus, Integer finalRowCnt) {
    	if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL)) {
    		return;
    	}
        String sqlStr = null;
        if(cmdStatus == Event.NEW){
        	Command cmd = qr.getCommand();
            sqlStr = cmd != null ? cmd.toString() : null;
        }
        String userName = qr.getWorkContext().getUserName();
        String transactionID = null;
        if ( qr.isTransactional() ) {
            transactionID = qr.getTransactionContext().getTransactionId();
        }
        
        String modelName = qr.getModelName();
        AtomicRequestID id = qr.getAtomicRequestID();
        
        String principal = userName == null ? "unknown" : userName; //$NON-NLS-1$
        
        CommandLogMessage message = null;
        if (cmdStatus == Event.NEW) {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, connectorName, qr.getWorkContext().getConnectionID(), principal, sqlStr, context);
        } 
        else {
            message = new CommandLogMessage(System.currentTimeMillis(), qr.getRequestID().toString(), id.getNodeID(), transactionID, modelName, connectorName, qr.getWorkContext().getConnectionID(), principal, finalRowCnt, cmdStatus, context);
        }         
        LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_COMMANDLOGGING, message);
    }
    
    /**
     * Get the <code>Connector</code> object managed by this
     * manager.
     * @return the <code>Connector</code>.
     */
    Connector getConnector() throws ConnectorException {
		try {
			InitialContext ic  = new InitialContext();
			return (Connector)ic.lookup(this.connectorName);    			
		} catch (NamingException e) {
			throw new ConnectorException(e, DQPPlugin.Util.getString("ConnectorManager.failed_to_lookup_connector", this.connectorName)); //$NON-NLS-1$
		}
    }
    
    DQPContextCache getContextCache() {
     	if (bufferService != null) {
    		return bufferService.getContextCache();
    	}
    	return null;
    }
    
    public ConnectorStatus getStatus() {
    	return this.state;
    }
    
    private void checkStatus() throws ConnectorException {
    	if (this.state != ConnectorStatus.OPEN) {
    		throw new ConnectorException(DQPPlugin.Util.getString("ConnectorManager.not_in_valid_state", this.connectorName)); //$NON-NLS-1$
    	}
    }
}
