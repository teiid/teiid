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
 * Date: Oct 2, 2003
 * Time: 4:40:36 PM
 */
package com.metamatrix.server.dqp.service;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.server.ResourceFinder;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.connector.service.ConnectorServiceInterface;

/**
 * Platform DataService.
 *
 * <p>A DQP service used to lookup <code>ConnectorService</code>s.</p>
 */
public class PlatformDataService implements DataService {
	
	private ConcurrentHashMap<ConnectorID, ConnectorServiceInterface> connectors;
	
    /**
     * DQPDataService
     * Default ctor.
     */
    public PlatformDataService() {
    }

    /**
     * Select a connector to use for the given connector binding.
     * @param connectorBindingID Connector binding identifier
     * @return ConnectorID identifying a connector instance
     */
    public ConnectorID selectConnector(String connectorBindingID) {
    	ArgCheck.isNotNull(connectorBindingID);
    	
        // get a new proxy that will be sticky to an appropriate connector
        ConnectorServiceInterface connector = PlatformProxyHelper.getConnectorServiceProxy(connectorBindingID, PlatformProxyHelper.ROUND_ROBIN_LOCAL);            
        ConnectorID connectorId = connector.getConnectorID();
        this.connectors.putIfAbsent(connectorId, connector);
        return connectorId;
    }

    /**
     * Execute the given request on a <code>Connector</code>.
     * @param request The request for data.
     */
	public void executeRequest(AtomicRequestMessage request,
			ConnectorID connectorId,
			ResultsReceiver<AtomicResultsMessage> resultListener)
			throws MetaMatrixComponentException {
    	ConnectorServiceInterface serverInstance = getConnector(connectorId);
    	RemoteResultsReceiver receiver = new RemoteResultsReceiver(ResourceFinder.getMessageBus());
    	receiver.setActualReceiver(resultListener);
    	serverInstance.executeRequest(request, receiver);
    }

	private ConnectorServiceInterface getConnector(ConnectorID connectorId)
			throws MetaMatrixComponentException {
		ConnectorServiceInterface serverInstance = this.connectors.get(connectorId);
        if ( serverInstance == null ) {
            throw new MetaMatrixComponentException(ServerPlugin.Util.getString("DQPDataService.Unable_to_find_a_connector_for_connector_ID__{0}", connectorId)); //$NON-NLS-1$
        }
		return serverInstance;
	}
	
	public void cancelRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		ConnectorServiceInterface serverInstance = getConnector(connectorId);
		serverInstance.cancelRequest(request);
	}

	public void closeRequest(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		ConnectorServiceInterface serverInstance = getConnector(connectorId);
		serverInstance.closeRequest(request);
	}

	public void requestBatch(AtomicRequestID request, ConnectorID connectorId)
			throws MetaMatrixComponentException {
		ConnectorServiceInterface serverInstance = getConnector(connectorId);
		serverInstance.requestBatch(request);
	}

	public SourceCapabilities getCapabilities(RequestMessage request, DQPWorkContext dqpWorkContext, ConnectorID connectorId) throws MetaMatrixComponentException {
		ConnectorServiceInterface serverInstance = getConnector(connectorId);
        try {
			return serverInstance.getCapabilities(dqpWorkContext.getRequestID(request.getExecutionId()), request.getExecutionPayload(), dqpWorkContext);
		} catch (ConnectorException e) {
			throw new MetaMatrixComponentException(e);
		}
    }

    /**
     * Initialize the service with the specified properties.
     * @param props Initialialization properties
     * @throws com.metamatrix.common.application.exception.ApplicationInitializationException If an error occurs during initialization
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
    	connectors = new ConcurrentHashMap<ConnectorID, ConnectorServiceInterface>();
    }

    /**
     * Start the service with the specified environment.  The environment can
     * be used to find other services or resources.
     * @param environment Environment
     * @throws com.metamatrix.common.application.exception.ApplicationLifecycleException If an error occurs while starting
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
    }

    /**
     * Stop the service.
     * @throws ApplicationLifecycleException If an error occurs while starting
     */
    public void stop() throws ApplicationLifecycleException {
        // FIXME: Won't work - need some way to indicate to the ConnectorService
        // that the client is shutting down
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String connectorBindingName) 
        throws ApplicationLifecycleException,ComponentNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#stopConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void stopConnectorBinding(String connectorBindingName) 
        throws ApplicationLifecycleException,ComponentNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindings()
     * @since 4.3
     */
    public List getConnectorBindings() throws ComponentNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingState(java.lang.String)
     * @since 4.3
     */
    public ConnectorStatus getConnectorBindingState(String connectorBindingName) throws MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding getConnectorBinding(String connectorBindingName) 
        throws MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }

    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectorBindingStatistics(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingStatistics(String connectorBindingName) 
        throws MetaMatrixComponentException{
        throw new UnsupportedOperationException();
    }
    
    /** 
     * @see com.metamatrix.dqp.service.DataService#getConnectionPoolStatistics(java.lang.String)
     * @since 6.1
     */
    public Collection getConnectionPoolStatistics(String connectorBindingName) throws MetaMatrixComponentException {
    	throw new UnsupportedOperationException();
    } 

    /** 
     * @see com.metamatrix.dqp.service.DataService#clearConnectorBindingCache(java.lang.String)
     * @since 4.3
     */
    public void clearConnectorBindingCache(String connectorBindingName) 
        throws MetaMatrixComponentException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public ConnectorMetadata getConnectorMetadata(String vdbName,
    		String vdbVersion, String modelName, Properties importProperties) {
    	throw new UnsupportedOperationException();
    }

}
