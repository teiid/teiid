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

package com.metamatrix.dqp.service;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

/**
 * Defines interface for management of the Data Services, i.e Connectors with
 * which all the physical data is accessed.
 *  
 */
public interface DataService extends ApplicationService {

    /**
     * Select a connector to use for the given connector binding.
     * @param connectorBindingName Connector binding identifier
     * @return ConnectorID identifying a connector instance
     */
    ConnectorID selectConnector(String connectorBindingName) 
        throws MetaMatrixComponentException;
    
    /**
     * Execute the given request on a <code>Connector</code>.
     * @param request
     * @return
     */
    void executeRequest(AtomicRequestMessage request, ConnectorID connector, ResultsReceiver<AtomicResultsMessage> resultListener) 
        throws MetaMatrixComponentException;
    
	void cancelRequest(AtomicRequestID request, ConnectorID connectorId) throws MetaMatrixComponentException;

	void closeRequest(AtomicRequestID request, ConnectorID connectorId) throws MetaMatrixComponentException;
	
	void requestBatch(AtomicRequestID request, ConnectorID connectorId) throws MetaMatrixComponentException;

    /**
     * Find the capabilities of this source.
     * @param request Original request message, used to extract security information
     * @param connector Connector to retrieve capabilities from
     * @return All capability information as key-value pairs
     */
    SourceCapabilities getCapabilities(RequestMessage request, DQPWorkContext dqpWorkContext, ConnectorID connector) 
        throws MetaMatrixComponentException;
    
    /**
     * Return the metadata for a given connector
     * @param vdbName
     * @param vdbVersion
     * @param modelName
     * @param importProperties
     * @return
     * @throws MetaMatrixComponentException 
     */
    ConnectorMetadata getConnectorMetadata(String vdbName, String vdbVersion, String modelName, Properties importProperties) throws MetaMatrixComponentException;
               
    /**
     * Start the Connector Binding by the name given, if it is already added and not srarted. 
     * @param connectorBindingName
     * @throws ApplicationLifecycleException
     * @throws ComponentNotFoundException
     * @since 4.3
     */
    void startConnectorBinding(String connectorBindingName)
        throws ApplicationLifecycleException, MetaMatrixComponentException;

    /**
     * Stop the Connector Binding by the given name 
     * @param connectorBindingName
     * @throws ApplicationLifecycleException
     * @throws ComponentNotFoundException
     * @since 4.3
     */
    void stopConnectorBinding(String connectorBindingName) 
        throws ApplicationLifecycleException, MetaMatrixComponentException;
    
    /**
     * Get the list of connector bindings deployed in the system. 
     * @return list {@link com.metamatrix.common.config.api.ConnectorBinding}
     * @throws ComponentNotFoundException
     * @since 4.3
     */
    List getConnectorBindings() throws MetaMatrixComponentException;
    

    /**
     * Get the connector binding specifed the name 
     * @return ConnectorBinding
     * @throws ComponentNotFoundException
     * @since 4.3
     */
    ConnectorBinding getConnectorBinding(String connectorBindingName) 
        throws MetaMatrixComponentException;
    
    /**
     * Get the State of the connector binding name 
     * @return {@link ConnectorStatus}
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    ConnectorStatus getConnectorBindingState(String connectorBindingName) 
        throws MetaMatrixComponentException;
    
    /**
     * Get connector bindings queue statistics 
     * @param connectorBindingName - Name of the connector binding
     * @return a list of {@link com.metamatrix.common.queue.WorkerPoolStats}
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    Collection getConnectorBindingStatistics(String connectorBindingName)
        throws MetaMatrixComponentException;
    
    /**
     * Clear any caches for the connector binding. Incase of the JDBC Connector
     * clear the result set cache. 
     * @param connectorBindingName
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    void clearConnectorBindingCache(String connectorBindingName) 
        throws MetaMatrixComponentException;
    
    /**
     * Get connection pool statistics for connector binding 
     * @param connectorBindingName - Name of the connector binding
     * @return a list of {@link com.metamatrix.common.stats.ConnectionPoolStats}
     * @throws MetaMatrixComponentException
     * @since 6.1
     */
    Collection getConnectionPoolStatistics(String connectorBindingName)
        throws MetaMatrixComponentException;
}
