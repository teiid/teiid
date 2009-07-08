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

package com.metamatrix.cdk.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import org.teiid.dqp.internal.datamgr.impl.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.language.BatchedUpdatesImpl;
import org.teiid.metadata.index.VDBMetadataFactory;

import com.metamatrix.cdk.IConnectorHost;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.util.PropertiesUtils;

/**
 * A simple test environment to execute commands on a connector.
 * Provides an alternative to deploying the connector in the full DQP environment.
 * Can be used for testing a connector.
 */
public class ConnectorHost implements IConnectorHost {

    private Connector connector;
    private TranslationUtility util;
    private ConnectorEnvironment connectorEnvironment;
    private ApplicationEnvironment applicationEnvironment;
    private ExecutionContext executionContext;
    private Properties connectorEnvironmentProperties;

    private boolean connectorStarted = false;
    
    /**
     * Create a new environment to test a connector.
     * @param connector a newly constructed connector to host in the new environment
     * @param connectorEnvironmentProperties the properties to expose to the connector as part of the connector environment
     * @param vdbFileName the path to the VDB file to load and use as the source of metadata for the queries sent to this connector
     */
    public ConnectorHost(Connector connector, Properties connectorEnvironmentProperties, String vdbFileName) {
        this(connector, connectorEnvironmentProperties, vdbFileName, true);
    }
    
    public ConnectorHost(Connector connector, Properties connectorEnvironmentProperties, String vdbFileName, boolean showLog) {  
        initialize(connector, connectorEnvironmentProperties, new TranslationUtility(VDBMetadataFactory.getVDBMetadata(vdbFileName)), showLog);
    }
    
    public ConnectorHost(Connector connector, Properties connectorEnvironmentProperties, TranslationUtility util) {
        initialize(connector, connectorEnvironmentProperties, util, true);
    }

    public ConnectorHost(Connector connector, Properties connectorEnvironmentProperties, TranslationUtility util, boolean showLog) {
        initialize(connector, connectorEnvironmentProperties, util, showLog);
    }
    
    private void initialize(Connector connector, Properties connectorEnvironmentProperties, TranslationUtility util, boolean showLog) {

        this.connector = connector;
        this.util = util;

        applicationEnvironment = new ApplicationEnvironment();
        connectorEnvironment = new ConnectorEnvironmentImpl(connectorEnvironmentProperties, new SysLogger(showLog), applicationEnvironment);
        this.connectorEnvironmentProperties = PropertiesUtils.clone(connectorEnvironmentProperties);
    }

    public void startConnectorIfNeeded() throws ConnectorException {
        if (!connectorStarted) {
            startConnector();
        }
    }

    private void startConnector() throws ConnectorException {
        connector.start(connectorEnvironment);
        connectorStarted = true;
    }

    public Properties getConnectorEnvironmentProperties() {
        return PropertiesUtils.clone(connectorEnvironmentProperties);
    }

    public void addResourceToConnectorEnvironment(String resourceName, Object resource) {
        applicationEnvironment.bindService(resourceName, (ApplicationService) resource);
    }

    /** 
     * @see com.metamatrix.cdk.IConnectorHost#setSecurityContext(java.lang.String, java.lang.String, java.lang.String, java.io.Serializable)
     * @since 4.2
     */
    public void setSecurityContext(String vdbName,
                                   String vdbVersion,
                                   String userName,
                                   Serializable trustedPayload) {
        setSecurityContext(vdbName, vdbVersion, userName, trustedPayload, null);
    }
    
    public void setSecurityContext(String vdbName, String vdbVersion, String userName, Serializable trustedPayload, Serializable executionPayload) {          
        this.executionContext = new ExecutionContextImpl(vdbName, vdbVersion, userName, trustedPayload, executionPayload, "Connection", "Connector<CDK>", "Request", "1", "0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$  
    }
    
    public void setExecutionContext(ExecutionContext context) {
    	this.executionContext = context;
    }
    
    public List executeCommand(String query) throws ConnectorException {
        startConnectorIfNeeded();

        Connection connection = null;
        try {
            connection = getConnection();
            ICommand command = getCommand(query);
            RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

            return executeCommand(connection, command, runtimeMetadata);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    public List executeCommand(ICommand command) throws ConnectorException {
        startConnectorIfNeeded();

        Connection connection = null;
        try {
            connection = getConnection();
            RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

            return executeCommand(connection, command, runtimeMetadata);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private List executeCommand(Connection connection, ICommand command, RuntimeMetadata runtimeMetadata)
        throws ConnectorException {

        ExecutionContext execContext = EnvironmentUtility.createExecutionContext("100", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        
        Execution exec = connection.createExecution(command, execContext, runtimeMetadata);
        exec.execute();
        List results = readResultsFromExecution(exec);
        exec.close();                

        return results;
    }

    public int[] executeBatchedUpdates(String[] updates) throws ConnectorException {
        startConnectorIfNeeded();

        Connection connection = null;
        try {
            connection = getConnection();
            RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
            ICommand[] commands = new ICommand[updates.length];
            for (int i = 0; i < updates.length; i++) {
                commands[i] = getCommand(updates[i]);
            }

            return executeBatchedUpdates(connection, commands, runtimeMetadata);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    public int[] executeBatchedUpdates(Connection connection, ICommand[] commands, RuntimeMetadata runtimeMetadata) throws ConnectorException {
    	List<List> result = executeCommand(connection, new BatchedUpdatesImpl(Arrays.asList(commands)), runtimeMetadata);
    	int[] counts = new int[result.size()];
    	for (int i = 0; i < counts.length; i++) {
    		counts[i] = ((Integer)result.get(i).get(0)).intValue();
    	}
    	return counts;
    }
    
    private List<List> readResultsFromExecution(Execution execution) throws ConnectorException {
    	List<List> results = new ArrayList<List>();
    	while (true) {
	    	try {
		    	if (execution instanceof ResultSetExecution) {
		    		ResultSetExecution rs = (ResultSetExecution)execution;
		    		List result = null;
		    		while ((result = rs.next()) != null) {
		    			results.add(result);
		    		}
		    		break;
		    	} 
		    	UpdateExecution rs = (UpdateExecution)execution;
	    		int[] result = rs.getUpdateCounts();
	    		for (int i = 0; i < result.length; i++) {
	    			results.add(Arrays.asList(result[i]));
	    		}
	    		break;
	    	} catch (DataNotAvailableException e) {
	    		try {
					Thread.sleep(e.getRetryDelay());
				} catch (InterruptedException e1) {
					throw new ConnectorException(e1);
				}
	    	}
    	}
    	return results;
    }

    private RuntimeMetadata getRuntimeMetadata() {
        return util.createRuntimeMetadata();
    }

    public ICommand getCommand(String query) {
    	return util.parseCommand(query);
    }

    private Connection getConnection() throws ConnectorException {
        Connection connection = connector.getConnection(executionContext);
        return connection;
    }
}
