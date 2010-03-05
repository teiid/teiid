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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.language.BatchedUpdates;
import org.teiid.connector.language.Command;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.metadata.index.VDBMetadataFactory;

/**
 * A simple test environment to execute commands on a connector.
 * Provides an alternative to deploying the connector in the full DQP environment.
 * Can be used for testing a connector.
 */
public class ConnectorHost {

    private Connector connector;
    private TranslationUtility util;
    private ExecutionContext executionContext;
    
    public ConnectorHost(Connector connector, ConnectorEnvironment connectorEnvironment, String vdbFileName) throws ConnectorException {  
        initialize(connector, connectorEnvironment, new TranslationUtility(VDBMetadataFactory.getVDBMetadata(vdbFileName)));
    }
    
    public ConnectorHost(Connector connector, ConnectorEnvironment connectorEnvironment, TranslationUtility util) throws ConnectorException{
        initialize(connector, connectorEnvironment, util);
    }
    
    private void initialize(Connector connector, final ConnectorEnvironment env, TranslationUtility util) throws ConnectorException {
        this.connector = connector;
        this.util = util;
        this.connector.initialize(env);
    }

    public void setExecutionContext(ExecutionContext context) {
    	this.executionContext = context;
    }
    
    public List executeCommand(String query) throws ConnectorException {

        Connection connection = null;
        try {
            connection = getConnection();
            Command command = getCommand(query);
            RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

            return executeCommand(connection, command, runtimeMetadata);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    public List executeCommand(Command command) throws ConnectorException {

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

    private List executeCommand(Connection connection, Command command, RuntimeMetadata runtimeMetadata)
        throws ConnectorException {

        Execution exec = connection.createExecution(command, this.executionContext, runtimeMetadata);
        exec.execute();
        List results = readResultsFromExecution(exec);
        exec.close();                

        return results;
    }

    public int[] executeBatchedUpdates(String[] updates) throws ConnectorException {

        Connection connection = null;
        try {
            connection = getConnection();
            RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
            Command[] commands = new Command[updates.length];
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
    
    public int[] executeBatchedUpdates(Connection connection, Command[] commands, RuntimeMetadata runtimeMetadata) throws ConnectorException {
    	List<List> result = executeCommand(connection, new BatchedUpdates(Arrays.asList(commands)), runtimeMetadata);
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

    public Command getCommand(String query) {
    	return util.parseCommand(query);
    }

    private Connection getConnection() throws ConnectorException {
        Connection connection = connector.getConnection();
        return connection;
    }
    
}
