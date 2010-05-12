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

import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.UpdateExecution;

/**
 * A simple test environment to execute commands on a connector.
 * Provides an alternative to deploying the connector in the full DQP environment.
 * Can be used for testing a connector.
 */
public class ConnectorHost {

    private ExecutionFactory connector;
    private TranslationUtility util;
    private ExecutionContext executionContext;
    private Object connectionFactory;
    
    public ConnectorHost(ExecutionFactory connector, Object connectionFactory, String vdbFileName) throws ConnectorException {  
        initialize(connector, connectionFactory, new TranslationUtility(VDBMetadataFactory.getVDBMetadata(vdbFileName)));
    }
    
    public ConnectorHost(ExecutionFactory connector, Object connectionFactory, TranslationUtility util) throws ConnectorException{
        initialize(connector, connectionFactory, util);
    }
    
    private void initialize(ExecutionFactory connector, Object connectionFactory, TranslationUtility util) throws ConnectorException {
        this.connector = connector;
        this.util = util;
        this.connectionFactory = connectionFactory;
        this.connector.start();
    }

    public void setExecutionContext(ExecutionContext context) {
    	this.executionContext = context;
    }
    
    public List executeCommand(String query) throws ConnectorException {
        Command command = getCommand(query);
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

        return executeCommand(command, runtimeMetadata);
    }
    
    public List executeCommand(Command command) throws ConnectorException {
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
        return executeCommand(command, runtimeMetadata);
    }

    private List executeCommand(Command command, RuntimeMetadata runtimeMetadata)
        throws ConnectorException {

        Execution exec = connector.createExecution(command, this.executionContext, runtimeMetadata, this.connectionFactory);
        exec.execute();
        List results = readResultsFromExecution(exec);
        exec.close();                

        return results;
    }

    public int[] executeBatchedUpdates(String[] updates) throws ConnectorException {
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
        Command[] commands = new Command[updates.length];
        for (int i = 0; i < updates.length; i++) {
            commands[i] = getCommand(updates[i]);
        }

        return executeBatchedUpdates(commands, runtimeMetadata);
    }
    
    public int[] executeBatchedUpdates(Command[] commands, RuntimeMetadata runtimeMetadata) throws ConnectorException {
    	List<List> result = executeCommand(new BatchedUpdates(Arrays.asList(commands)), runtimeMetadata);
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
}
