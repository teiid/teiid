/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.cdk.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
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

    public ConnectorHost(ExecutionFactory connector, Object connectionFactory, String vdbFileName) throws TranslatorException {
        initialize(connector, connectionFactory, new TranslationUtility(VDBMetadataFactory.getVDBMetadata(vdbFileName)));
    }

    public ConnectorHost(ExecutionFactory connector, Object connectionFactory, TranslationUtility util) throws TranslatorException{
        initialize(connector, connectionFactory, util);
    }

    private void initialize(ExecutionFactory connector, Object connectionFactory, TranslationUtility util) throws TranslatorException {
        this.connector = connector;
        this.util = util;
        this.connectionFactory = connectionFactory;
        this.connector.start();
    }

    public void setExecutionContext(ExecutionContext context) {
        this.executionContext = context;
    }

    public List executeCommand(String query) throws TranslatorException {
        Command command = getCommand(query);
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

        return executeCommand(command, runtimeMetadata, true);
    }

    public List executeCommand(String query, boolean close) throws TranslatorException {
        Command command = getCommand(query);
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();

        return executeCommand(command, runtimeMetadata, close);
    }

    public List executeCommand(Command command) throws TranslatorException {
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
        return executeCommand(command, runtimeMetadata, true);
    }

    private List executeCommand(Command command, RuntimeMetadata runtimeMetadata, boolean close)
        throws TranslatorException {

        Execution exec = connector.createExecution(command, this.executionContext, runtimeMetadata, this.connectionFactory);
        exec.execute();
        List results = readResultsFromExecution(exec);
        if (close) {
            exec.close();
        }
        return results;
    }

    public int[] executeBatchedUpdates(String[] updates) throws TranslatorException {
        RuntimeMetadata runtimeMetadata = getRuntimeMetadata();
        Command[] commands = new Command[updates.length];
        for (int i = 0; i < updates.length; i++) {
            commands[i] = getCommand(updates[i]);
        }

        return executeBatchedUpdates(commands, runtimeMetadata);
    }

    public int[] executeBatchedUpdates(Command[] commands, RuntimeMetadata runtimeMetadata) throws TranslatorException {
        List<List> result = executeCommand(new BatchedUpdates(Arrays.asList(commands)), runtimeMetadata, true);
        int[] counts = new int[result.size()];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = ((Integer)result.get(i).get(0)).intValue();
        }
        return counts;
    }

    private List<List> readResultsFromExecution(Execution execution) throws TranslatorException {
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
                if (e.getRetryDelay() > 0) {
                    try {
                        Thread.sleep(e.getRetryDelay());
                    } catch (InterruptedException e1) {
                        throw new TranslatorException(e1);
                    }
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
