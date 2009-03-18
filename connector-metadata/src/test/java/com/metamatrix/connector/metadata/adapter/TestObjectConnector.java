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

package com.metamatrix.connector.metadata.adapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

/**
 */
public class TestObjectConnector extends TestCase {
    private QueryMetadataInterfaceBuilder metadataBuilder;
    private FakeObjectConnector connector;

    /**
     * Constructor for ObjectConnectorTest.
     * @param name
     */
    public TestObjectConnector(String name) {
        super(name);
    }

    public void test() {
        List[] rows = runQuery(new String[] {"hey", "there"}, "select length from objects", String.class); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals(new Integer(3), rows[0].get(0));
        assertEquals(new Integer(5), rows[1].get(0));
    }

    public void testToUpperCase() {
        List[] rows = runQuery(new String[] {"hey", "there"}, "select toUpperCase from objects", String.class); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("HEY", rows[0].get(0)); //$NON-NLS-1$
        assertEquals("THERE", rows[1].get(0)); //$NON-NLS-1$
    }

    public void testCustomObjects() {
        FakeObject[] objects = new FakeObject[2];
        objects[0] = new FakeObject("tom", 20); //$NON-NLS-1$
        objects[1] = new FakeObject("jim", 30); //$NON-NLS-1$
        List[] rows = runQuery(objects, "select age, name from objects", FakeObject.class); //$NON-NLS-1$
        assertEquals(new Integer(20), rows[0].get(0));
        assertEquals("tom", rows[0].get(1)); //$NON-NLS-1$
        assertEquals(new Integer(30), rows[1].get(0));
        assertEquals("jim", rows[1].get(1)); //$NON-NLS-1$
    }

    public void testMethodsAsTables() {
        FakeObjectWithNonScalarMethod fakeObject1 = new FakeObjectWithNonScalarMethod("bob", new String[] {"boss", "supervisor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeObjectWithNonScalarMethod fakeObject2 = new FakeObjectWithNonScalarMethod("jim", new String[] {"worker", "doctor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] rows = runQuery( "objects", "objects(getTitles)", new FakeObjectWithNonScalarMethod[] {fakeObject1, fakeObject2}, //$NON-NLS-1$ //$NON-NLS-2$
            "select getTitles from objects", FakeObjectWithNonScalarMethod.class, "getTitles"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("boss", rows[0].get(0)); //$NON-NLS-1$
        assertEquals("supervisor", rows[1].get(0)); //$NON-NLS-1$
        assertEquals("worker", rows[2].get(0)); //$NON-NLS-1$
        assertEquals("doctor", rows[3].get(0)); //$NON-NLS-1$
    }

    public void testMethodsAsTablesStripsMethodOffTableName() {
        FakeObjectWithNonScalarMethod fakeObject1 = new FakeObjectWithNonScalarMethod("bob", new String[] {"boss", "supervisor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeObjectWithNonScalarMethod fakeObject2 = new FakeObjectWithNonScalarMethod("jim", new String[] {"worker", "doctor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        runQuery( "objects", "objects(getTitles)", new FakeObjectWithNonScalarMethod[] {fakeObject1, fakeObject2}, //$NON-NLS-1$ //$NON-NLS-2$
            "select getTitles from objects", FakeObjectWithNonScalarMethod.class, "getTitles"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("objects", connector.getTableNameQueried()); //$NON-NLS-1$
    }

    public void testMethodsAsTablesCombinedWithRegularColumns() {
        FakeObjectWithNonScalarMethod fakeObject1 = new FakeObjectWithNonScalarMethod("bob", new String[] {"boss", "supervisor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeObjectWithNonScalarMethod fakeObject2 = new FakeObjectWithNonScalarMethod("jim", new String[] {"worker", "doctor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] rows = runQuery( "objects", "objects(getTitles)", new FakeObjectWithNonScalarMethod[] {fakeObject1, fakeObject2}, //$NON-NLS-1$ //$NON-NLS-2$
            "select getTitles, getName from objects", FakeObjectWithNonScalarMethod.class, "getTitles"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("boss", rows[0].get(0)); //$NON-NLS-1$
        assertEquals("bob", rows[0].get(1)); //$NON-NLS-1$
        assertEquals("supervisor", rows[1].get(0)); //$NON-NLS-1$
        assertEquals("bob", rows[1].get(1)); //$NON-NLS-1$
        assertEquals("worker", rows[2].get(0)); //$NON-NLS-1$
        assertEquals("jim", rows[2].get(1)); //$NON-NLS-1$
        assertEquals("doctor", rows[3].get(0)); //$NON-NLS-1$
        assertEquals("jim", rows[3].get(1)); //$NON-NLS-1$
    }

    public void testMethodsAsTablesOfTypeCollectionCombinedWithRegularColumns() {
        FakeObjectWithNonScalarMethod fakeObject1 = new FakeObjectWithNonScalarMethod("bob", new String[] {"boss", "supervisor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeObjectWithNonScalarMethod fakeObject2 = new FakeObjectWithNonScalarMethod("jim", new String[] {"worker", "doctor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List[] rows = runQuery( "objects", "objects(getTitlesAsCollection)", new FakeObjectWithNonScalarMethod[] {fakeObject1, fakeObject2}, //$NON-NLS-1$ //$NON-NLS-2$
            "select getTitlesAsCollection, getName from objects", FakeObjectWithNonScalarMethod.class, "getTitlesAsCollection"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("boss", rows[0].get(0)); //$NON-NLS-1$
        assertEquals("bob", rows[0].get(1)); //$NON-NLS-1$
        assertEquals("supervisor", rows[1].get(0)); //$NON-NLS-1$
        assertEquals("bob", rows[1].get(1)); //$NON-NLS-1$
        assertEquals("worker", rows[2].get(0)); //$NON-NLS-1$
        assertEquals("jim", rows[2].get(1)); //$NON-NLS-1$
        assertEquals("doctor", rows[3].get(0)); //$NON-NLS-1$
        assertEquals("jim", rows[3].get(1)); //$NON-NLS-1$
    }

    public void testQueryCancel_Defect18362() {
        FakeObjectWithNonScalarMethod fakeObject1 = new FakeObjectWithNonScalarMethod("bob", new String[] {"boss", "supervisor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        FakeObjectWithNonScalarMethod fakeObject2 = new FakeObjectWithNonScalarMethod("jim", new String[] {"worker", "doctor"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        defineMetadata("objects", "objects(getTitles)", FakeObjectWithNonScalarMethod.class, "getTitles"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        List objects = Arrays.asList(new FakeObjectWithNonScalarMethod[] {fakeObject1, fakeObject2});
        connector = new FakeObjectConnector(objects);
        Properties properties = new Properties();
        ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, null, null);

        try {
            connector.start(environment);
            Connection connection = connector.getConnection(null);
            QueryMetadataInterface metadata = metadataBuilder.getQueryMetadata();
            CommandBuilder commandBuilder = new CommandBuilder(metadata);
            final IQuery command = (IQuery) commandBuilder.getCommand("select getTitles from objects"); //$NON-NLS-1$

            RuntimeMetadata runtimeMetadata = metadataBuilder.getRuntimeMetadata();
            ExecutionContext execContext = EnvironmentUtility.createExecutionContext("100", "1"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetExecution execution = (ResultSetExecution) connection.createExecution(command, execContext, runtimeMetadata);
            ExecutionThread executeThread = new ExecutionThread(execution);
            executeThread.start();
            execution.cancel();
            try {
                executeThread.join();
            } catch (InterruptedException e) {

            }
            if (executeThread.exception != null) {
                assertTrue(executeThread.exception instanceof ConnectorException);
            }
        } catch (ConnectorException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    public List[] runQuery(String tableName, String tableNameInSource, Object[] objectArray, String query, Class type,
        String tableDefiningMethodName) {
        defineMetadata(tableName, tableNameInSource, type, tableDefiningMethodName);
        List objects = Arrays.asList(objectArray);
        connector = new FakeObjectConnector(objects);
        Properties properties = new Properties();
        ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, null, null);

        try {
            connector.start(environment);
            Connection connection = connector.getConnection(null);
            QueryMetadataInterface metadata = metadataBuilder.getQueryMetadata();
            CommandBuilder commandBuilder = new CommandBuilder(metadata);
            IQuery command = (IQuery) commandBuilder.getCommand(query);

            RuntimeMetadata runtimeMetadata = metadataBuilder.getRuntimeMetadata();
            ExecutionContext execContext = EnvironmentUtility.createExecutionContext("100", "1"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetExecution execution = (ResultSetExecution) connection.createExecution(command, execContext, runtimeMetadata);
            execution.execute();
            List<List> results = new ArrayList<List>();
            List row = null;
            while ((row = execution.next()) != null) {
            	results.add(row);
            }
            return (List[])results.toArray(new List[0]);
        } catch (ConnectorException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    public List[] runQuery(Object[] objectArray, String query, Class type) {
        return runQuery("objects", "objects", objectArray, query, type, null); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void defineMetadata(String tableName, String tableNameInSource, Class type, String tableDefiningMethodName) {
        metadataBuilder = new QueryMetadataInterfaceBuilder();
        metadataBuilder.addMetadataForType(tableName, tableNameInSource, type, tableDefiningMethodName);
    }

    private static class ExecutionThread extends Thread {
        private ResultSetExecution execution;
        private Throwable exception;

        private ExecutionThread(ResultSetExecution execution) {
            this.execution = execution;
        }
        public void run() {
            try {
                execution.execute();
                execution.next();
            } catch (Throwable t) {
                exception = t;
            }
        }
    }
}

