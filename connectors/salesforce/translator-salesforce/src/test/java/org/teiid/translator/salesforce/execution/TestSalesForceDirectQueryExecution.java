/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.salesforce.execution;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestSalesForceDirectQueryExecution {

    private static SalesForceExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new SalesForceExecutionFactory();
        TRANSLATOR.setSupportsDirectQueryProcedure(true);
        TRANSLATOR.start();
    }

    @Test public void testSearch() throws Exception {
        String input = "exec native('search;SELECT Account.Id, Account.Type, Account.Name FROM Account')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        QueryResult qr = Mockito.mock(QueryResult.class);
        Mockito.stub(qr.isDone()).toReturn(true);

        SObject[] results = new SObject[1];

        SObject s = new SObject();
        s.setType("Account");
        s.setId("The ID");
        results[0] = s;

        s.addField("Type", "The Type");
        s.addField("Name", "The Name");

        Mockito.stub(qr.getRecords()).toReturn(results);
        Mockito.stub(connection.query("SELECT Account.Id, Account.Type, Account.Name FROM Account", 0, false)).toReturn(qr);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection, Mockito.times(1)).query("SELECT Account.Id, Account.Type, Account.Name FROM Account", 0, false);

        assertArrayEquals(new Object[] {"The ID", "The Type", "The Name"}, (Object[])execution.next().get(0));

    }

    @Test(expected=TranslatorException.class) public void testWithoutMarker() throws Exception {
        String input = "exec native('salesforce query')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();
    }

    @Test public void testDelete() throws Exception {
        String input = "exec native('delete;', 'id1','id2')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        ArgumentCaptor<String[]> payloadArgument = ArgumentCaptor.forClass(String[].class);
        Mockito.stub(connection.delete(payloadArgument.capture())).toReturn(23);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection, Mockito.times(1)).delete(payloadArgument.capture());

        assertEquals("id1", payloadArgument.getValue()[0]);
        assertEquals("id2", payloadArgument.getValue()[1]);

        assertArrayEquals(new Object[] {23}, (Object[])execution.next().get(0));
    }

    @Test public void testUpdate() throws Exception {
        String input = "exec native('update;id=pk;type=table;attributes=one,two,three', 'one', 2, 3.0)";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        ArgumentCaptor<List> payloadArgument = ArgumentCaptor.forClass(List.class);
        Mockito.stub(connection.update(payloadArgument.capture())).toReturn(23);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection).update(payloadArgument.capture());

        assertEquals(1, payloadArgument.getValue().size());
        assertEquals("pk", ((DataPayload)payloadArgument.getValue().get(0)).getID());
        assertEquals("table", ((DataPayload)payloadArgument.getValue().get(0)).getType());
        assertEquals(3, ((DataPayload)payloadArgument.getValue().get(0)).getMessageElements().size());

        assertArrayEquals(new Object[] {23}, (Object[])execution.next().get(0));
    }

    @Test public void testCreate() throws Exception {
        String input = "exec native('create;id=pk;type=table;attributes=one,two,three', 'one', 2, 3.0)";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        ArgumentCaptor<DataPayload> payloadArgument = ArgumentCaptor.forClass(DataPayload.class);
        Mockito.stub(connection.create(payloadArgument.capture())).toReturn(23);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();


        Mockito.verify(connection).create(payloadArgument.capture());

        assertEquals("pk", payloadArgument.getValue().getID());
        assertEquals("table", payloadArgument.getValue().getType());
        assertEquals(3, payloadArgument.getValue().getMessageElements().size());

        assertArrayEquals(new Object[] {23}, (Object[])execution.next().get(0));
    }

    @Test(expected=TranslatorException.class) public void testCreateFail() throws Exception {
        String input = "exec native('create;id=pk;type=table;attributes=one,two,three', 'one')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        DirectQueryExecution execution = (DirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();
    }
}
