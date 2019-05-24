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
package org.teiid.translator.jpa;

import static org.junit.Assert.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;

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

@SuppressWarnings("nls")
public class TestJPADirectQueryExecution {

    private static JPA2ExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new JPA2ExecutionFactory();
        TRANSLATOR.setSupportsDirectQueryProcedure(true);
        TRANSLATOR.start();
    }

    @Test public void testSearch() throws Exception {
        String input = "exec native('search;SELECT Account.Id, Account.Type, Account.Name FROM Account')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        EntityManager connection = Mockito.mock(EntityManager.class);
        Query query = Mockito.mock(Query.class);

        Mockito.stub(connection.createQuery("SELECT Account.Id, Account.Type, Account.Name FROM Account")).toReturn(query);

        JPQLDirectQueryExecution execution = (JPQLDirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection, Mockito.times(1)).createQuery("SELECT Account.Id, Account.Type, Account.Name FROM Account");
    }

    @Test public void testWithoutMarker() throws Exception {
        String input = "exec native('jpa query')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        EntityManager connection = Mockito.mock(EntityManager.class);

        try {
            JPQLDirectQueryExecution execution = (JPQLDirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
            execution.execute();
            fail("the above should have thrown exception");
        } catch (TranslatorException e) {
        }
    }

    @Test public void testDelete() throws Exception {
        String input = "exec native('delete;delete-query')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        EntityManager connection = Mockito.mock(EntityManager.class);

        Query query = Mockito.mock(Query.class);
        Mockito.stub(query.executeUpdate()).toReturn(12);
        Mockito.stub(connection.createQuery("delete-query")).toReturn(query);

        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        JPQLDirectQueryExecution execution = (JPQLDirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection, Mockito.times(1)).createQuery(argument.capture());

        assertEquals("delete-query", argument.getValue());

        assertArrayEquals(new Object[] {12}, (Object[])execution.next().get(0));
    }

    @Test public void testCreate() throws Exception {
        String input = "exec native('create;', 'one')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        EntityManager connection = Mockito.mock(EntityManager.class);

        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        Mockito.stub(connection.merge(argument.capture())).toReturn(new String("one"));

        JPQLDirectQueryExecution execution = (JPQLDirectQueryExecution)TRANSLATOR.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection).merge(argument.capture());

        assertEquals("one", argument.getValue());
    }
}
