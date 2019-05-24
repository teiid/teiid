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

package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Parameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.datastax.driver.core.ResultSetFuture;

@SuppressWarnings("nls")
public class TestUpdates {

    @Test public void testBatchedUpdate() throws TranslatorException {
        CassandraExecutionFactory cef = new CassandraExecutionFactory();

        String input = "insert into pm1.g1 (e1) values ('a')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        Command command1 = util.parseCommand("update pm1.g1 set e1 = 'b'");

        command = new BatchedUpdates(Arrays.asList(command, command1));

        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.stub(rsf.isDone()).toReturn(true);

        Mockito.stub(connection.executeBatch(Arrays.asList("INSERT INTO g1 (e1) VALUES ('a')", "UPDATE g1 SET e1 = 'b'"))).toReturn(rsf);

        UpdateExecution execution = (UpdateExecution)cef.createExecution(command, ec, rm, connection);
        execution.execute();
        assertArrayEquals(new int[] {2}, execution.getUpdateCounts());

        Mockito.verify(connection).executeBatch(Arrays.asList("INSERT INTO g1 (e1) VALUES ('a')", "UPDATE g1 SET e1 = 'b'"));
    }

    @Test public void testBulkUpdate() throws Exception {
        CassandraExecutionFactory cef = new CassandraExecutionFactory();

        String input = "insert into pm1.g1 (e1) values ('a')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        Insert insert = (Insert)command;
        Parameter p = new Parameter();
        p.setType(String.class);
        p.setValueIndex(0);
        ((ExpressionValueSource)insert.getValueSource()).getValues().set(0, p);
        insert.setParameterValues(Arrays.asList(Arrays.asList("a"), Arrays.asList("b")).iterator());

        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.stub(rsf.isDone()).toReturn(true);

        Mockito.stub(connection.executeBatch(Mockito.eq("INSERT INTO g1 (e1) VALUES (?)"), (List<Object[]>) Mockito.anyObject())).toReturn(rsf);

        UpdateExecution execution = (UpdateExecution)cef.createExecution(command, ec, rm, connection);
        execution.execute();
        assertArrayEquals(new int[] {2}, execution.getUpdateCounts());
    }

}
