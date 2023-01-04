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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.List;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("nls")
public class TestNativeCassandra {

    @Test public void testDirect() throws TranslatorException {
        CassandraExecutionFactory cef = new CassandraExecutionFactory();
        cef.setSupportsDirectQueryProcedure(true);

        String input = "call native('select $1', 'a')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.when(rsf.isDone()).thenReturn(true);
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rsf.getUninterruptibly()).thenReturn(rs);
        Row row = Mockito.mock(Row.class);
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.when(row.getColumnDefinitions()).thenReturn(cd);
        Mockito.when(rs.one()).thenReturn(row).thenReturn(null);

        Mockito.when(connection.executeQuery("select 'a'")).thenReturn(rsf);

        ResultSetExecution execution = (ResultSetExecution)cef.createExecution(command, ec, rm, connection);
        execution.execute();

        List<?> vals = execution.next();
        assertTrue(vals.get(0) instanceof Object[]);
    }

    @Test public void testNativeQuery() throws Exception {
        CassandraExecutionFactory cef = new CassandraExecutionFactory();
        cef.setSupportsDirectQueryProcedure(true);

        String input = "call proc('a', 1)";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign procedure proc (in x string, in y integer) options (\"teiid_rel:native-query\" 'delete from $1 where $2')", "x", "y");
        TranslationUtility util = new TranslationUtility(metadata);
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.when(connection.executeQuery("delete from 'a' where 1")).thenReturn(rsf);

        Execution execution = cef.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection).executeQuery("delete from 'a' where 1");
    }

}
