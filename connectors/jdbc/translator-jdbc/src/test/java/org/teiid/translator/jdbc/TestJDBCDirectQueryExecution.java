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

package org.teiid.translator.jdbc;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.dqp.internal.datamgr.FakeExecutionContextImpl;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;

import java.sql.*;

import static org.junit.Assert.assertArrayEquals;

@SuppressWarnings("nls")
public class TestJDBCDirectQueryExecution {

    @Test public void testSelectExecution() throws Exception {
        Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('select * from Source')"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        Statement stmt = Mockito.mock(Statement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);

        Mockito.when(stmt.getUpdateCount()).thenReturn(-1);
        Mockito.when(stmt.getResultSet()).thenReturn(rs);
        Mockito.when(rs.getMetaData()).thenReturn(rsm);
        Mockito.when(rsm.getColumnCount()).thenReturn(2);
        Mockito.when(connection.createStatement()).thenReturn(stmt); //$NON-NLS-1$
        Mockito.when(stmt.execute("select * from Source")).thenReturn(true);
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getObject(1)).thenReturn(5);
        Mockito.when(rs.getObject(2)).thenReturn("five");
        DatabaseMetaData dbmd = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(dbmd);

        JDBCExecutionFactory ef = new JDBCExecutionFactory();
        ef.setSupportsDirectQueryProcedure(true);
        ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  Mockito.mock(ExecutionContext.class), Mockito.mock(RuntimeMetadata.class), connection);
        execution.execute();
        assertArrayEquals(new Object[] {5, "five"}, (Object[])execution.next().get(0));
    }

    @Test public void testPrepareExecution() throws Exception {
        Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('select * from Source where e1 = ?', 2)"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);

        Mockito.when(stmt.getUpdateCount()).thenReturn(-1);
        Mockito.when(stmt.getResultSet()).thenReturn(rs);
        Mockito.when(stmt.execute()).thenReturn(true);
        Mockito.when(rs.getMetaData()).thenReturn(rsm);
        Mockito.when(rsm.getColumnCount()).thenReturn(2);
        Mockito.when(connection.prepareStatement("select * from Source where e1 = ?")).thenReturn(stmt); //$NON-NLS-1$
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getObject(1)).thenReturn(5);
        Mockito.when(rs.getObject(2)).thenReturn("five");
        DatabaseMetaData dbmd = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(dbmd);

        JDBCExecutionFactory ef = new JDBCExecutionFactory();
        ef.setSupportsDirectQueryProcedure(true);
        ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  new FakeExecutionContextImpl(), Mockito.mock(RuntimeMetadata.class), connection);
        execution.execute();
        Mockito.verify(stmt).setObject(1, 2);
        assertArrayEquals(new Object[] {5, "five"}, (Object[])execution.next().get(0));
    }

    @Test public void testPrepareUpdateCount() throws Exception {
        Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('update source set e1=? where e2 = ?', 2, 'foo')"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);

        Mockito.when(stmt.getUpdateCount()).thenReturn(-1);
        Mockito.when(stmt.getUpdateCount()).thenReturn(5);
        Mockito.when(stmt.execute()).thenReturn(false);
        Mockito.when(rs.getMetaData()).thenReturn(rsm);
        Mockito.when(rsm.getColumnCount()).thenReturn(2);
        Mockito.when(connection.prepareStatement("update source set e1=? where e2 = ?")).thenReturn(stmt); //$NON-NLS-1$
        DatabaseMetaData dbmd = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(dbmd);

        JDBCExecutionFactory ef = new JDBCExecutionFactory();
        ef.setSupportsDirectQueryProcedure(true);
        ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  new FakeExecutionContextImpl(), Mockito.mock(RuntimeMetadata.class), connection);
        execution.execute();
        assertArrayEquals(new Object[] {5}, (Object[])execution.next().get(0));
    }
}
