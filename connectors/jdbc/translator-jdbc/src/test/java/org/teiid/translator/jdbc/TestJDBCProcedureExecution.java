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
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestJDBCProcedureExecution {

    @Test public void testProcedureExecution() throws Exception {
        Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "exec pm2.spTest8a()"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        CallableStatement cs = Mockito.mock(CallableStatement.class);
        Mockito.when(cs.getUpdateCount()).thenReturn(-1);
        Mockito.when(cs.getInt(1)).thenReturn(5);
        Mockito.when(connection.prepareCall("{call spTest8a(?)}")).thenReturn(cs); //$NON-NLS-1$
        JDBCExecutionFactory ef = new JDBCExecutionFactory();

        JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class),  ef);
        procedureExecution.execute();
        assertEquals(Arrays.asList(5), procedureExecution.getOutputParameterValues());
        Mockito.verify(cs, Mockito.times(1)).registerOutParameter(1, Types.INTEGER);
    }
    @Test public void testProcedureExecution1() throws Exception {
        Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "exec pm2.spTest8(1)"); //$NON-NLS-1$
        Connection connection = Mockito.mock(Connection.class);
        CallableStatement cs = Mockito.mock(CallableStatement.class);
        Mockito.when(cs.getUpdateCount()).thenReturn(-1);
        Mockito.when(cs.getInt(2)).thenReturn(5);
        Mockito.when(connection.prepareCall("{call spTest8(?,?)}")).thenReturn(cs); //$NON-NLS-1$
        JDBCExecutionFactory config = new JDBCExecutionFactory();

        JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class), config);
        procedureExecution.execute();
        assertEquals(Arrays.asList(5), procedureExecution.getOutputParameterValues());
        Mockito.verify(cs, Mockito.times(1)).registerOutParameter(2, Types.INTEGER);
    }

}
