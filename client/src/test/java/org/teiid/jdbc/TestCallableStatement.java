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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.security.LogonResult;
import org.teiid.core.types.DataTypeManager;
import org.teiid.net.ServerConnection;

@SuppressWarnings("nls")
public class TestCallableStatement {

    @Test public void testWasNull() throws Exception {
        CallableStatementImpl mmcs = getCallableStatement();

        Map<Integer, Integer> params = new HashMap<Integer, Integer>();
        mmcs.outParamIndexMap = params;
        params.put(Integer.valueOf(1), Integer.valueOf(1));
        params.put(Integer.valueOf(2), Integer.valueOf(2));
        ResultSetImpl rs = Mockito.mock(ResultSetImpl.class);
        mmcs.resultSet = rs;
        Mockito.stub(rs.getOutputParamValue(1)).toReturn(null);
        Mockito.stub(rs.getOutputParamValue(2)).toReturn(Boolean.TRUE);
        mmcs.getBoolean(1);
        assertTrue(mmcs.wasNull());
        assertTrue(mmcs.getBoolean(2));
        assertFalse(mmcs.wasNull());
    }

    @Test public void testGetOutputParameter() throws Exception {
        CallableStatementImpl mmcs = getCallableStatement();

        RequestMessage request = new RequestMessage();
        request.setExecutionId(1);
        ResultsMessage resultsMsg = new ResultsMessage();
        List<?>[] results = new List[] {Arrays.asList(null, null, null), Arrays.asList(null, 1, 2)};
        resultsMsg.setResults(results);
        resultsMsg.setColumnNames(new String[] { "IntNum", "Out1", "Out2" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        resultsMsg.setDataTypes(new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER });
        resultsMsg.setFinalRow(results.length);
        resultsMsg.setLastRow(results.length);
        resultsMsg.setFirstRow(1);
        resultsMsg.setParameters(Arrays.asList(new ParameterInfo(ParameterInfo.RESULT_SET, 1), new ParameterInfo(ParameterInfo.OUT, 1), new ParameterInfo(ParameterInfo.OUT, 1)));
        mmcs.createResultSet(resultsMsg);
        assertEquals(1, mmcs.getInt(1));
        assertEquals(2, mmcs.getInt(2));
        assertEquals(1, mmcs.getInt("Out1"));
        assertEquals(2, mmcs.getInt("Out2"));
    }

    @Test public void testUnknownIndex() throws Exception {
        CallableStatementImpl mmcs = getCallableStatement();

        mmcs.outParamIndexMap = new HashMap<Integer, Integer>();

        try {
            mmcs.getBoolean(0);
            fail("expected exception"); //$NON-NLS-1$
        } catch (SQLException e) {
            assertEquals("Parameter 0 was not found.", e.getMessage());
        }
    }

    @Test public void testSetLobs() throws Exception {
        CallableStatementImpl mmcs = getCallableStatement();
        mmcs.paramsByName = new TreeMap<String, Integer>();
        mmcs.paramsByName.put("foo", 2);
        mmcs.paramsByName.put("bar", 4);

        mmcs.setBlob(1, Mockito.mock(InputStream.class));
        mmcs.setBlob("foo", Mockito.mock(InputStream.class));
        mmcs.setNClob(3, Mockito.mock(Reader.class));
        mmcs.setBlob("bar", Mockito.mock(InputStream.class), 1);
        mmcs.setClob(5, Mockito.mock(Reader.class));

        List<Object> params = mmcs.getParameterValues();
        assertTrue(params.get(0) instanceof Blob);
        assertTrue(params.get(1) instanceof Blob);
        assertTrue(params.get(2) instanceof Clob);
        assertTrue(params.get(3) instanceof Blob);
        assertTrue(params.get(4) instanceof Clob);
    }

    private CallableStatementImpl getCallableStatement() throws SQLException {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        ServerConnection sc = Mockito.mock(ServerConnection.class);

        Mockito.stub(sc.getLogonResult()).toReturn(new LogonResult());
        Mockito.stub(conn.getServerConnection()).toReturn(sc);

        CallableStatementImpl mmcs = new CallableStatementImpl(conn, "{?=call x(?)}", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        return mmcs;
    }

}
