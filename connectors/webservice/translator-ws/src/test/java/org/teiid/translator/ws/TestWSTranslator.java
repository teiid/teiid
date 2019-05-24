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

package org.teiid.translator.ws;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.activation.DataSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.handler.MessageContext;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestWSTranslator {

    @Test public void testPre81Procedure() throws Exception {
        WSExecutionFactory ef = new WSExecutionFactory();
        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        ef.getMetadata(mf, mockConnection);
        Procedure p = mf.getSchema().getProcedure(WSExecutionFactory.INVOKE_HTTP);
        assertEquals(7, p.getParameters().size());
        p.getParameters().remove(4);
        p.getParameters().remove(5);
        //designer treated the result as an out parameter
        p.getParameters().get(0).setType(Type.Out);
        p.getParameters().add(3,p.getParameters().remove(0));
        for (int i = 0; i < p.getParameters().size(); i++) {
            p.getParameters().get(i).setPosition(i+1);
        }

        p = mf.getSchema().getProcedure("invoke");
        assertEquals(6, p.getParameters().size());
        p.getParameters().remove(5);
        //designer treated the result as an out parameter
        p.getParameters().get(0).setType(Type.Out);
        p.getParameters().add(p.getParameters().remove(0));
        for (int i = 0; i < p.getParameters().size(); i++) {
            p.getParameters().get(i).setPosition(i+1);
        }

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
        Dispatch<Object> mockDispatch = mockDispatch();
        DataSource source = Mockito.mock(DataSource.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(source);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
        CommandBuilder cb = new CommandBuilder(tm);

        Call call = (Call)cb.getCommand("call invokeHttp('GET', null, null)");
        BinaryWSProcedureExecution pe = new BinaryWSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        pe.execute();
        pe.getOutputParameterValues();

        mockConnection = Mockito.mock(WSConnection.class);
        mockDispatch = Mockito.mock(Dispatch.class);
        StAXSource ssource = Mockito.mock(StAXSource.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(StAXSource.class))).toReturn(ssource);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
        call = (Call)cb.getCommand("call invoke()");
        WSProcedureExecution wpe = new WSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        wpe.execute();
        wpe.getOutputParameterValues();
    }

    @Test public void testStreaming() throws Exception {
        WSExecutionFactory ef = new WSExecutionFactory();
        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        ef.getMetadata(mf, mockConnection);
        Procedure p = mf.getSchema().getProcedure(WSExecutionFactory.INVOKE_HTTP);
        assertEquals(7, p.getParameters().size());

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
        Dispatch<Object> mockDispatch = mockDispatch();
        DataSource mock = Mockito.mock(DataSource.class);
        ByteArrayInputStream baos = new ByteArrayInputStream(new byte[100]);
        Mockito.stub(mock.getInputStream()).toReturn(baos);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(mock);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
        CommandBuilder cb = new CommandBuilder(tm);

        Call call = (Call)cb.getCommand("call invokeHttp('GET', null, null, true)");
        BinaryWSProcedureExecution pe = new BinaryWSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        pe.execute();
        List<?> result = pe.getOutputParameterValues();

        Blob b = (Blob) result.get(0);
        assertEquals(100, ObjectConverterUtil.convertToByteArray(b.getBinaryStream()).length);
        try {
            ObjectConverterUtil.convertToByteArray(b.getBinaryStream());
            fail();
        } catch (SQLException e) {
            //should only be able to read once
        }
    }

    @Test public void testHeaders() throws Exception {
        WSExecutionFactory ef = new WSExecutionFactory();
        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        ef.getMetadata(mf, mockConnection);
        Procedure p = mf.getSchema().getProcedure(WSExecutionFactory.INVOKE_HTTP);

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
        Dispatch<Object> mockDispatch = mockDispatch();
        DataSource mock = Mockito.mock(DataSource.class);
        ByteArrayInputStream baos = new ByteArrayInputStream(new byte[100]);
        Mockito.stub(mock.getInputStream()).toReturn(baos);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(mock);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
        CommandBuilder cb = new CommandBuilder(tm);

        Call call = (Call)cb.getCommand("call invokeHttp('GET', null, null, false, '{\"ContentType\":\"application/json\"}')");
        BinaryWSProcedureExecution pe = new BinaryWSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        pe.execute();

        Map<String, List<String>> headers = (Map<String, List<String>>) mockDispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
        assertEquals(Arrays.asList("application/json"), headers.get("ContentType"));
    }

    private Dispatch<Object> mockDispatch() {
        Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(WSConnection.STATUS_CODE, 200);
        Map<String, Object> requestMap = new HashMap<String, Object>();
        Mockito.stub(mockDispatch.getRequestContext()).toReturn(requestMap);
        requestMap.put(MessageContext.HTTP_REQUEST_HEADERS, new LinkedHashMap<String, List<String>>());
        Mockito.stub(mockDispatch.getResponseContext()).toReturn(map);
        return mockDispatch;
    }

    @Test public void testJSONHeader() throws Exception {
        HashMap<String, List<String>> vals = new HashMap<String, List<String>>();
        BinaryWSProcedureExecution.parseHeader(vals, new ClobImpl("{\"a\":1, \"b\":[\"x\",\"y\"]}"));
        assertEquals(2, vals.size());
        assertEquals(vals.get("b"), Arrays.asList("x", "y"));
        assertEquals(vals.get("a"), Arrays.asList("1"));
    }

    @Test(expected=TranslatorException.class) public void testJSONHeaderInvalid() throws Exception {
        HashMap<String, List<String>> vals = new HashMap<String, List<String>>();
        BinaryWSProcedureExecution.parseHeader(vals, new ClobImpl("[]"));
    }

}
