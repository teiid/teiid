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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;

import javax.activation.DataSource;
import javax.xml.namespace.QName;
import javax.xml.transform.stax.StAXSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;

@SuppressWarnings("nls")
public class TestWSTranslatorMetadata {

    @Test public void testMetadata() throws Exception {
        WSExecutionFactory ef = new WSExecutionFactory();

        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getWsdl()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/xquotes.wsdl").toURI().toURL());
        Mockito.stub(mockConnection.getServiceQName()).toReturn(new QName("http://www.xignite.com/services/", "XigniteQuotes"));
        Mockito.stub(mockConnection.getPortQName()).toReturn(new QName("http://www.xignite.com/services/", "XigniteQuotesSoap"));

        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);

        assertEquals(36, mf.getSchema().getProcedures().size());

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);

        Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
        StAXSource source = Mockito.mock(StAXSource.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(source);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);

        CommandBuilder cb = new CommandBuilder(tm);

        Call call = (Call)cb.getCommand("call GetFundQuote('<foo/>')");
        WSWSDLProcedureExecution wpe = new WSWSDLProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        wpe.execute();
        wpe.getOutputParameterValues();
    }

    @Ignore
    @Test public void testHttpMetadata() throws Exception {
        WSExecutionFactory ef = new WSExecutionFactory();

        Properties props = new Properties();
        props.setProperty("importer.servicename", "XigniteQuotes");
        props.setProperty("importer.portname", "XigniteQuotesHttpGet");

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getWsdl()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/xquotes.wsdl").toURI().toURL());

        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);

        assertEquals(34, mf.getSchema().getProcedures().size());

        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);

        Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(Mockito.mock(StAXSource.class));
        Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);

        CommandBuilder cb = new CommandBuilder(tm);

        Call call = (Call)cb.getCommand("call GetFundQuote('<foo/>')");
        WSProcedureExecution wpe = new WSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        wpe.execute();
        wpe.getOutputParameterValues();
    }
}
