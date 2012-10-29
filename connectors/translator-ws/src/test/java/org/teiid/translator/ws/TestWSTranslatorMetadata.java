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

package org.teiid.translator.ws;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import javax.activation.DataSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

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
import org.teiid.translator.WSConnection;

@SuppressWarnings("nls")
public class TestWSTranslatorMetadata {

	@Test public void testMetadata() throws Exception {
		WSExecutionFactory ef = new WSExecutionFactory();
		
		Properties props = new Properties();
		props.setProperty("importer.servicename", "XigniteQuotes");
		props.setProperty("importer.portname", "XigniteQuotesSoap");

		WSConnection mockConnection = Mockito.mock(WSConnection.class);
		Mockito.stub(mockConnection.getWsdl()).toReturn(UnitTestUtil.getTestDataPath()+"/xquotes.wsdl");
		
    	MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		ef.getMetadata(mf, mockConnection);
		
		assertEquals(36, mf.getSchema().getProcedures().size());		
		
		TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
		RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
		
		Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
		Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(Mockito.mock(StAXSource.class));
		Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
		
		CommandBuilder cb = new CommandBuilder(tm);
		
		Call call = (Call)cb.getCommand("call GetFundQuote('<foo/>')");
		assertEquals("SOAP11", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.BINDING, false));
		assertEquals("http://www.xignite.com/services/GetFundQuote", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ACTION, false));
		assertEquals("http://www.xignite.com/xquotes.asmx", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ENDPOINT, false));
		WSProcedureExecution wpe = new WSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
		wpe.execute();
		wpe.getOutputParameterValues();
	}
	
	
	
	@Test public void testHttpMetadata() throws Exception {
		WSExecutionFactory ef = new WSExecutionFactory();
		
		Properties props = new Properties();
		props.setProperty("importer.servicename", "XigniteQuotes");
		props.setProperty("importer.portname", "XigniteQuotesHttpGet");

		WSConnection mockConnection = Mockito.mock(WSConnection.class);
		Mockito.stub(mockConnection.getWsdl()).toReturn(UnitTestUtil.getTestDataPath()+"/xquotes.wsdl");
		
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
		assertEquals("HTTP", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.BINDING, false));
		assertEquals("GET", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ACTION, false));
		assertEquals("http://www.xignite.com/GetFundQuote", call.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ENDPOINT, false));
		WSProcedureExecution wpe = new WSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
		wpe.execute();
		wpe.getOutputParameterValues();
	}	
}
