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

import static org.junit.Assert.*;

import java.util.Properties;

import javax.activation.DataSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.WSConnection;

@SuppressWarnings("nls")
public class TestWSTranslator {

	@Test public void testPre81Procedure() throws Exception {
		WSExecutionFactory ef = new WSExecutionFactory();
    	MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		ef.getMetadata(mf, null);
		Procedure p = mf.getSchema().getProcedure(WSExecutionFactory.INVOKE_HTTP);
		assertEquals(6, p.getParameters().size());
		p = mf.getSchema().getProcedure("invoke");
		assertEquals(6, p.getParameters().size());
		p.getParameters().remove(5);
		
		TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
		RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
		WSConnection mockConnection = Mockito.mock(WSConnection.class);
		Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
		Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(Mockito.mock(DataSource.class));
		Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
		CommandBuilder cb = new CommandBuilder(tm);
		
		Call call = (Call)cb.getCommand("call invokeHttp('GET', null, null, true)");
		BinaryWSProcedureExecution pe = new BinaryWSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
		pe.execute();
		pe.getOutputParameterValues();

		mockConnection = Mockito.mock(WSConnection.class);
		mockDispatch = Mockito.mock(Dispatch.class);
		Mockito.stub(mockDispatch.invoke(Mockito.any(StAXSource.class))).toReturn(Mockito.mock(StAXSource.class));
		Mockito.stub(mockConnection.createDispatch(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
		call = (Call)cb.getCommand("call invoke()");
		WSProcedureExecution wpe = new WSProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
		wpe.execute();
		wpe.getOutputParameterValues();
	}
	
}
