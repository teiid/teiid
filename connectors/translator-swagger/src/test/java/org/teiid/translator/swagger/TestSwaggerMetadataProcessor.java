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
package org.teiid.translator.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isPathParam;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.activation.DataSource;
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
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.swagger.SwaggerExecutionFactory;
import org.teiid.translator.swagger.SwaggerProcedureExecution;

@SuppressWarnings("nls")
public class TestSwaggerMetadataProcessor {
    
    @Test
    public void testSwaggerMetadata() throws TranslatorException {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn("http://localhost:8080/swagger.json");
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        Set<String> procSet = new HashSet<String>();
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            procSet.add(p.getName());
        }
        
        assertEquals(16, procSet.size());
        
        assertTrue(procSet.contains("customer"));
        assertTrue(procSet.contains("customer_customerList"));
        assertTrue(procSet.contains("customer_getAll"));
        assertTrue(procSet.contains("customer_getByCity"));
        assertTrue(procSet.contains("customer_getByCountry"));
        assertTrue(procSet.contains("customer_getByNumCityCountry"));
        assertTrue(procSet.contains("customer_getByName"));
        assertTrue(procSet.contains("customer_getByNumber"));
        assertTrue(procSet.contains("customer_status"));
        assertTrue(procSet.contains("customer_update"));
        assertTrue(procSet.contains("customer_delete"));
        assertTrue(procSet.contains("customer_deleteByNumber"));
        assertTrue(procSet.contains("customer_deleteByName"));
        assertTrue(procSet.contains("customer_deleteByCity"));
        assertTrue(procSet.contains("customer_deleteByCountry"));
        assertTrue(procSet.contains("customer_deleteByNumCityCountry"));
        
    }
    
    @Test
    public void testSwaggerMetadataParams() throws TranslatorException {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn("http://localhost:8080/swagger.json");
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            
            // multiple in parameter
            if(p.getName().equals("customer_getByNumCityCountry")) {
                List<ProcedureParameter> params = p.getParameters();
                Set<String> paramSet = new HashSet<String>();
                for(ProcedureParameter param : params) {
                    if(param.getType().equals(Type.In)  && !param.getName().equals("headers")) {
                        paramSet.add(param.getName());
                        assertFalse(isPathParam(param));
                    }
                }
                assertEquals(3, paramSet.size());
                assertTrue(paramSet.contains("customernumber"));
                assertTrue(paramSet.contains("city"));
                assertTrue(paramSet.contains("country"));
            }
            
            // QueryParameter and  PathParameter
            if(p.getName().equals("customer_getByCity") && p.getName().equals("customer_getByCountry") && p.getName().equals("customer_getByNumCityCountry")){
                for(ProcedureParameter param : p.getParameters()) {
                    if(param.getType().equals(Type.In) && !param.getName().equals("headers")) {
                        assertFalse(isPathParam(param));
                    }
                }
            } else if(p.getName().equals("customer_getByNumber") && p.getName().equals("customer_getByName")) {
                for(ProcedureParameter param : p.getParameters()) {
                    if(param.getType().equals(Type.In) && !param.getName().equals("headers")) {
                        assertTrue(isPathParam(param));
                    }
                }
            }
            
            List<ProcedureParameter> params = p.getParameters();
            
            // result parameter
            ProcedureParameter param = params.get(0);
            assertEquals("result", param.getName());
            assertEquals(Type.ReturnValue, param.getType());
            
            // headers parameter
            param = params.get(params.size() - 2);
            assertEquals("headers", param.getName());
            assertEquals(Type.In, param.getType());
            
            // contentType parameter
            param = params.get(params.size() - 1);
            assertEquals("contentType", param.getName());
            assertEquals(Type.Out, param.getType());            
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSwaggerExecution() throws Exception {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn("http://localhost:8080/swagger.json");
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
      
        Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
        StAXSource source = Mockito.mock(StAXSource.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(source);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
      
        CommandBuilder cb = new CommandBuilder(tm);
        
        Call call = (Call)cb.getCommand("EXEC customer_getByNumCityCountry('161', 'Burlingame', 'USA')");
        SwaggerProcedureExecution wpe = new SwaggerProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
        wpe.execute();
        wpe.getOutputParameterValues();
      
        String[] calls = new String[] {"EXEC customer_customerList()",
                                       "EXEC customer_getAll()",
                                       "EXEC customer_getByNumber('161')",
                                       "EXEC customer_getByCity('Burlingame')",
                                       "EXEC customer_getByCountry('USA')"};
      
    }
    
    @Test 
    public void testSwaggerMetadata_1() throws Exception {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        assertEquals(7, mf.getSchema().getProcedures().size());
        
        Set<String> procSet = new HashSet<String>();
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            procSet.add(p.getName());
        }
        
        assertTrue(procSet.contains("customer_customerList"));
        assertTrue(procSet.contains("customer_getAll"));
        assertTrue(procSet.contains("customer_getByCity"));
        assertTrue(procSet.contains("customer_getByCountry"));
        assertTrue(procSet.contains("customer_getByName"));
        assertTrue(procSet.contains("customer_getByNumber"));
        
        
    }
    
    @SuppressWarnings("unchecked")
    @Ignore
    @Test
    public void testSwaggerExecution_1() throws Exception {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb");
        RuntimeMetadataImpl rm = new RuntimeMetadataImpl(tm);
      
        Dispatch<Object> mockDispatch = Mockito.mock(Dispatch.class);
        StAXSource source = Mockito.mock(StAXSource.class);
        Mockito.stub(mockDispatch.invoke(Mockito.any(DataSource.class))).toReturn(source);
        Mockito.stub(mockConnection.createDispatch(Mockito.any(Class.class), Mockito.any(Service.Mode.class))).toReturn(mockDispatch);
      
        CommandBuilder cb = new CommandBuilder(tm);
      
        String[] calls = new String[] {"EXEC customer_customerList()",
                                       "EXEC customer_getAll()",
                                       "EXEC customer_getByNumber('161')",
                                       "EXEC customer_getByCity('Burlingame')",
                                       "EXEC customer_getByCountry('USA')"};
      
        for(String c : calls) {
            Call call = (Call)cb.getCommand(c);
            SwaggerProcedureExecution wpe = new SwaggerProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
            wpe.execute();
            wpe.getOutputParameterValues();
        }
    }

}
