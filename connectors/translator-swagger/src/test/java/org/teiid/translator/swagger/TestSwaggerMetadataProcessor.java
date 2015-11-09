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
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getProcuces;

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
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        Set<String> procSet = new HashSet<String>();
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            procSet.add(p.getName());
        }
        
        assertEquals(23, procSet.size());
        
        assertTrue(procSet.contains("addCustomer"));
        assertTrue(procSet.contains("addOneCustomer"));
        assertTrue(procSet.contains("addCustomerList"));
        
        assertTrue(procSet.contains("getCustomers"));
        assertTrue(procSet.contains("getCustomerList"));
        assertTrue(procSet.contains("getCustomerByCity"));
        assertTrue(procSet.contains("getCustomerByCountry"));
        assertTrue(procSet.contains("getCustomerByName"));
        assertTrue(procSet.contains("getCustomerByNumber"));
        assertTrue(procSet.contains("getByNumCityCountry"));
        assertTrue(procSet.contains("size"));
        
        assertTrue(procSet.contains("removeCustomer"));
        assertTrue(procSet.contains("removeCustomerByCity"));
        assertTrue(procSet.contains("removeCustomerByCountry"));
        assertTrue(procSet.contains("removeCustomerByName"));
        assertTrue(procSet.contains("removeCustomerByNumber"));
        assertTrue(procSet.contains("removeCustomerByNumCityCountry"));
        
        assertTrue(procSet.contains("updateCustomer"));
        assertTrue(procSet.contains("updateCustomerByCity"));
        assertTrue(procSet.contains("updateCustomerByCountry"));
        assertTrue(procSet.contains("updateCustomerByName"));
        assertTrue(procSet.contains("updateCustomerByNumber"));
        assertTrue(procSet.contains("updateCustomerByNumCityCountry"));
        
        
    }
    
    @Test
    public void testSwaggerMetadataParams() throws TranslatorException {
        
        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            
            // multiple in parameter
            if(p.getName().equals("getByNumCityCountry")) {
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
            else if(p.getName().equals("getCustomerByCity") || p.getName().equals("getCustomerByCountry") && p.getName().equals("getByNumCityCountry")){
                for(ProcedureParameter param : p.getParameters()) {
                    if(param.getType().equals(Type.In) && !param.getName().equals("headers")) {
                        assertFalse(isPathParam(param));
                    }
                }
            } else if(p.getName().equals("getCustomerByNumber") || p.getName().equals("getCustomerByName")) {
                for(ProcedureParameter param : p.getParameters()) {
                    if(param.getType().equals(Type.In) && !param.getName().equals("headers")) {
                        assertTrue(isPathParam(param));
                    }
                }
            }
            
            // Post parameter
            else if(p.getName().equals("addCustomer") || p.getName().equals("addOneCustomer") || p.getName().equals("addCustomerList")){
                ProcedureParameter param = p.getParameters().get(1);
                assertEquals("body", param.getName());
                assertEquals(Type.In, param.getType());
            }
            
            
            List<ProcedureParameter> params = p.getParameters();
            
            // result parameter
            ProcedureParameter param = params.get(0);
            assertEquals("result", param.getName());
            assertEquals(Type.ReturnValue, param.getType());
            
            // headers parameter
            param = params.get(params.size() - 1);
            assertEquals("headers", param.getName());
            assertEquals(Type.In, param.getType());
                       
        }
    }
    
    @Test
    public void testAnnotation() throws TranslatorException{

        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        for(Procedure p : mf.getSchema().getProcedures().values()){
            if(p.getName().equals("addCustomer")){
                assertEquals("Add a Customer", p.getAnnotation());
            } else if (p.getName().equals("getByNumCityCountry")){
                assertEquals("get customer by Number, City, Country as return xml/json", p.getAnnotation());
            }
        }
    }
    
    @Test
    public void testMediaTypes() throws TranslatorException{

        SwaggerExecutionFactory ef = new SwaggerExecutionFactory();
        
        Properties props = new Properties();

        WSConnection mockConnection = Mockito.mock(WSConnection.class);
        Mockito.stub(mockConnection.getSwagger()).toReturn(new File(UnitTestUtil.getTestDataPath()+"/swagger.json").getAbsolutePath());
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        ef.getMetadata(mf, mockConnection);
        
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            Set<String> produceMediaTypes = getProcuces(p);
            if(p.getName().equals("getCustomers")){
                assertTrue(produceMediaTypes.contains("application/xml"));
                assertFalse(produceMediaTypes.contains("application/json"));
            } else if (p.getName().equals("getByNumCityCountry")) {
                assertTrue(produceMediaTypes.contains("application/xml"));
                assertTrue(produceMediaTypes.contains("application/json"));
            }
        }
    }
    
    
    
    @Ignore
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
      
        String[] calls = new String[] {"EXEC getByNumCityCountry('161', 'Burlingame', 'USA', jsonObject('application/json' as ContentType, jsonArray('gzip', 'deflate') as \"Accept-Encoding\"))",
                "EXEC getCustomers()",
                "EXEC getCustomerList()",
                "EXEC getCustomerByCity('Burlingame')",
                "EXEC getCustomerByCountry('USA')",
                "EXEC getCustomerByName('Technics Stores Inc.', jsonObject('application/json' as Accept))",
                "EXEC getCustomerByNumber('161', jsonObject('application/json' as Accept))",
                "EXEC size(jsonObject('application/json' as Accept))"};
        
        for(String c : calls) {
            Call call = (Call)cb.getCommand(c);
            SwaggerProcedureExecution wpe = new SwaggerProcedureExecution(call, rm, Mockito.mock(ExecutionContext.class), ef, mockConnection);
            wpe.execute();
            wpe.getOutputParameterValues();
        }
      
    }
    
    
}
