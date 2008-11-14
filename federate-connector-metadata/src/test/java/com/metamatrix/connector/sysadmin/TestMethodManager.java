/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.sysadmin;

import java.lang.reflect.Method;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.connector.sysadmin.extension.command.ProcedureCommand;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

/**
 * <p>Test cases for {@link TestMethodManager} </p>
 */
public class TestMethodManager extends TestCase {

    private RuntimeMetadata metadata;
    private CommandBuilder commandBuilder;
    

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestMethodManager( String name ) {
        super( name );
    }

    private ProcedureCommand getProcedure(String method, String query) throws Exception {
        
        QueryMetadataInterfaceBuilder builder = FakeObjectUtil.createBuilder(method);
        metadata = builder.getRuntimeMetadata();
        commandBuilder = new CommandBuilder(builder.getQueryMetadata());
        
        return new ProcedureCommand(metadata, (IProcedure) commandBuilder.getCommand(query));
    }    
    
    
     
    public void testMethod1() {
        performTest(FakeObjectUtil.METHOD1, FakeObjectUtil.QUERY_METHOD1, 0);
       
    }   
    
    public void testMethod21() {
        performTest(FakeObjectUtil.METHOD21, FakeObjectUtil.QUERY_METHOD21, 0);
       
    }    
    
    public void testMethod22() {
        performTest(FakeObjectUtil.METHOD22, FakeObjectUtil.QUERY_METHOD22, 3);
       
    }    
        
    public void testMethod4() {
        performTest(FakeObjectUtil.METHOD4, FakeObjectUtil.QUERY_METHOD4, 3);
       
    }   
    
    public void testMethod5() {
        performTest(FakeObjectUtil.METHOD5, FakeObjectUtil.QUERY_METHOD5, 1);
       
    }     
    
    private void performTest(String method, String query, int resultcnt) {
        Method m = null;
        try {
            SysAdminMethodManager mgr = new SysAdminMethodManager(ITestMethods.class);
            
            ProcedureCommand pc = getProcedure(method, query);
            m = mgr.getMethodFromAPI(pc);
            
            if (!FakeObjectUtil.isValidMethod(m) ) {
                fail("Did find the right method for command " + query);  //$NON-NLS-1$
            }

            if (pc.getCommand() == null) {
                fail("The IProcedure command was not stored");  //$NON-NLS-1$
            }
                           
            pc.getCriteria();
            pc.getCriteriaTypes();
            pc.getCriteriaValues();
            
            if (pc.getGroupNameInSource() == null) {
                fail("Group NameInSource should have been null"); //$NON-NLS-1$
            }
                        
            if (resultcnt > 0) {
                if (pc.getResultColumnNames() == null || pc.getResultColumnNames().length == 0)  {
                        fail("No results are returned for this query " + query); //$NON-NLS-1$
                }              
            }
            
            if (resultcnt == 0) {
                if (pc.getResultColumnNames() != null && pc.getResultColumnNames().length > 0)  {
                    fail("Results columns where specified when result cnt is zero for query " + query);  //$NON-NLS-1$
                }
            }            
            

            
        } catch (Exception err) {
            System.out.println(err.getMessage());
            fail(err.getMessage());
        }           


    }

  

    
//    private void testMethod(String tableName, String tableNameInSource, Object[] objectArray, String query, Class type,
//       String tableDefiningMethodName) {
//       defineMetadata(tableName, tableNameInSource, type, tableDefiningMethodName);
//       List objects = Arrays.asList(objectArray);
////       connector = new FakeObjectConnector(objects);
//       Properties properties = new Properties();
//       ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, null, null);
//       Method m = null;
//
//       try {
// //          connector.initialize(environment);
// //          connector.start();
// //          Connection connection = connector.getConnection(null);
//           QueryMetadataInterface metadata = metadataBuilder.getQueryMetadata();
//           CommandBuilder commandBuilder = new CommandBuilder(metadata);
//           IProcedure command = (IProcedure) commandBuilder.getCommand(query);
//
//           RuntimeMetadata runtimeMetadata = metadataBuilder.getRuntimeMetadata();
//           
//           SysAdminMethodManager mgr = new SysAdminMethodManager(getInstance().getClass());
//           
//           try {
//               ProcedureCommand pc = new ProcedureCommand(runtimeMetadata, command);
//               m = mgr.getMethodFromAPI(pc);
//           } catch (Exception err) {
//               fail(err.getMessage());
//           }           
//
//           if (m == null) {
//               fail("Did not find the method for query " + query);
//           }
//       } catch (Exception e) {
//           throw new MetaMatrixRuntimeException(e);
//       }
//   }
//    
//    private void defineMetadata(String procName, String procNameInSource, Class type, String tableDefiningMethodName) {
//        metadataBuilder = new QueryMetadataInterfaceBuilder();
//        metadataBuilder.addProcedure(procName, procNameInSource);
//        metadataBuilder.addMetadataForType(tableName, tableNameInSource, type, tableDefiningMethodName);
//    }    
    
        
} // END CLASS

