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

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory;
import com.metamatrix.connector.sysadmin.util.SysAdminUtil;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

public class TestExecutionAndTransation extends TestCase {
    
    private static final String USERNAME="TestUser";//$NON-NLS-1$
    private static final String SESSIONID="5526";//$NON-NLS-1$
    
   
    private RuntimeMetadata metadata;
    
    private SysAdminProcedureExecution procExec;
    private ISysAdminConnectionFactory factory;
    private ConnectorEnvironment env;

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestExecutionAndTransation( String name ) {
        super( name );
    }

    /**
     * Test suite, with one-time setup.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite( TestExecutionAndTransation.class );

        // One-time setup and teardown
        return new TestSetup(suite) ;
    }

    /* 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
//        ISourceTranslator st = new FakeObjectSourceTranslator(FakeObjectUtil.getInstance().getClass()) ;
//        SysAdminSourceTranslator(FakeObjectUtil.getInstance().getClass());
        env = createEnvironment();
//        st.initialize(env);
        factory = SysAdminUtil.createFactory(env, this.getClass().getClassLoader());

    }
    
    private ConnectorEnvironment createEnvironment() {
        Properties properties = new Properties();
        
        properties.setProperty(SysAdminPropertyNames.SYSADMIN_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.sysadmin.FakeSysAdminConnectionFactory"); //$NON-NLS-1$
        ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, null, null);

        return environment;
    }
    
    private IProcedure getProcedure(String method, String query) throws Exception {
        
        QueryMetadataInterfaceBuilder builder = FakeObjectUtil.createBuilder(method);
        metadata = builder.getRuntimeMetadata();
        CommandBuilder commandBuilder = new CommandBuilder(builder.getQueryMetadata());
        
        return (IProcedure) commandBuilder.getCommand(query);
     }    
    
    private SecurityContext getContext() {
        ExecutionContext envContext = EnvironmentUtility.createExecutionContext("vdbName", "1", USERNAME, null, null, SESSIONID, "Connector<IndexTest>", "1000", "1", false);//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        return (SecurityContext)envContext;
    }
       

     public void testMethod1() {
        
        try {
     
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD1, FakeObjectUtil.QUERY_METHOD1);
            procExec = new SysAdminProcedureExecution(metadata,
                                                      factory.getObjectSource(getContext()),
                                                      env );
        
            procExec.execute(iproc, 0);
                       
            Batch b = procExec.nextBatch();
            if (b.getRowCount() > 0) {
                fail("No results should have been returned");//$NON-NLS-1$
            }
            
          if (b.getResults() != null && b.getResults().length > 0) {
              fail("No results should have been returned"); //$NON-NLS-1$
          }            

        } catch (Exception err) {
            err.printStackTrace();
            fail(err.getMessage());
        }
               
    }
     
     public void testMethod21() {
         
         try {
      
             IProcedure iproc = getProcedure(FakeObjectUtil.METHOD21, FakeObjectUtil.QUERY_METHOD21);
             procExec = new SysAdminProcedureExecution(metadata,
                                                       factory.getObjectSource(getContext()),
                                                       env );
         
             procExec.execute(iproc, 0);
                        
             Batch b = procExec.nextBatch();
             if (b.getRowCount() > 0) {
                 fail("No results should have been returned"); //$NON-NLS-1$
             }
             
           if (b.getResults() != null && b.getResults().length > 0) {
               fail("No results should have been returned"); //$NON-NLS-1$
           }            

         } catch (Exception err) {
             err.printStackTrace();
             fail(err.getMessage());
         }
                
     }  
     
     public void testMethod22() {
         
         try {
      
             IProcedure iproc = getProcedure(FakeObjectUtil.METHOD22, FakeObjectUtil.QUERY_METHOD22);
             procExec = new SysAdminProcedureExecution(metadata,
                                                       factory.getObjectSource(getContext()),
                                                       env );
         
             procExec.execute(iproc, 99);
                        
             Batch b = procExec.nextBatch();
             if (b.getRowCount() != 3) {
                 fail("3 Results should have been returned, returned " + b.getRowCount()); //$NON-NLS-1$
             }
             
           if (b.getResults() != null && b.getResults().length != 3) {
               fail("3 results should have been returned, returned " + b.getResults().length); //$NON-NLS-1$
           }            

         } catch (Exception err) {
             err.printStackTrace();
             fail(err.getMessage());
         }
                
     }       
     
     public void testMethod4() {
         
         try {
      
             IProcedure iproc = getProcedure(FakeObjectUtil.METHOD4, FakeObjectUtil.QUERY_METHOD4);
             procExec = new SysAdminProcedureExecution(metadata,
                                                       factory.getObjectSource(getContext()),
                                                       env );
         
             procExec.execute(iproc, 99);
                        
             Batch b = procExec.nextBatch();
             if (b.getRowCount() != 3) {
                 fail("3 Results should have been returned, returned " + b.getRowCount()); //$NON-NLS-1$
             }
             
           if (b.getResults() != null && b.getResults().length != 3) {
               fail("3 results should have been returned, returned " + b.getResults().length); //$NON-NLS-1$
           }            

         } catch (Exception err) {
             err.printStackTrace();
             fail(err.getMessage());
         }
                
     } 

     public void testMethod5() {
         
         try {
      
             IProcedure iproc = getProcedure(FakeObjectUtil.METHOD5, FakeObjectUtil.QUERY_METHOD5);
             procExec = new SysAdminProcedureExecution(metadata,
                                                       factory.getObjectSource(getContext()),
                                                       env );
         
             procExec.execute(iproc, 99);
                        
             Batch b = procExec.nextBatch();
             if (b.getRowCount() != 1) {
                 fail("1 Results should have been returned, returned " + b.getRowCount()); //$NON-NLS-1$
             }
             
           if (b.getResults() != null && b.getResults().length != 1) {
               fail("1 results should have been returned, returned " + b.getResults().length); //$NON-NLS-1$
           }            

         } catch (Exception err) {
             err.printStackTrace();
             fail(err.getMessage());
         }
                
     }  
     
     public void testMethod6() {
         
         try {
      
             IProcedure iproc = getProcedure(FakeObjectUtil.METHOD6, FakeObjectUtil.QUERY_METHOD6);
             
             procExec = new SysAdminProcedureExecution(metadata,
                                                       factory.getObjectSource(getContext()),
                                                       env );
         
             procExec.execute(iproc, 99);
                        
             Batch b = procExec.nextBatch();
             if (b.getRowCount() != 2) {
                 fail("2 Results should have been returned, returned " + b.getRowCount()); //$NON-NLS-1$
             }
             
           if (b.getResults() != null && b.getResults().length != 2) {
               fail("2 results should have been returned, returned " + b.getResults().length); //$NON-NLS-1$
           }            

         } catch (Exception err) {
             err.printStackTrace();
             fail(err.getMessage());
         }
                
     }     
 
     
     protected void printResults(List results) {
         int rowcnt = 1;
         for (Iterator it=results.iterator(); it.hasNext();) {
                List row = (List) it.next();
             int col = 0;
             for (Iterator rit=row.iterator(); rit.hasNext();) {
                 Object o = rit.next();
                 System.out.println("Row:" + rowcnt + " Col:" + col + " Value: " + o.toString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 ++col;
             }
             ++rowcnt;
         }

     }
         
   
} // END CLASS

