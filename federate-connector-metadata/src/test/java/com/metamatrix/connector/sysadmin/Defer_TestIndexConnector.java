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

import java.io.Serializable;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.connector.metadata.IndexConnector;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

public class Defer_TestIndexConnector extends TestCase{
//    private FakeApplicationService fakeApplicationService = null;
    
    private static final String USERNAME="TestUser";//$NON-NLS-1$
    private static final String SESSIONID="5525";//$NON-NLS-1$
    
    RuntimeMetadata runtimeMetadata;
    
    // save the initial system properties settings
    private Properties sysprops = PropertiesUtils.clone(System.getProperties(), false);
  
    public Defer_TestIndexConnector(String name) {
        super(name);
        
    }
    
    
    
    /** 
     * @see junit.framework.TestCase#setUp()
     * @since 4.3
     */
    protected void setUp() throws Exception {
        super.setUp();
        // must set the system property to indicate the current running metamatrix vm, otherwise
        // the system admin feature is not enabled.
        System.setProperty(VMNaming.VM_NAME_PROPERTY, "TestVM");//$NON-NLS-1$
        
    }

    /* 
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        // must remove the vm setting property
         System.setProperties(sysprops);

    }

    private IProcedure getProcedure(String method, String query) throws Exception {
        
        QueryMetadataInterfaceBuilder builder = FakeObjectUtil.createBuilder(method);
        runtimeMetadata = builder.getRuntimeMetadata();
        CommandBuilder commandBuilder = new CommandBuilder(builder.getQueryMetadata());
        
        return (IProcedure) commandBuilder.getCommand(query);
     } 

    public void testAuthorized() {
        IndexConnector connector = new IndexConnector();

        try {
            connector.initialize(createEnvironment());
            connector.start();
            Connection connection = connector.getConnection(helpGetSecurityContext());
            
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD1, FakeObjectUtil.QUERY_METHOD1);
            
            
            ProcedureExecution execution = (ProcedureExecution) connection.createExecution(ConnectorCapabilities.EXECUTION_MODE.PROCEDURE, getContext(), runtimeMetadata); 
            execution.execute(iproc, 2);
            Batch b = execution.nextBatch();
            if (b.getRowCount() > 0) {
                fail("No results should have been returned");//$NON-NLS-1$
            }
            
            if (b.getResults() != null && b.getResults().length > 0) {
              fail("No results should have been returned"); //$NON-NLS-1$
            }            
            execution.close();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    public void testAuthorized2() {
        IndexConnector connector = new IndexConnector();

        try {
            connector.initialize(createEnvironment());
            connector.start();
            Connection connection = connector.getConnection(helpGetSecurityContext());
            
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD22, FakeObjectUtil.QUERY_METHOD22);
            
            ProcedureExecution execution = (ProcedureExecution) connection.createExecution(ConnectorCapabilities.EXECUTION_MODE.PROCEDURE, getContext(), runtimeMetadata); 
            execution.execute(iproc, 2);
            Batch b = execution.nextBatch();
            if (b.getRowCount() < 1) {
                fail("Results should have been returned");//$NON-NLS-1$
            }
            
            if (b.getResults() != null && b.getResults().length > 0) {
            } else {
              fail("No results should have been returned"); //$NON-NLS-1$
            }            
            execution.close();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }    
    
    public void testAuthorized3() {
        IndexConnector connector = new IndexConnector();

        try {
            connector.initialize(createEnvironment());
            connector.start();
            Connection connection = connector.getConnection(helpGetSecurityContext());
            
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD6, FakeObjectUtil.QUERY_METHOD6);
            
            ProcedureExecution execution = (ProcedureExecution) connection.createExecution(ConnectorCapabilities.EXECUTION_MODE.PROCEDURE, getContext(), runtimeMetadata); 
            execution.execute(iproc, 2);
            Batch b = execution.nextBatch();
            if (b.getRowCount() < 1) {
                fail("Results should have been returned");//$NON-NLS-1$
            }
            
            if (b.getResults() != null && b.getResults().length > 0) {
            } else {
              fail("No results should have been returned"); //$NON-NLS-1$
            }            
            execution.close();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }
    
    public void testAuthorized4() {
        IndexConnector connector = new IndexConnector();

        try {
            connector.initialize(createEnvironment());
            connector.start();
            Connection connection = connector.getConnection(helpGetSecurityContext());
            
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD5, FakeObjectUtil.QUERY_METHOD5);
            
            ProcedureExecution execution = (ProcedureExecution) connection.createExecution(ConnectorCapabilities.EXECUTION_MODE.PROCEDURE, getContext(), runtimeMetadata); 
            execution.execute(iproc, 2);
            Batch b = execution.nextBatch();
            if (b.getRowCount() < 1) {
                fail("Results should have been returned");//$NON-NLS-1$
            }
            
            if (b.getResults() != null && b.getResults().length > 0) {
            } else {
              fail("No results should have been returned"); //$NON-NLS-1$
            }            
            execution.close();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }       
    
    public void testUnAuthorized() {
        IndexConnector connector = new IndexConnector();
        ProcedureExecution execution = null;
        try {
            connector.initialize(createEnvironment());
            connector.start();
            Connection connection = connector.getConnection(helpGetSecurityContext());
            
            IProcedure iproc = getProcedure(FakeObjectUtil.METHOD7, FakeObjectUtil.QUERY_METHOD7);
                      
            execution = (ProcedureExecution) connection.createExecution(ConnectorCapabilities.EXECUTION_MODE.PROCEDURE, getContext(), runtimeMetadata); 
            execution.execute(iproc, 2);
            fail("AuthorizationException should have been thrown");//$NON-NLS-1$
        } catch (ConnectorException au) {
            if (au.getMessage().indexOf("administrative") == -1) {//$NON-NLS-1$
                fail("AuthorizationException should have been thrown indicating admin role was needed");//$NON-NLS-1$
            }
             //  auth exception is the cause of this exception
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        } finally {
            if (execution != null) {
                try {
                    execution.close();
                } catch (Exception err) {
                }
            }
        }
    }

    
    
     
    private ConnectorEnvironment createEnvironment() {
        Properties properties = new Properties();
        
        properties.setProperty(SysAdminPropertyNames.SYSADMIN_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.sysadmin.FakeSysAdminConnectionFactory"); //$NON-NLS-1$
        ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, new SysLogger(false), null);

        return environment;
    }
        
    private SecurityContext getContext() {
        ExecutionContext envContext = EnvironmentUtility.createExecutionContext("vdbName", "1", USERNAME, null, null, //$NON-NLS-1$ //$NON-NLS-2$
                                                                                SESSIONID, "Connector<Index>", "1000", "1", false);//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return (SecurityContext)envContext;

    }    
    
    
    private SecurityContext helpGetSecurityContext() {
        return new SecurityContext() {
            public String getVirtualDatabaseName() {
                return "testName"; //$NON-NLS-1$
            }

            public String getVirtualDatabaseVersion() {
                return "testVersion"; //$NON-NLS-1$
            }

            public String getUser() {
                return USERNAME;
            }

            public Serializable getTrustedPayload() {
                return null;
            }

            public Serializable getExecutionPayload() {
                return null;
            }

            public String getRequestIdentifier() {
                return null;
            }

            public String getPartIdentifier() {
                return null;
            }

			public String getConnectionIdentifier() {
				return SESSIONID;
			}

			public boolean useResultSetCache() {
				return false;
			}

            public String getConnectorIdentifier() {
                return null;
            }

            public String getExecutionCountIdentifier() {
                return null;
            }

            public void keepExecutionAlive(boolean alive) {
            }
        };
    }  
    

}

