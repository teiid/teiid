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

package com.metamatrix.admin.server;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.ConnectorBinding;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMConnectorType;
import com.metamatrix.admin.objects.MMDQP;
import com.metamatrix.admin.objects.MMExtensionModule;
import com.metamatrix.admin.objects.MMHost;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.admin.objects.MMQueueWorkerPool;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.admin.objects.MMResource;
import com.metamatrix.admin.objects.MMSession;
import com.metamatrix.admin.objects.MMSourceRequest;
import com.metamatrix.admin.objects.MMSystem;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.FakeRegistryUtil;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.vm.controller.VMControllerID;


/** 
 * Unit tests of ServerMonitoringAdminImpl
 * @since 4.3
 */
public class TestServerMonitoringAdminImpl extends TestCase implements IdentifierConstants {
    
    private ServerAdminImpl parent;
    private ServerMonitoringAdminImpl admin;
    
    
    public void setUp() throws Exception {
        System.setProperty("metamatrix.config.none", "true"); //$NON-NLS-1$ //$NON-NLS-2$
        System.setProperty("metamatrix.message.bus.type", "noop.message.bus"); //$NON-NLS-1$ //$NON-NLS-2$

        ClusteredRegistryState registry = FakeRegistryUtil.getFakeRegistry();
        parent = new FakeServerAdminImpl(registry);
        admin = new ServerMonitoringAdminImpl(parent, registry);        
    }
   
    
    /**
     * Tests <code>ServerMonitoringImpl.trimString()</code> 
     * @since 4.3
     */
    public void testTrimString() {
        String string = "  string "; //$NON-NLS-1$
        assertEquals("string", ServerMonitoringAdminImpl.trimString(string)); //$NON-NLS-1$
        
        string = null;
        assertEquals("", ServerMonitoringAdminImpl.trimString(string)); //$NON-NLS-1$
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getConnectorBindings()</code>.
     * 
     * Fake data is set up in FakeConfiguration.getDeployedComponents(), FakeConfiguration.getConnectorBindings(),
     * FakeRuntimeStateAdminAPIHelper.getService(), FakeRuntimeStateAdminAPIHelper.getServiceBinding()
     * Expects connectorBinding1 to be deployed, but not running. 
     * Expects connectorBinding2 to be deployed and running. 
     * Expects connectorBinding3 to be running, but not deployed. 
     * @since 4.3
     */
    public void testGetConnectorBindings() throws AdminException {
    	
        VMControllerID vmId = new VMControllerID(3, "3.3.3.3"); //$NON-NLS-1$
        try {
			ServiceRegistryBinding binding = admin.registry.getServiceBinding(vmId.getHostName(), vmId.toString(), new ServiceID(3,vmId));
			binding.updateState(ConnectorBinding.STATE_CLOSED);
		} catch (ResourceNotBoundException e1) {
		}

		
        Collection results = admin.getConnectorBindings(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
            MMConnectorBinding binding = (MMConnectorBinding) iter.next();
            if (HOST_1_1_1_1_PROCESS1_CONNECTOR_BINDING1.equals(binding.getIdentifier())) { 
                
                assertEquals("Not Registered", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(false, binding.isRegistered());
                assertEquals(true, binding.isDeployed());
                
                
            } else if (HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2.equals(binding.getIdentifier())){ 
            
                assertEquals("connectorBinding2", binding.getDescription()); //$NON-NLS-1$
                //assertEquals("Open", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(true, binding.isRegistered());
                assertEquals(true, binding.isDeployed());
                Properties properties = binding.getProperties();
                assertEquals("value1", properties.get("prop1")); //$NON-NLS-1$ //$NON-NLS-2$
                assertEquals("value2", properties.get("prop2")); //$NON-NLS-1$ //$NON-NLS-2$            
            } else if (_3_3_3_3_PROCESS3_CONNECTOR_BINDING3.equals(binding.getIdentifier())) { 
                
            	//assertEquals("Closed", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(true, binding.isRegistered());
                assertEquals(true, binding.isDeployed());            
            } else {
                fail("Unexpected d: "+binding.getIdentifier()); //$NON-NLS-1$
            }
        }            
        
        
        
        results = admin.getConnectorBindings(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2);  
        assertEquals(1, results.size());

        results = admin.getConnectorBindings(HOST_2_2_2_2_PROCESS2_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getConnectorBindings(HOST_2_2_2_2_WILDCARD);  
        assertEquals(1, results.size());

        
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getConnectorTypes()</code> 
     * Fake data is setup in FakeConfigurationServiceProxy.
     * @since 4.3
     */
    public void testGetConnectorTypes() throws AdminException {
        Collection results = admin.getConnectorTypes(AdminObject.WILDCARD);  
        List resultsList = new ArrayList(results);
        assertEquals(2, results.size());
        
        MMConnectorType type = (MMConnectorType) resultsList.get(0);
        assertEquals("connectorType1", type.getIdentifier()); //$NON-NLS-1$
        
        
        
        results = admin.getConnectorTypes("connectorType1"); //$NON-NLS-1$
        assertEquals(1, results.size());        
    }
    
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getDQPs()</code>.
     * 
     * Fake data is set up in FakeConfiguration.getDeployedComponents(),
     * FakeRuntimeStateAdminAPIHelper.getService(), FakeRuntimeStateAdminAPIHelper.getServiceBinding()
     * Expects dqp1 to be deployed, but not running. 
     * Expects dqp2 to be deployed and running. 
     * Expects dqp3 to be running, but not deployed. 
     * @since 4.3
     */
    public void testGetDQPs() throws AdminException {
        Collection results = admin.getDQPs(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
            MMDQP dqp = (MMDQP) iter.next();
            if (HOST_1_1_1_1_PROCESS1_DQP1.equals(dqp.getIdentifier())) { 
                assertEquals("Not Registered", dqp.getStateAsString()); //$NON-NLS-1$
                assertEquals(false, dqp.isRegistered());
                assertEquals(true, dqp.isDeployed());
            } else if (HOST_2_2_2_2_PROCESS2_DQP2.equals(dqp.getIdentifier())){ 
                assertEquals("dqp2", dqp.getDescription()); //$NON-NLS-1$
                assertEquals(true, dqp.isRegistered());
                assertEquals(true, dqp.isDeployed());
            } else if (_3_3_3_3_PROCESS3_DQP3.equals(dqp.getIdentifier())) { 
                assertEquals(true, dqp.isRegistered());
                assertEquals(true, dqp.isDeployed());            
            } else {
                fail("Unexpected dqp: "+dqp.getIdentifier()); //$NON-NLS-1$
            }
        }            
        
        
        
        results = admin.getDQPs(HOST_2_2_2_2_PROCESS2_DQP2);  
        assertEquals(1, results.size());

        results = admin.getDQPs(HOST_2_2_2_2_PROCESS2_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getDQPs(HOST_2_2_2_2_WILDCARD);  
        assertEquals(1, results.size());

        
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getExtensionModules()</code> 
     * Fake data is setup in FakeExtensionSourceManager.getSourceDescriptors(), ExtensionSourceManager.getSource()
     * 
     * @since 4.3
     */
    public void testGetExtensionModules() throws AdminException {
        Collection results = admin.getExtensionModules(AdminObject.WILDCARD);  
        List resultsList = new ArrayList(results);
        assertEquals(2, results.size());
        
        MMExtensionModule module = (MMExtensionModule) resultsList.get(0);
        assertEquals("extensionModule1", module.getIdentifier()); //$NON-NLS-1$
        assertEquals("testUser1", module.getCreatedBy()); //$NON-NLS-1$
        assertEquals("description1", module.getDescription()); //$NON-NLS-1$
        assertEquals("bytes1", new String(module.getFileContents())); //$NON-NLS-1$
        
        
        results = admin.getExtensionModules("extensionModule1"); //$NON-NLS-1$
        assertEquals(1, results.size());        
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getHosts()</code> 
     * Fake data is setup in FakeRuntimeStateAdminAPIHelper.getSystemState(), FakeConfigurationServiceProxy.getHosts()
     * 
     * @since 4.3
     */
    public void testGetHosts() throws AdminException {
        Collection results = admin.getHosts(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {            
            MMHost host = (MMHost) iter.next();
            if ("1.1.1.1".equals(host.getIdentifier())) { //$NON-NLS-1$
                assertFalse(host.isRunning());
                assertTrue(host.isDeployed());
        
            } else if ("2.2.2.2".equals(host.getIdentifier())) { //$NON-NLS-1$
                assertTrue(host.isRunning());
                assertTrue(host.isDeployed());

            } else if ("3.3.3.3".equals(host.getIdentifier())) { //$NON-NLS-1$
                assertTrue(host.isRunning());
                assertFalse(host.isDeployed());
                
            } else {
                fail("Unexpected host "+host.getIdentifier()); //$NON-NLS-1$
            }
        }
        
        
        
        results = admin.getHosts("1.1.1.1"); //$NON-NLS-1$
        assertEquals(1, results.size());        
    }
    
       
    
    /**
     * Tests <code>ServerMonitoringImpl.getProcesss()</code> 
     * Fake data is setup in FakeRuntimeStateAdminAPIHelper.getSystemState(), FakeRuntimeStateAdminAPIHelper.getVMStatistics,
     * FakeConfiguration.getVMComponentDefns()
     * 
     * @since 4.3
     */
    public void testGetProcesses() throws AdminException {
        Collection results = admin.getProcesses(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {            
            MMProcess process = (MMProcess) iter.next();
            if (HOST_1_1_1_1_PROCESS1.equals(process.getIdentifier())) { 
                assertFalse(process.isRunning());
                assertTrue(process.isDeployed());
        
            } else if (HOST_2_2_2_2_PROCESS2.equals(process.getIdentifier())) { 
                
                assertTrue(process.isRunning());
                assertTrue(process.isDeployed());
                assertEquals(2, process.getFreeMemory());
                assertEquals(2, process.getThreadCount());
                assertEquals(2, process.getSockets());
                assertEquals(HOST_2_2_2_2_PROCESS2_POOL, process.getQueueWorkerPool().getIdentifier()); 
                assertEquals(2, process.getQueueWorkerPool().getQueued());

            } else if (HOST_3_3_3_3_PROCESS3.equals(process.getIdentifier())) { 
                assertTrue(process.isRunning());
                assertFalse(process.isDeployed());
                
            } else {
                fail("Unexpected process "+process.getIdentifier()); //$NON-NLS-1$
            }
        }
        
        
        
        results = admin.getProcesses(HOST_1_1_1_1_WILDCARD); 
        assertEquals(1, results.size());
        
        results = admin.getProcesses(HOST_1_1_1_1_PROCESS1); 
        assertEquals(1, results.size());
        
    }
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getQueueWorkerPools()</code> 
     * Fake data is setup in RuntimeStateAdminAPIHelper.getServices(), RuntimeStateAdminAPIHelper.getServiceBinding(),
     * RuntimeStateAdminAPIHelper.getServiceQueueStatistics()
     * 
     * @since 4.3
     */
    public void testGetQueueWorkerPools() throws AdminException {
        Collection results = admin.getQueueWorkerPools(AdminObject.WILDCARD);  
        assertEquals(4, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {            
            MMQueueWorkerPool pool = (MMQueueWorkerPool) iter.next();
            
            if (HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_POOL.equals(pool.getIdentifier())) { 
                assertEquals(2, pool.getQueued());
                assertEquals(2, pool.getTotalEnqueues());        
            } else if (HOST_3_3_3_3_PROCESS3_CONNECTOR_BINDING3_POOL.equals(pool.getIdentifier())) { 
                assertEquals(3, pool.getQueued());
                assertEquals(3, pool.getTotalEnqueues());        
            } else if (HOST_2_2_2_2_PROCESS2_CONNECTOR_DQP2_POOL.equals(pool.getIdentifier())) { 
                assertEquals(5, pool.getQueued());
                assertEquals(5, pool.getTotalEnqueues());        
            } else if (HOST_3_3_3_3_PROCESS3_CONNECTOR_DQP3_POOL.equals(pool.getIdentifier())) { 
                assertEquals(6, pool.getQueued());
                assertEquals(6, pool.getTotalEnqueues());  
            } else {
                fail("unexpected pool "+pool.getIdentifier()); //$NON-NLS-1$
            }            
        }
        
        
        results = admin.getQueueWorkerPools(HOST_2_2_2_2_WILDCARD);  
        assertEquals(2, results.size());
        
        results = admin.getQueueWorkerPools(HOST_2_2_2_2_PROCESS2_WILDCARD);  
        assertEquals(2, results.size());
        
        results = admin.getQueueWorkerPools(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getQueueWorkerPools(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_POOL);  
        assertEquals(1, results.size());
        
    }
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getRequests()</code> 
     * Fake data is setup in FakeQueryServiceProxy.getAllQueries()
     * 
     * @since 4.3
     */
    public void testGetRequests() throws AdminException {
        Collection results = admin.getRequests(AdminObject.WILDCARD);  
        assertEquals(2, results.size());
        
        MMRequest request = (MMRequest) results.iterator().next();
        assertEquals(REQUEST_1_1, request.getIdentifier()); 
        assertEquals(1, request.getSessionID());
        assertEquals("1", request.getRequestID()); //$NON-NLS-1$
        assertEquals("user1", request.getUserName()); //$NON-NLS-1$
        
        
        results = admin.getRequests(_1_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getRequests(REQUEST_1_1);  
        assertEquals(1, results.size());        
    }
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getResources()</code> 
     * Fake data is setup in FakeConfigurationServiceProxy.getResources()
     * 
     * @since 4.3
     */
    public void testGetResources() throws AdminException {
        Collection results = admin.getResources(AdminObject.WILDCARD);  
        assertEquals(2, results.size());
        
        MMResource resource = (MMResource) results.iterator().next();
        assertEquals("resource1", resource.getIdentifier()); //$NON-NLS-1$
        assertEquals(SharedResource.JDBC_COMPONENT_TYPE_NAME, resource.getResourceType());
        assertEquals("pool", resource.getConnectionPoolIdentifier()); //$NON-NLS-1$
        
        
        
        results = admin.getResources("resource1");  //$NON-NLS-1$
        assertEquals(1, results.size());
    }
        
    
    /**
     * Tests <code>ServerMonitoringImpl.getSourceRequests()</code> 
     * Fake data is setup in FakeQueryServiceProxy.getAllQueries(), FakeConfiguration.getConnectorBindingByRoutingID(uuid)
     * 
     * @since 4.3
     */
    public void testGetSourceRequests() throws AdminException {
        Collection results = admin.getSourceRequests(AdminObject.WILDCARD);  
        assertEquals(2, results.size());
        
        MMSourceRequest request = (MMSourceRequest) results.iterator().next();
        assertEquals(REQUEST_1_1_1, request.getIdentifier()); 
        assertEquals(1, request.getSessionID());
        assertEquals(REQUEST_1_1, request.getRequestID()); 
        assertEquals("connectorBinding1", request.getConnectorBindingName()); //$NON-NLS-1$
        assertEquals("user1", request.getUserName()); //$NON-NLS-1$
        
        
        results = admin.getSourceRequests(_1_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getSourceRequests(_1_1_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getSourceRequests(REQUEST_1_1_1);  
        assertEquals(1, results.size());
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getSessions()</code> 
     * Fake data is setup in FakeSessionServiceProxy.getAllSessions()
     * 
     * @since 4.3
     */
    public void testGetSessions() throws AdminException {
        Collection results = admin.getSessions(AdminObject.WILDCARD);  
        assertEquals(2, results.size());

        MMSession session = (MMSession) results.iterator().next();
        assertEquals("00000000-0000-0001-0000-000000000001", session.getIdentifier()); //$NON-NLS-1$
        assertEquals("vdb1", session.getVDBName()); //$NON-NLS-1$
        assertEquals("1", session.getVDBVersion()); //$NON-NLS-1$
        assertEquals("app1", session.getApplicationName()); //$NON-NLS-1$
        assertEquals("product1", session.getProductName()); //$NON-NLS-1$
               
        
        
        results = admin.getSessions("00000000-0000-0001-0000-000000000001");  //$NON-NLS-1$
        assertEquals(1, results.size());
    }
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getSystem()</code> 
     * Fake data is setup in FakeRuntimeStateAdminAPIHelper.isSystemStarted(),
     * FakeConfigurationServiceProxy().getServerStartupTime(),
     * FakeConfiguration
     * 
     * @since 4.3
     */
    public void testGetSystem() throws AdminException {
        MMSystem system = (MMSystem) admin.getSystem();  
    
        assertTrue(system.isStarted());
        assertEquals(new Date(1234), system.getStartTime());
        assertEquals("value1", system.getProperties().getProperty("key1")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getVDBs()</code> 
     * Fake data is setup in FakeRuntimeMetadataCatalog.getVirtualDatabases()
     * 
     * @since 4.3
     */
    public void testGetVDBs() {
        //TODO: need to add mock RuntimeMetaDataCatalog.        
        
    }
    
}
    
    
