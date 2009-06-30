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

package com.metamatrix.admin.server;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;

import junit.framework.TestCase;

import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMConnectorType;
import com.metamatrix.admin.objects.MMDQP;
import com.metamatrix.admin.objects.MMExtensionModule;
import com.metamatrix.admin.objects.MMHost;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.admin.objects.MMQueueWorkerPool;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.admin.objects.MMResource;
import com.metamatrix.admin.objects.MMService;
import com.metamatrix.admin.objects.MMSession;
import com.metamatrix.admin.objects.MMSystem;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.platform.config.ConfigUpdateMgr;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.FakeRegistryUtil;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;


/** 
 * Unit tests of ServerMonitoringAdminImpl
 * @since 4.3
 */
public class TestServerMonitoringAdminImpl extends TestCase implements IdentifierConstants {
    
    private ServerAdminImpl parent;
    private ServerMonitoringAdminImpl admin;
    
    
    public void setUp() throws Exception {
    	
        ConfigUpdateMgr.createSystemProperties("config_multihost.xml");        

        FakeRegistryUtil.clear();
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
    @SuppressWarnings("unchecked")
	public void testGetConnectorBindings() throws AdminException {
    	
        Collection<MMConnectorBinding> results = admin.getConnectorBindings(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
            MMConnectorBinding binding = (MMConnectorBinding) iter.next();
            if (HOST_1_1_1_1_PROCESS1_CONNECTOR_BINDING1.equals(binding.getIdentifier())) { 
                
                assertEquals("Not Registered", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(false, binding.isRegistered());
                assertEquals(true, binding.isEnabled());
                
                
            } else if (HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2.equals(binding.getIdentifier())){ 
            
                assertEquals("Open", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(true, binding.isRegistered());
                assertEquals(true, binding.isEnabled());
                Properties properties = binding.getProperties();
                assertEquals("value1", properties.get("prop1")); //$NON-NLS-1$ //$NON-NLS-2$
                assertEquals("value2", properties.get("prop2")); //$NON-NLS-1$ //$NON-NLS-2$            
            } else if (_3_3_3_3_PROCESS3_CONNECTOR_BINDING3.equals(binding.getIdentifier())) { 
                
            	assertEquals("Open", binding.getStateAsString()); //$NON-NLS-1$
                assertEquals(true, binding.isRegistered());
                assertEquals(true, binding.isEnabled());            
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
     * Tests <code>ServerMonitoringImpl.getServices()</code>.
     * 
     * Fake data is set up in FakeConfiguration.getDeployedComponents(), FakeConfiguration.getServices(),
     * FakeRuntimeStateAdminAPIHelper.getService(), 
     * Expects service1 to be deployed, but not running. 
     * Expects service2 to be deployed and running. 
     * Expects service3 to be running, but not deployed. 
     * @since 6.1
     */
    public void testGetServices() throws AdminException {
    	


		
        Collection<MMService> results = admin.getServices(AdminObject.WILDCARD + "dqp*");  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
        	MMService svc = (MMService) iter.next();

            if (HOST_2_2_2_2_PROCESS2_DQP2.equals(svc.getIdentifier())){ 
               	assertEquals("Open", svc.getStateAsString()); //$NON-NLS-1$

                assertEquals(true, svc.isRegistered());

                try {
                	
                	ServiceID id = new ServiceID(svc.getServiceID(), svc.getHostName(), svc.getProcessName());
                	ServiceRegistryBinding binding = admin.registry.getServiceBinding(svc.getHostName(), svc.getProcessName(), id);
       				binding.updateState(ConnectorBinding.STATE_CLOSED);
                } catch (ResourceNotBoundException e1) {
                } 
                
             } else if (_3_3_3_3_PROCESS3_DQP3.equals(svc.getIdentifier())) { 
                
                  assertEquals(false, svc.isRegistered());                                
            }
        }       
        
        results = admin.getServices(AdminObject.WILDCARD + "dqp*");  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
        	MMService svc = (MMService) iter.next();

            if (HOST_2_2_2_2_PROCESS2_DQP2.equals(svc.getIdentifier())){ 
              	assertEquals("Closed", svc.getStateAsString()); //$NON-NLS-1$

                assertEquals(true, svc.isRegistered());
                
             } else if (_3_3_3_3_PROCESS3_DQP3.equals(svc.getIdentifier())) { 
                
                  assertEquals(false, svc.isRegistered());                                
            }
        }            

        
        
        results = admin.getServices(HOST_2_2_2_2_PROCESS2_DQP2);  
        assertEquals(1, results.size());
        
        results = admin.getServices(_3_3_3_3_PROCESS3_DQP3);  
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
        assertEquals(26, results.size());
        
        MMConnectorType type = (MMConnectorType) admin.getConnectorTypes("connectorType1").iterator().next();
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
        Collection<MMDQP> results = admin.getDQPs(AdminObject.WILDCARD);  
        assertEquals(3, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {
            
            MMDQP dqp = (MMDQP) iter.next();
            if (HOST_1_1_1_1_PROCESS1_DQP1.equals(dqp.getIdentifier())) { 
                assertEquals("Not Registered", dqp.getStateAsString()); //$NON-NLS-1$
                assertEquals(false, dqp.isRegistered());
                assertEquals(true, dqp.isEnabled());
            } else if (HOST_2_2_2_2_PROCESS2_DQP2.equals(dqp.getIdentifier())){ 
                assertEquals(true, dqp.isRegistered());
                assertEquals(true, dqp.isEnabled());
            } else if (_3_3_3_3_PROCESS3_DQP3.equals(dqp.getIdentifier())) { 
                assertEquals(false, dqp.isRegistered());
                assertEquals(false, dqp.isEnabled());            
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
        
        
        // host.enabled() is not a valid attribute and the Host implementation doesn't allow
        // it to be set, the extended class ComponentDefn, defaults it to true
        // therefore the .isEnabled() test have been removed
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {            
            MMHost host = (MMHost) iter.next();
            if (HOST_1_1_1_1.equals(host.getIdentifier())) { //$NON-NLS-1$
                assertTrue(host.isRunning());
         
            } else if (HOST_2_2_2_2.equals(host.getIdentifier())) { //$NON-NLS-1$
                assertTrue(host.isRunning());
 
            } else if (HOST_3_3_3_3.equals(host.getIdentifier())) { //$NON-NLS-1$
                assertTrue(host.isRunning());
             } else {
                fail("Unexpected host "+host.getIdentifier()); //$NON-NLS-1$
            }
        }
              
        results = admin.getHosts(HOST_1_1_1_1); //$NON-NLS-1$
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
                assertFalse(process.isEnabled());
        
            } else if (HOST_2_2_2_2_PROCESS2.equals(process.getIdentifier())) { 
                
                assertTrue(process.isRunning());
                assertTrue(process.isEnabled());
                assertEquals(2, process.getFreeMemory());
                assertEquals(2, process.getThreadCount());
                assertEquals(2, process.getSockets());
                assertEquals(HOST_2_2_2_2_PROCESS2_POOL, process.getQueueWorkerPool().getIdentifier()); 
                assertEquals(2, process.getQueueWorkerPool().getQueued());

            } else if (HOST_3_3_3_3_PROCESS3.equals(process.getIdentifier())) { 
                assertTrue(process.isRunning());
                assertTrue(process.isEnabled());
                
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
        try {
			ConfigUpdateMgr.createSystemProperties("config_multihost_getWorkerPools.xml");
			FakeRegistryUtil.clear();
	        ClusteredRegistryState registry = FakeRegistryUtil.getFakeRegistry();
	        parent = new FakeServerAdminImpl(registry);
	        admin = new ServerMonitoringAdminImpl(parent, registry);        

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}        

    	
        Collection results = admin.getQueueWorkerPools(AdminObject.WILDCARD);  
        assertEquals(4, results.size());
        
        for (Iterator iter = results.iterator(); iter.hasNext(); ) {            
            MMQueueWorkerPool pool = (MMQueueWorkerPool) iter.next();
            
            
            if (HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_POOL.equals(pool.getIdentifier())) { 
                Collection<MMConnectorBinding> cbc = admin.getConnectorBindings(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2);    
                if (cbc == null || cbc.size() != 1){
                	fail("Didnt find the connector binding that corresponds to the worker pool");
                }
                MMConnectorBinding cb = cbc.iterator().next();

            	
                assertEquals(cb.getServiceID(), pool.getQueued());
                assertEquals(cb.getServiceID(), pool.getTotalEnqueues());        
            } else if (HOST_3_3_3_3_PROCESS3_CONNECTOR_BINDING3_POOL.equals(pool.getIdentifier())) { 
                Collection<MMConnectorBinding> cbc = admin.getConnectorBindings(_3_3_3_3_PROCESS3_CONNECTOR_BINDING3);    
                if (cbc == null || cbc.size() != 1){
                	fail("Didnt find the connector binding that corresponds to the worker pool");
                }
                MMConnectorBinding cb = cbc.iterator().next();

                assertEquals(cb.getServiceID(), pool.getQueued());
                assertEquals(cb.getServiceID(), pool.getTotalEnqueues());        
            } else if (HOST_2_2_2_2_PROCESS2_CONNECTOR_DQP2_POOL.equals(pool.getIdentifier())) { 
            	Collection<MMDQP> dqps = admin.getDQPs(HOST_2_2_2_2_PROCESS2_DQP2); 
                if (dqps == null || dqps.size() != 1){
                	fail("Didnt find the dqp that corresponds to the worker pool");
                }
                MMDQP dqp = dqps.iterator().next();
            	
                assertEquals(dqp.getServiceID(), pool.getQueued());
                assertEquals(dqp.getServiceID(), pool.getTotalEnqueues());        
            } else if (HOST_3_3_3_3_PROCESS3_CONNECTOR_DQP3_POOL.equals(pool.getIdentifier())) { 
               	Collection<MMDQP> dqps = admin.getDQPs(_3_3_3_3_PROCESS3_DQP3); 
                if (dqps == null || dqps.size() != 1){
                	fail("Didnt find the dqp that corresponds to the worker pool");
                }
                MMDQP dqp = dqps.iterator().next();
            	
                assertEquals(dqp.getServiceID(), pool.getQueued());
                assertEquals(dqp.getServiceID(), pool.getTotalEnqueues());        

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
        
        FakeRegistryUtil.clear();
        
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
        assertEquals("1", request.getSessionID()); //$NON-NLS-1$
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
        assertEquals(5, results.size());
        
        boolean matched = false;
        Iterator it=results.iterator();
        while(it.hasNext()) {
        
	        MMResource resource = (MMResource) it.next();
	        if (resource.getIdentifier().equalsIgnoreCase("resource1")) {
		        assertEquals("resource1", resource.getIdentifier()); //$NON-NLS-1$
		        assertEquals(SharedResource.MISC_COMPONENT_TYPE_NAME, resource.getResourceType());
		        matched = true;
	        }
        }
        
        assertTrue(matched);
        
        
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
        
        boolean matched = false;
        Iterator it=results.iterator();
        while(it.hasNext()) {
        
        
	        Request request = (Request) it.next();
	        if (request.getConnectorBindingName().equalsIgnoreCase("connectorBinding1")) {
		        assertEquals(REQUEST_1_1_1_0, request.getIdentifier()); 
		        assertEquals("1", request.getSessionID()); //$NON-NLS-1$
		        assertEquals("1", request.getRequestID()); //$NON-NLS-1$ 
		        assertEquals("1", request.getNodeID()); //$NON-NLS-1$ 
		        assertEquals("connectorBinding1", request.getConnectorBindingName()); //$NON-NLS-1$
		        assertEquals("user1", request.getUserName()); //$NON-NLS-1$
		        matched = true;
	        }
        }
        assertTrue(matched);
        
        results = admin.getSourceRequests(_1_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getSourceRequests(_1_1_WILDCARD);  
        assertEquals(1, results.size());
        
        results = admin.getSourceRequests(REQUEST_1_1_1_0);  
        assertEquals(1, results.size());
    }
    
    
    
    /**
     * Tests <code>ServerMonitoringImpl.getSessions()</code> 
     * Fake data is setup in FakeSessionServiceProxy.getAllSessions()
     * 
     * @since 4.3
     */
    public void testGetSessions() throws AdminException {
        Collection<Session> results = admin.getSessions(AdminObject.WILDCARD);  
        assertEquals(2, results.size());

        MMSession session = (MMSession) results.iterator().next();
        assertEquals("1", session.getIdentifier()); //$NON-NLS-1$
        assertEquals("vdb1", session.getVDBName()); //$NON-NLS-1$
        assertEquals("1", session.getVDBVersion()); //$NON-NLS-1$
        assertEquals("app1", session.getApplicationName()); //$NON-NLS-1$
        results = admin.getSessions("1");  //$NON-NLS-1$
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
    
    
