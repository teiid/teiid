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
import java.util.List;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Cache;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Request;

import junit.framework.TestCase;

import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.platform.config.ConfigUpdateMgr;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.FakeRegistryUtil;
import com.metamatrix.platform.registry.ProcessRegistryBinding;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;


/** 
 * Unit tests of ServerRuntimeStateAdminImpl
 * @since 4.3
 */
public class TestServerRuntimeStateAdminImpl extends TestCase implements IdentifierConstants {
    
    private ServerAdminImpl parent;
    private ServerRuntimeStateAdminImpl admin;
    
    
    public void setUp() throws Exception {
        DBIDGenerator.getInstance().setUseMemoryIDGeneration(true);
        
        FakeQueryService.clearState();
        FakeCacheAdmin.clearState();
        FakeRuntimeStateAdminAPIHelper.clearState();
        
        ConfigUpdateMgr.createSystemProperties("config_multihost.xml");        

        
        ClusteredRegistryState registry = FakeRegistryUtil.getFakeRegistry();
        parent = new FakeServerAdminImpl(registry);
        admin = new ServerRuntimeStateAdminImpl(parent, registry);        
    }

    /**
     * Tests <code>ServerRuntimeStateAdminImpl.cancelRequest()</code> 
     * Fake data is set up in FakeQueryService.cancelQuery()
     * @since 4.3
     */
    public void testCancelRequest() throws AdminException {
        
        //failure case: invalid request ID        
        boolean failed = false;
        try {
            admin.cancelRequest("1" + AdminObject.WILDCARD);  //$NON-NLS-1$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed); //$NON-NLS-1$
        
        
        //positive case
        assertTrue(FakeQueryService.cancelledQueries.isEmpty());
        
        admin.cancelRequest(REQUEST_1_1);  
        
        assertTrue(FakeQueryService.cancelledQueries.contains(REQUEST_1_1)); 
        
        
        //failure case: invalid request ID - contains WILDCARD
        failed = false;
        try {
            admin.cancelRequest("abc" + Request.DELIMITER + "def");  //$NON-NLS-1$ //$NON-NLS-2$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);         //$NON-NLS-1$
    }
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.cancelSourceRequest()</code> 
     * Fake data is set up in FakeQueryService.cancelQuery()
     * @since 4.3
     */
    public void testCancelSourceRequest() throws AdminException {
                
        //positive case
        assertTrue(FakeQueryService.cancelledQueries.isEmpty());
        
        admin.cancelSourceRequest(REQUEST_1_1_1_0);  
        
        assertTrue(FakeQueryService.cancelledQueries.contains(REQUEST_1_1_1_0)); 
        
        
        
        //failure case: invalid source request ID - contains WILDCARD        
        boolean failed = false;
        try {
            admin.cancelSourceRequest("1" + Request.DELIMITER + "1" + AdminObject.WILDCARD);  //$NON-NLS-1$ //$NON-NLS-2$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed); //$NON-NLS-1$
        
        
        //failure case: invalid source request ID
        failed = false;
        try {
            admin.cancelSourceRequest("abc" + Request.DELIMITER + "def" + Request.DELIMITER + "ghi");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);         //$NON-NLS-1$
    }
    
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.changeVDBStatus()</code> 
     * @since 4.3
     */
    public void testChangeVDBStatus(){
        //TODO: need to add mock RuntimeMetaDataCatalog.  
    }    
    
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.clearCache()</code> 
     * Fake data setup in FakeMetaMatrixRegistry.getVMControllerBindings(),
     * FakeCacheAdmin.clearCache()
     * 
     * @since 4.3
     */
    public void testClearCache() throws AdminException {
        //positive case
        assertTrue(FakeCacheAdmin.clearedCaches.isEmpty());
        
        admin.clearCache(Cache.PREPARED_PLAN_CACHE);
        
        assertTrue(FakeCacheAdmin.clearedCaches.contains("cache1")); //$NON-NLS-1$
        
        
        
        //failure case: bad cache name
        boolean failed = false;
        try {
            admin.cancelSourceRequest(AdminObject.WILDCARD + "badcachename");  //$NON-NLS-1$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);                 //$NON-NLS-1$
    }
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.startConnectorBinding()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.restartService(),
     * FakeConfiguration.getDeployedComponents(),
     * FakeRuntimeStateAdminAPIHelper.getService(), FakeRuntimeStateAdminAPIHelper.getServiceBinding()
     * 
     * @since 4.3
     */
    public void testStartConnectorBinding() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.restartedServices.isEmpty());
        
        String host = "2.2.2.2"; //$NON-NLS-1$
        String process = "process2"; //$NON-NLS-1$
        
    	List<ServiceRegistryBinding> svcbindings = admin.registry.getServiceBindings(host, process, "connectorType2");
    	ServiceRegistryBinding binding = svcbindings.get(0);   	
		binding.updateState(ConnectorBinding.STATE_CLOSED);
		
        admin.startConnectorBinding(AdminObject.WILDCARD + binding.getInstanceName());
       // 		"connectorBinding2"); //$NON-NLS-1$
        assertTrue(FakeRuntimeStateAdminAPIHelper.restartedServices.contains(binding.getServiceID().toString())); //$NON-NLS-1$
      
        
        //failure case: unknown connectorBinding
        boolean success = false;
        try {
            admin.startConnectorBinding(AdminObject.WILDCARD + "badcbname");  //$NON-NLS-1$
            success = true;
        } catch (AdminProcessingException e) {
            success = false;
        }
        assertFalse("Should not have been able to startConnectorBinding - " + AdminObject.WILDCARD + "badcbname", success); //$NON-NLS-1$ //$NON-NLS-2$
        
        // Success case: Start all connector bindings
        success = false;
        try {
            admin.startConnectorBinding(AdminObject.WILDCARD);  
            success = true;
        } catch (AdminProcessingException e) {
            success = false;
        }
        assertTrue("Should have been able to startConnectorBinding - " + AdminObject.WILDCARD, success);                 //$NON-NLS-1$
    }
    
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.startHost()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.startHost()
     * 
     * @since 4.3
     */
    public void testStartHost() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.startedHosts.isEmpty());
        
        admin.startHost("2.2.2.2", true); //$NON-NLS-1$
        assertTrue(FakeRuntimeStateAdminAPIHelper.startedHosts.contains("2.2.2.2")); //$NON-NLS-1$
        
    }
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.startProcess()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.startProcess()
     * 
     * @since 4.3
     */
    public void testStartProcess() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.startedProcesses.isEmpty());
        
        admin.startProcess(HOST_2_2_2_2_PROCESS2, true); 
        assertTrue(FakeRuntimeStateAdminAPIHelper.startedProcesses.contains(HOST_2_2_2_2_PROCESS2)); 
        
        
        
        //failure case: bad process ID
        boolean failed = false;
        try {
            admin.startProcess(AdminObject.WILDCARD + "process2", true);  //$NON-NLS-1$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);                 //$NON-NLS-1$
        
    }
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.stopConnectorBinding()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.stopService(),
     * FakeConfiguration.getDeployedComponents(),
     * FakeRuntimeStateAdminAPIHelper.getService(), FakeRuntimeStateAdminAPIHelper.getServiceBinding()
     * 
     * @since 4.3
     */
    public void testStopConnectorBinding() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedServices.isEmpty());
        
       	List<ServiceRegistryBinding> svcbindings = admin.registry.getServiceBindings("2.2.2.2", "process2", "connectorType2");
    	ServiceRegistryBinding binding = svcbindings.get(0);   	

        
        admin.stopConnectorBinding(AdminObject.WILDCARD + binding.getInstanceName(), true); //$NON-NLS-1$
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedServices.contains(binding.getServiceID().toString())); //$NON-NLS-1$
        

        //failure case: unknown connectorBinding
        boolean success = false;
        try {
            admin.stopConnectorBinding(AdminObject.WILDCARD + "badcbname", true);  //$NON-NLS-1$
            success = true;
        } catch (AdminProcessingException e) {
            success = false;
        }
        assertFalse("Should not have been able to stopConnectorBinding - " + AdminObject.WILDCARD + "badcbname", success);                 //$NON-NLS-1$ //$NON-NLS-2$
        
        // Success case: Stop all connector bindings
        success = false;
        try {
            admin.stopConnectorBinding(AdminObject.WILDCARD, true);  
            success = true;
        } catch (AdminProcessingException e) {
            success = false;
        }
        assertTrue("Should have been able to stopConnectorBinding - " + AdminObject.WILDCARD, success);                 //$NON-NLS-1$
    }
        
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.stopHost()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.stopHost()
     * 
     * @since 4.3
     */
    public void testStopHost() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedHosts.isEmpty());
        
        admin.stopHost(HOST_2_2_2_2, true, false); 
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedHosts.contains(HOST_2_2_2_2)); 
        
    }
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.stopProcess()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.stopProcess(),
     * 
     * @since 4.3
     */
    public void testStopProcess() throws AdminException {
        
        //positive case
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedProcesses.isEmpty());
        
       	ProcessRegistryBinding binding=null;
		try {
			binding = admin.registry.getProcessBinding("2.2.2.2", "process2");
		} catch (ResourceNotBoundException e1) {
			// TODO Auto-generated catch block
			fail(e1.getMessage());
		}
 
        
        admin.stopProcess(AdminObject.WILDCARD +  binding.getProcessName(), true, false); //$NON-NLS-1$
        assertTrue(FakeRuntimeStateAdminAPIHelper.stoppedProcesses.contains(binding.getHostName() + "|" + binding.getProcessName()));

        //failure case: unknown process
        boolean failed = false;
        try {
            admin.stopProcess(AdminObject.WILDCARD + "badprocessname", true, false);  //$NON-NLS-1$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);                 //$NON-NLS-1$
        
        //failure case: process name not specific enough
        failed = false;
        try {
            admin.stopProcess(AdminObject.WILDCARD, true, false);  
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);                 //$NON-NLS-1$
    }
        
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.stopSystem()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.stopSystem()
     * 
     * @since 4.3
     */
    public void testStopSystem() throws AdminException {
        
        //positive case
        assertFalse(FakeRuntimeStateAdminAPIHelper.shutdownSystem);        
        admin.stopSystem(); 
        assertTrue(FakeRuntimeStateAdminAPIHelper.shutdownSystem); 
        
    }
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.synchronizeSystem()</code> 
     * Fake data setup in FakeRuntimeStateAdminAPIHelper.synchronizeSystem()
     * 
     * @since 4.3
     */
    public void testSynchronizeSystem() throws AdminException {
        
        //positive case
        assertFalse(FakeRuntimeStateAdminAPIHelper.synchronizeSystem);        
        admin.synchronizeSystem(false); 
        assertTrue(FakeRuntimeStateAdminAPIHelper.synchronizeSystem); 
        
    }
    
    
    
    /**
     * Tests <code>ServerRuntimeStateAdminImpl.terminateSession</code> 
     * Fake data is set up in FakeSessionServiceProxy.terminateSession()
     * @since 4.3
     */
    public void testTerminateSession() throws AdminException {
        
        //positive case
        assertTrue(FakeServerSessionService.terminatedSessions.isEmpty());
        
        admin.terminateSession("1");  //$NON-NLS-1$
        
        assertTrue(FakeServerSessionService.terminatedSessions.contains("1")); //$NON-NLS-1$
        
        //failure case: invalid session ID
        boolean failed = false;
        try {
            admin.terminateSession("abc" + AdminObject.DELIMITER + "def");  //$NON-NLS-1$ //$NON-NLS-2$
        } catch (AdminProcessingException e) {
            failed = true;
        }
        assertTrue("Expected AdminProcessingException", failed);         //$NON-NLS-1$
    }
}
    
    
