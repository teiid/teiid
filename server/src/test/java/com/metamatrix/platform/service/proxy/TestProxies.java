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

package com.metamatrix.platform.service.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.admin.server.FakeConfiguration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.platform.registry.FakeRegistryUtil;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.registry.ProcessRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;
import com.metamatrix.platform.service.controller.FakeService;
import com.metamatrix.platform.service.controller.FakeServiceInterface;
import com.metamatrix.server.query.service.QueryService;

public class TestProxies extends TestCase {

    private final class FakePolicy implements ServiceSelectionPolicy {

        private final List bindings;

        int index = 0;

        private FakePolicy(List bindings) {
            super();
            this.bindings = bindings;
        }

        public String getServiceSelectionPolicyName() {
            return "Dummy"; //$NON-NLS-1$
        }

        public boolean prefersLocal() {
            return false;
        }

        public ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException {
            return (ServiceRegistryBinding)this.bindings.get(index++%this.bindings.size());
        }

        public List getInstances() throws ServiceNotFoundException {
            return this.bindings;
        }

		@Override
		public void updateServices(List<ServiceRegistryBinding> localServices,
				List<ServiceRegistryBinding> remoteServices) {
		}
    }

    public void testMultipleInvocation() throws Exception {
        
        
        final FakeService[] fakeServices = new FakeService[2];
        
        fakeServices[0] = new FakeService();
        fakeServices[1] = new FakeService();
        
        final List serviceBindings = new ArrayList();
        
        for (int i = 0; i < fakeServices.length; i++) {
        	ProcessRegistryBinding vmBinding2  = FakeRegistryUtil.buildVMRegistryBinding("2.2.2.2", "process2");             //$NON-NLS-1$ //$NON-NLS-2$ 
        	ServiceID sid1 = new ServiceID(i, vmBinding2.getHostName(), vmBinding2.getProcessName());
        	ServiceRegistryBinding binding = new ServiceRegistryBinding(sid1, fakeServices[i], QueryService.SERVICE_NAME,
                                                                        "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                        "dqp2", "2.2.2.2",(DeployedComponent)new FakeConfiguration().deployedComponents.get(4),  //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                        ServiceState.STATE_CLOSED,
                                                                        new Date(),  
                                                                        false, new NoOpMessageBus());         	
            serviceBindings.add(binding);
        }
        
        ServiceSelectionPolicy policy = new FakePolicy(serviceBindings);

        Properties props = new Properties();

        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, FakeServiceInterface.class.getName());
        props.put(ServiceProxyProperties.SERVICE_MULTIPLE_DELEGATION, Boolean.TRUE.toString());
        
        FakeServiceInterface fakeService = (FakeServiceInterface)ProxyManager.createProxy("foo", props, policy); //$NON-NLS-1$
        
        //ensures that the multiple invokation handles null
        Collection result = fakeService.test2();
        assertNotNull(result);
        assertTrue(result.isEmpty());
        
        fakeService.test1();
        
        for (int i = 0; i < fakeServices.length; i++) {
            assertEquals(1, fakeServices[i].getTest1Count());
        }
        
        fakeService.test1();
        
        for (int i = 0; i < fakeServices.length; i++) {
            assertEquals(2, fakeServices[i].getTest1Count());
        }
        
        result = fakeService.test4();
        
        assertEquals(Arrays.asList(new Object[] {new Integer(1), new Integer(1)}), result);
    }
    
    public void testSingleInvocation() throws Exception {
        
        
        final FakeService[] fakeServices = new FakeService[2];
        
        fakeServices[0] = new FakeService();
        fakeServices[1] = new FakeService();
        
        final List serviceBindings = new ArrayList();
        
        for (int i = 0; i < fakeServices.length; i++) {
        	
        	ProcessRegistryBinding vmBinding2  = FakeRegistryUtil.buildVMRegistryBinding("2.2.2.2", "process2");             //$NON-NLS-1$ //$NON-NLS-2$ 
        	ServiceID sid1 = new ServiceID(i, vmBinding2.getHostName(), vmBinding2.getProcessName());
        	ServiceRegistryBinding binding = new ServiceRegistryBinding(sid1, fakeServices[i], QueryService.SERVICE_NAME,
                                                                        "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                        "dqp2", "2.2.2.2",(DeployedComponent)new FakeConfiguration().deployedComponents.get(4),  //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                        ServiceState.STATE_CLOSED,
                                                                        new Date(),  
                                                                        false, new NoOpMessageBus());        	
            serviceBindings.add(binding);
        }
        
        ServiceSelectionPolicy policy = new FakePolicy(serviceBindings);

        Properties props = new Properties();

        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, FakeServiceInterface.class.getName());
        
        FakeServiceInterface fakeService = (FakeServiceInterface)ProxyManager.createProxy("foo", props, policy); //$NON-NLS-1$
        
        Collection result = fakeService.test2();
        assertNotNull(result);
        assertTrue(result.isEmpty());
        
        fakeService.test1();
        
        int total = 0;
        
        for (int i = 0; i < fakeServices.length; i++) {
            total += fakeServices[i].getTest1Count();
        }
        
        assertEquals(1, total);
        
        fakeService.test1();
        
        total = 0;
        
        for (int i = 0; i < fakeServices.length; i++) {
            total += fakeServices[i].getTest1Count();
        }
        
        assertEquals(2, total);
        
        assertFalse(fakeService.test3());
        
        result = fakeService.test4();
        
        assertEquals(Arrays.asList(new Object[] {new Integer(1)}), result);
    }
    
}
