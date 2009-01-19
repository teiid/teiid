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

package com.metamatrix.platform.registry;

import java.net.InetAddress;
import java.util.Date;

import org.jboss.cache.notifications.annotation.CacheListener;
import org.mockito.Mockito;

import com.metamatrix.admin.server.FakeCacheAdmin;
import com.metamatrix.admin.server.FakeConfiguration;
import com.metamatrix.admin.server.FakeQueryService;
import com.metamatrix.cache.FakeCache.FakeCacheFactory;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.server.query.service.QueryService;

@CacheListener
public class FakeRegistryUtil {

	private static ClusteredRegistryState registry;
	
	public static ClusteredRegistryState getFakeRegistry() throws Exception {
	
		if (registry != null) {
			return registry;
		}
		
		registry = new ClusteredRegistryState(new FakeCacheFactory());
        
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("2.2.2.2"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("3.3.3.3"); //$NON-NLS-1$
		registry.addHost(host1);
		registry.addHost(host2);
		
		VMRegistryBinding vmBinding2  = buildVMRegistryBinding("2.2.2.2", 2, "process2");             //$NON-NLS-1$ //$NON-NLS-2$
        ServiceRegistryBinding serviceBinding2 = buildServiceRegistryBinding("connectorBinding2", 2, vmBinding2, "Cache","psc2");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
        registry.addVM(vmBinding2.getHostName(), vmBinding2.getVMControllerID().toString(), vmBinding2);
        registry.addServiceBinding(vmBinding2.getHostName(), vmBinding2.getVMControllerID().toString(), serviceBinding2);
		
		
		VMRegistryBinding vmBinding3  = buildVMRegistryBinding("3.3.3.3", 3, "process3");             //$NON-NLS-1$ //$NON-NLS-2$
        ServiceRegistryBinding serviceBinding3 = buildServiceRegistryBinding("connectorBinding3", 3, vmBinding3, "Cache", "psc3");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
        registry.addVM(vmBinding3.getHostName(), vmBinding3.getVMControllerID().toString(), vmBinding3);
        registry.addServiceBinding(vmBinding3.getHostName(), vmBinding3.getVMControllerID().toString(), serviceBinding3);
		
		
		// dqps
		ServiceID sid1 = new ServiceID(5, vmBinding2.getVMControllerID());
		registry.addServiceBinding(vmBinding2.getHostName(), vmBinding2.getVMControllerID().toString(), new ServiceRegistryBinding(sid1, new FakeQueryService(sid1), QueryService.SERVICE_NAME,
                                                                    "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                    "dqp2", "2.2.2.2",(DeployedComponent)new FakeConfiguration().deployedComponents.get(4), null, //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                    AbstractService.STATE_CLOSED,
                                                                    new Date(),  
                                                                    false, new NoOpMessageBus()));

		ServiceID sid2 = new ServiceID(6, vmBinding3.getVMControllerID());	
		registry.addServiceBinding(vmBinding3.getHostName(), vmBinding3.getVMControllerID().toString(), new ServiceRegistryBinding(sid2, new FakeQueryService(sid2), QueryService.SERVICE_NAME,
                "dqp3", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                "dqp3", "3.3.3.3", (DeployedComponent)new FakeConfiguration().deployedComponents.get(5), null, //$NON-NLS-1$ //$NON-NLS-2$ 
                AbstractService.STATE_CLOSED,
                new Date(),  
                false, new NoOpMessageBus())); 
		
		return registry;
		
	}
	
	
	public static VMRegistryBinding buildVMRegistryBinding(String hostName, int vmID, String process) throws Exception {
	    VMControllerID vmID1 = new VMControllerID(vmID, hostName);             
	    HostID hostID1 = new HostID(hostName); 
	    VMComponentDefnID defnID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID1, process);  
	    VMComponentDefn defn1 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID1, defnID1, new ComponentTypeID(VMComponentDefnType.COMPONENT_TYPE_NAME)); 
	    VMControllerInterface vmInterface1 = Mockito.mock(VMControllerInterface.class);
		Mockito.stub(vmInterface1.getAddress()).toReturn(InetAddress.getLocalHost());

	    
	    VMRegistryBinding binding = new VMRegistryBinding(hostName, vmID1, defn1, vmInterface1, new NoOpMessageBus());
	    binding.setAlive(true);
	    return binding;
	}

	public static ServiceRegistryBinding buildServiceRegistryBinding(String name, int id, VMRegistryBinding vm, String type, String psc) {
		ServiceID sid = new ServiceID(id, vm.getVMControllerID());	
		
        DeployedComponentID deployedComponentID1 = new DeployedComponentID(name, Configuration.NEXT_STARTUP_ID, vm.getDeployedComponent().getHostID(), (VMComponentDefnID)vm.getDeployedComponent().getID());
		ConnectorBindingID connectorBindingID1 = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, name); 
		
		ProductServiceConfigID pscID = new ProductServiceConfigID(Configuration.NEXT_STARTUP_ID, psc);  
		BasicDeployedComponent deployedComponent = new BasicDeployedComponent(deployedComponentID1, Configuration.NEXT_STARTUP_ID, vm.getDeployedComponent().getHostID(), (VMComponentDefnID)vm.getDeployedComponent().getID(), connectorBindingID1, pscID,ConnectorBindingType.CONNECTOR_TYPE_ID);
		deployedComponent.setDescription(name); 
		
	    return new ServiceRegistryBinding(sid, new FakeCacheAdmin(sid), type,"instance-"+id, null, name, vm.getHostName(), deployedComponent, null, ServiceInterface.STATE_OPEN, new Date(), false, new NoOpMessageBus());	 //$NON-NLS-1$
	}
	
	static HostControllerRegistryBinding buildHostRegistryBinding(String name) {
		return new HostControllerRegistryBinding(name, null, new NoOpMessageBus());
	}	
}

