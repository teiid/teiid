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

package com.metamatrix.platform.registry;

import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.jboss.cache.notifications.annotation.CacheListener;
import org.mockito.Mockito;

import com.metamatrix.admin.server.FakeCacheAdmin;
import com.metamatrix.admin.server.FakeConfiguration;
import com.metamatrix.admin.server.FakeQueryService;
import com.metamatrix.cache.FakeCache.FakeCacheFactory;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.server.query.service.QueryService;

@CacheListener
public class FakeRegistryUtil {

	private static ClusteredRegistryState registry;
	
	public static void clear() {
		registry = null;
	}
	
	public static ClusteredRegistryState getFakeRegistry(ConfigurationModelContainer model) throws Exception {
		
		registry = new ClusteredRegistryState(new FakeCacheFactory());
		
		int svcid = 0;
		
		Iterator<Host> hostIt = model.getHosts().iterator();
		while(hostIt.hasNext()) {
			Host h = hostIt.next();
			registry.addHost(new HostControllerRegistryBinding(h.getName(), h.getProperties(), null, new NoOpMessageBus()));
			
			
			Iterator<VMComponentDefn> vmIt = model.getConfiguration().getVMsForHost((HostID)h.getID()).iterator();
			while(vmIt.hasNext()) {
				VMComponentDefn vm = vmIt.next();
				
				if (!vm.isEnabled()) {
					continue;
				}
			    ProcessManagement vmInterface1 = Mockito.mock(ProcessManagement.class);
				Mockito.stub(vmInterface1.getAddress()).toReturn(InetAddress.getLocalHost());

			    
			    ProcessRegistryBinding binding = new ProcessRegistryBinding(h.getName(), vm.getName(), vm, vmInterface1, new NoOpMessageBus());
			    binding.setAlive(true);
			    binding.setStartTime(new Date(1234).getTime());
			    registry.addProcess(binding.getHostName(), binding.getProcessName(), binding);
			    
			    
			    Iterator<DeployedComponent> depIt = model.getConfiguration().getDeployedServicesForVM(vm).iterator();
			    while(depIt.hasNext()) {
			    	DeployedComponent dep = depIt.next();
			    	
			    	if (dep.isEnabled()) {
				    	ServiceID sid = new ServiceID(++svcid, binding.getHostName(), binding.getProcessName());	
				    	ServiceRegistryBinding svcbinding = new ServiceRegistryBinding(sid, 
				    			new FakeCacheAdmin(sid), 
				    			dep.getComponentTypeID().getName(),
				    			dep.getServiceComponentDefnID().getName(), 
				    			null, 
				    			dep.getServiceComponentDefnID().getName(),
				    			binding.getHostName(), 
				    			dep,  
				    			(dep.isEnabled() ? ServiceState.STATE_OPEN : ServiceState.STATE_CLOSED), 
				    			new Date(), 
				    			false, 
				    			new NoOpMessageBus());
				    	
				    	svcbinding.updateState(dep.isEnabled() ? ServiceState.STATE_OPEN : ServiceState.STATE_CLOSED);
				    	
				    	
				    	registry.addServiceBinding(binding.getHostName(), binding.getProcessName(), svcbinding);
			    	}
			    	
			    }

				
			}
		}
		
		
		
		return registry;
	}
	
	public static ClusteredRegistryState getFakeRegistry() throws Exception {
	
		if (registry != null) {
			return registry;
		}
		
		ConfigurationModelContainer cmc = CurrentConfiguration.getInstance().getConfigurationModel();
		
		return getFakeRegistry(cmc);
		
		
//		registry = new ClusteredRegistryState(new FakeCacheFactory());
//        
//		HostControllerRegistryBinding host1 = buildHostRegistryBinding("2.2.2.2"); //$NON-NLS-1$
//		HostControllerRegistryBinding host2 = buildHostRegistryBinding("3.3.3.3"); //$NON-NLS-1$
//		registry.addHost(host1);
//		registry.addHost(host2);
//		
//		ProcessRegistryBinding vmBinding2  = buildVMRegistryBinding("2.2.2.2", "process2");             //$NON-NLS-1$ //$NON-NLS-2$ 
//        ServiceRegistryBinding serviceBinding2 = buildServiceRegistryBinding("connectorBinding2", 2, vmBinding2, "Cache","psc2");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//		
//        registry.addProcess(vmBinding2.getHostName(), vmBinding2.getProcessName(), vmBinding2);
//        registry.addServiceBinding(vmBinding2.getHostName(), vmBinding2.getProcessName(), serviceBinding2);
//		
//		
//		ProcessRegistryBinding vmBinding3  = buildVMRegistryBinding("3.3.3.3", "process3");             //$NON-NLS-1$ //$NON-NLS-2$ 
//        ServiceRegistryBinding serviceBinding3 = buildServiceRegistryBinding("connectorBinding3", 3, vmBinding3, "Cache", "psc3");  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//		
//        registry.addProcess(vmBinding3.getHostName(), vmBinding3.getProcessName(), vmBinding3);
//        registry.addServiceBinding(vmBinding3.getHostName(), vmBinding3.getProcessName(), serviceBinding3);
//		
//		
//		// dqps
//		ServiceID sid1 = new ServiceID(5, vmBinding2.getHostName(),vmBinding2.getProcessName());
//		registry.addServiceBinding(vmBinding2.getHostName(), vmBinding2.getProcessName(), new ServiceRegistryBinding(sid1, new FakeQueryService(sid1), QueryService.SERVICE_NAME,
//                                                                    "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
//                                                                    "dqp2", "2.2.2.2",(DeployedComponent)new FakeConfiguration().deployedComponents.get(4),  //$NON-NLS-1$ //$NON-NLS-2$ 
//                                                                    ServiceState.STATE_CLOSED,
//                                                                    new Date(),  
//                                                                    false, new NoOpMessageBus()));
//
//		ServiceID sid2 = new ServiceID(6, vmBinding3.getHostName(),vmBinding3.getProcessName());	
//		registry.addServiceBinding(vmBinding3.getHostName(), vmBinding3.getProcessName(), new ServiceRegistryBinding(sid2, new FakeQueryService(sid2), QueryService.SERVICE_NAME,
//                "dqp3", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
//                "dqp3", "3.3.3.3", (DeployedComponent)new FakeConfiguration().deployedComponents.get(5),  //$NON-NLS-1$ //$NON-NLS-2$ 
//                ServiceState.STATE_CLOSED,
//                new Date(),  
//                false, new NoOpMessageBus())); 
//		
//		return registry;
		
	}
	
	
	public static ProcessRegistryBinding buildVMRegistryBinding(String hostName, String processName) throws Exception {
	    HostID hostID1 = new HostID(hostName); 
	    VMComponentDefnID defnID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID1, processName);  
	    VMComponentDefn defn1 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID1, defnID1, new ComponentTypeID(VMComponentDefnType.COMPONENT_TYPE_NAME)); 
	    ProcessManagement vmInterface1 = Mockito.mock(ProcessManagement.class);
		Mockito.stub(vmInterface1.getAddress()).toReturn(InetAddress.getLocalHost());

	    
	    ProcessRegistryBinding binding = new ProcessRegistryBinding(hostName, processName, defn1, vmInterface1, new NoOpMessageBus());
	    binding.setAlive(true);
	    binding.setStartTime(new Date(1234).getTime());
	    return binding;
	}

//	public static ServiceRegistryBinding buildServiceRegistryBinding(String name, int id, ProcessRegistryBinding vm, String type, String psc) {
//		ServiceID sid = new ServiceID(id, vm.getHostName(), vm.getProcessName());	
//		
//        DeployedComponentID deployedComponentID1 = new DeployedComponentID(name, Configuration.NEXT_STARTUP_ID, vm.getDeployedComponent().getHostID(), (VMComponentDefnID)vm.getDeployedComponent().getID());
//		ConnectorBindingID connectorBindingID1 = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, name); 
//		
//		BasicDeployedComponent deployedComponent = new BasicDeployedComponent(deployedComponentID1, Configuration.NEXT_STARTUP_ID, vm.getDeployedComponent().getHostID(), (VMComponentDefnID)vm.getDeployedComponent().getID(), connectorBindingID1, ConnectorBindingType.CONNECTOR_TYPE_ID);
//		deployedComponent.setDescription(name); 
//		
//	    return new ServiceRegistryBinding(sid, new FakeCacheAdmin(sid), type,name, null, name, vm.getHostName(), deployedComponent,  ServiceState.STATE_OPEN, new Date(), false, new NoOpMessageBus());	 //$NON-NLS-1$
//	}
	
//	static HostControllerRegistryBinding buildHostRegistryBinding(String name) {
//		return new HostControllerRegistryBinding(name, new Properties(), null, new NoOpMessageBus());
//	}	
}

