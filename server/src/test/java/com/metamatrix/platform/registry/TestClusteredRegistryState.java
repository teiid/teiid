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

import java.util.Date;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.FakeCache.FakeCacheFactory;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.platform.registry.ClusteredRegistryState.CacheNodeNotFoundException;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;

public class TestClusteredRegistryState extends TestCase {
	CacheFactory factory = new FakeCacheFactory();
	
	private String key(String key) {
		return key.toUpperCase();
	}
	
	public void testAddVM() throws Exception {		
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		Cache rootNode = state.cache;
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$

		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(rootNode.getChildren().size(), 2);
		assertNotNull(rootNode.getChild(key("host-1"))); //$NON-NLS-1$
		assertNotNull(rootNode.getChild(key("host-2"))); //$NON-NLS-1$
		assertNotNull(rootNode.getChild(key("host-2")).getChild(key("vm-1"))); //$NON-NLS-1$ //$NON-NLS-2$

		assertEquals(rootNode.getChild(key("host-1")).get("Name"), "HOST-1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals(rootNode.getChild(key("host-1")).getChild(key("vm-1")).get("Name"), "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testRemoveVM() throws Exception {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		Cache rootNode = state.cache;
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(rootNode.getChildren().size(), 2);
		
		state.removeProcess("host-1", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertNull(rootNode.getChild(key("host-1")).getChild(key("vm-1"))); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(rootNode.getChild(key("host-1")).getChild(key("vm-2"))); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(rootNode.getChild(key("host-2")).getChild(key("vm-1")));		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testGetVMs() throws Exception {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(0, state.getVMs("unknown").size()); //$NON-NLS-1$
		assertEquals(2, state.getVMs("host-1").size()); //$NON-NLS-1$
		assertEquals(1, state.getVMs("host-2").size()); //$NON-NLS-1$
		assertEquals(3, state.getVMs(null).size());
				
		assertEquals(vm2, state.getProcessBinding("host-1", "vm-2")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testHosts() throws Exception {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$

		assertEquals(2, state.getHosts().size());
		
		state.getHosts().contains("host-1"); //$NON-NLS-1$
		state.getHosts().contains("host-2"); //$NON-NLS-1$
	}

	public void testAddServiceBinding() throws Exception {
		ClusteredRegistryState state = buildRegistry();
		Cache rootNode = state.cache;
		
		assertEquals(2, rootNode.getChild(key("host-1")).getChild(key("vm-1")).getChild("Services").values().size()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals(1, rootNode.getChild(key("host-1")).getChild(key("vm-2")).getChild("Services").values().size()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals(3, rootNode.getChild(key("host-2")).getChild(key("vm-1")).getChild("Services").values().size()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	ClusteredRegistryState buildRegistry() throws ResourceAlreadyBoundException, CacheNodeNotFoundException {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		ServiceRegistryBinding s1 = buildServiceRegistryBinding(1, vm1, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s2 = buildServiceRegistryBinding(2, vm2, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s3 = buildServiceRegistryBinding(3, vm1, "Index"); //$NON-NLS-1$
		ServiceRegistryBinding s4 = buildServiceRegistryBinding(4, vm3, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s5 = buildServiceRegistryBinding(5, vm3, "Session"); //$NON-NLS-1$
		ServiceRegistryBinding s6 = buildServiceRegistryBinding(6, vm3, "Auth"); //$NON-NLS-1$
		
		state.addServiceBinding("host-1", "vm-1", s1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-1", "vm-1", s3); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addServiceBinding("host-1", "vm-2", s2); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addServiceBinding("host-2", "vm-1", s4); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-2", "vm-1", s5); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-2", "vm-1", s6); //$NON-NLS-1$ //$NON-NLS-2$
		return state;
	}

	public void testGetServiceBinding() throws Exception {
		ClusteredRegistryState state = buildRegistry();
	
		List<ServiceRegistryBinding> list = state.getServiceBindings("Query"); //$NON-NLS-1$
		assertEquals(3, list.size());

		list = state.getServiceBindings("host-1", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(2, list.size());
		
		list = state.getServiceBindings("host-1", "vm-2"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(1, list.size());

		list = state.getServiceBindings("host-2", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(3, list.size());
		
		list = state.getServiceBindings("host-1", null); //$NON-NLS-1$
		assertEquals(3, list.size());
		
		list = state.getServiceBindings(null, null);
		assertEquals(6, list.size());
	}


	public void testRemoveServiceBinding() throws Exception {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		
		HostControllerRegistryBinding host1 = buildHostRegistryBinding("host-1"); //$NON-NLS-1$
		HostControllerRegistryBinding host2 = buildHostRegistryBinding("host-2"); //$NON-NLS-1$
		state.addHost(host1);
		state.addHost(host2);
		
		ProcessRegistryBinding vm1 = buildVMRegistryBinding("host-1", "1"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm2 = buildVMRegistryBinding("host-1", "2"); //$NON-NLS-1$ //$NON-NLS-2$
		ProcessRegistryBinding vm3 = buildVMRegistryBinding("host-1", "3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addProcess("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addProcess("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		ServiceRegistryBinding s1 = buildServiceRegistryBinding(1, vm1, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s2 = buildServiceRegistryBinding(2, vm2, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s3 = buildServiceRegistryBinding(3, vm1, "Index"); //$NON-NLS-1$
		ServiceRegistryBinding s4 = buildServiceRegistryBinding(4, vm3, "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s5 = buildServiceRegistryBinding(5, vm3, "Session"); //$NON-NLS-1$
		ServiceRegistryBinding s6 = buildServiceRegistryBinding(6, vm3, "Auth"); //$NON-NLS-1$
		
		state.addServiceBinding("host-1", "vm-1", s1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-1", "vm-1", s3); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addServiceBinding("host-1", "vm-2", s2); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.addServiceBinding("host-2", "vm-1", s4); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-2", "vm-1", s5); //$NON-NLS-1$ //$NON-NLS-2$
		state.addServiceBinding("host-2", "vm-1", s6); //$NON-NLS-1$ //$NON-NLS-2$
		
		state.removeServiceBinding("host-1", "vm-1", s1.getServiceID()); //$NON-NLS-1$ //$NON-NLS-2$
		state.removeServiceBinding("host-2", "vm-1", s6.getServiceID()); //$NON-NLS-1$ //$NON-NLS-2$
		
		List<ServiceRegistryBinding> list = state.getServiceBindings("Query"); //$NON-NLS-1$
		assertEquals(2, list.size());

		list = state.getServiceBindings("host-1", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(1, list.size());
		
		list = state.getServiceBindings("host-1", "vm-2"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(1, list.size());

		list = state.getServiceBindings("host-2", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals(2, list.size());
		
		list = state.getServiceBindings("host-1", null); //$NON-NLS-1$
		assertEquals(2, list.size());
		
		list = state.getServiceBindings(null, null);
		assertEquals(4, list.size());
		
	}
	
	public void testUpdate() {
		ClusteredRegistryState state = new ClusteredRegistryState(factory);
		Cache rootNode = state.cache;
		
		Cache n = rootNode.addChild("test"); //$NON-NLS-1$
		n.put("x", new Foo()); //$NON-NLS-1$
		
		Foo f = (Foo)n.get("x"); //$NON-NLS-1$
		f.update = "updated"; //$NON-NLS-1$
		
		f = (Foo)n.get("x"); //$NON-NLS-1$
		assertEquals("updated", f.update); //$NON-NLS-1$
	}
	
	static class Foo{
		String update = "start"; //$NON-NLS-1$
	}
	
	
	static ProcessRegistryBinding buildVMRegistryBinding(String hostName, String vmID) {
	    HostID hostID1 = new HostID(hostName); 
	    VMComponentDefnID defnID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID1, "process1");  //$NON-NLS-1$
	    VMComponentDefn defn1 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID1, defnID1, new ComponentTypeID(VMComponentDefnType.COMPONENT_TYPE_NAME)); 
	    ProcessManagement vmInterface1 = SimpleMock.createSimpleMock(ProcessManagement.class);
	    ProcessRegistryBinding binding =  new ProcessRegistryBinding(hostName, vmID, defn1,vmInterface1, new NoOpMessageBus());
	    binding.setAlive(true);
	    return binding;
	}

	static ServiceRegistryBinding buildServiceRegistryBinding(int id, ProcessRegistryBinding process, String type) {
		ServiceID sid = new ServiceID(id, process.getHostName(), process.getProcessName());
		//ServiceInterface si = SimpleMock.createSimpleMock(ServiceInterface.class);
	    return new ServiceRegistryBinding(sid, null, type,"instance-"+id, null,"deployed-"+id, process.getHostName(), null,  ServiceState.STATE_OPEN,new Date(), false, new NoOpMessageBus());	 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	static HostControllerRegistryBinding buildHostRegistryBinding(String name) {
		return new HostControllerRegistryBinding(name, new Properties(), null, new NoOpMessageBus());
	}
}
