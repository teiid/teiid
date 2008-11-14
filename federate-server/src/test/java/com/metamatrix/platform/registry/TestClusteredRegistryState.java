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

import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.jboss.cache.Cache;
import org.jboss.cache.CacheFactory;
import org.jboss.cache.DefaultCacheFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMControllerIDImpl;

public class TestClusteredRegistryState extends TestCase {
	Cache cache;
	
	@Override
	protected void setUp() throws Exception {
		CacheFactory factory = new DefaultCacheFactory();
		cache = factory.createCache();
	}

	@Override
	protected void tearDown() throws Exception {
		cache.stop();
	}
	
	public void testAddVM() {		
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		Node rootNode = cache.getRoot().getChild("Registry"); //$NON-NLS-1$
		
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(rootNode.getChildren().size(), 2);
		assertNotNull(rootNode.getChild(Fqn.fromString("host-1"))); //$NON-NLS-1$
		assertNotNull(rootNode.getChild(Fqn.fromString("host-2"))); //$NON-NLS-1$
		assertNotNull(rootNode.getChild(Fqn.fromString("host-2")).getChild(Fqn.fromString("vm-1"))); //$NON-NLS-1$ //$NON-NLS-2$

		assertEquals(rootNode.getChild(Fqn.fromString("host-1")).get("Name"), "host-1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals(rootNode.getChild(Fqn.fromString("host-1")).getChild(Fqn.fromString("vm-1")).get("Name"), "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testRemoveVM() {
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		Node rootNode = cache.getRoot().getChild("Registry"); //$NON-NLS-1$
		
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(rootNode.getChildren().size(), 2);
		
		state.removeVM("host-1", "vm-1"); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertNull(rootNode.getChild(Fqn.fromString("host-1")).getChild(Fqn.fromString("vm-1"))); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(rootNode.getChild(Fqn.fromString("host-1")).getChild(Fqn.fromString("vm-2"))); //$NON-NLS-1$ //$NON-NLS-2$
		assertNotNull(rootNode.getChild(Fqn.fromString("host-2")).getChild(Fqn.fromString("vm-1")));		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testGetVMs() throws Exception {
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertEquals(0, state.getVMs("unknown").size()); //$NON-NLS-1$
		assertEquals(2, state.getVMs("host-1").size()); //$NON-NLS-1$
		assertEquals(1, state.getVMs("host-2").size()); //$NON-NLS-1$
		assertEquals(3, state.getVMs(null).size());
				
		assertEquals(vm2, state.getVM("host-1", "vm-2")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testHosts() {
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$

		assertEquals(2, state.getHosts().size());
		
		state.getHosts().contains("host-1"); //$NON-NLS-1$
		state.getHosts().contains("host-2"); //$NON-NLS-1$
	}

	public void testAddServiceBinding() throws Exception {
		buildRegistry();
		Node rootNode = cache.getRoot().getChild("Registry"); //$NON-NLS-1$
		
		assertEquals(2, rootNode.getChild(Fqn.fromString("host-1/vm-1/Services")).getData().size()); //$NON-NLS-1$
		assertEquals(1, rootNode.getChild(Fqn.fromString("host-1/vm-2/Services")).getData().size()); //$NON-NLS-1$
		assertEquals(3, rootNode.getChild(Fqn.fromString("host-2/vm-1/Services")).getData().size()); //$NON-NLS-1$
	}

	ClusteredRegistryState buildRegistry() throws ResourceAlreadyBoundException {
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		ServiceRegistryBinding s1 = buildServiceRegistryBinding(1, vm1.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s2 = buildServiceRegistryBinding(2, vm2.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s3 = buildServiceRegistryBinding(3, vm1.getVMControllerID(), "Index"); //$NON-NLS-1$
		ServiceRegistryBinding s4 = buildServiceRegistryBinding(4, vm3.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s5 = buildServiceRegistryBinding(5, vm3.getVMControllerID(), "Session"); //$NON-NLS-1$
		ServiceRegistryBinding s6 = buildServiceRegistryBinding(6, vm3.getVMControllerID(), "Auth"); //$NON-NLS-1$
		
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
		ClusteredRegistryState state = new ClusteredRegistryState(cache);
		VMRegistryBinding vm1 = buildVMRegistryBinding("host-1", 1); //$NON-NLS-1$
		VMRegistryBinding vm2 = buildVMRegistryBinding("host-1", 2); //$NON-NLS-1$
		VMRegistryBinding vm3 = buildVMRegistryBinding("host-1", 3); //$NON-NLS-1$
		
		state.addVM("host-1", "vm-1", vm1); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-1", "vm-2", vm2); //$NON-NLS-1$ //$NON-NLS-2$
		state.addVM("host-2", "vm-1", vm3); //$NON-NLS-1$ //$NON-NLS-2$
		
		ServiceRegistryBinding s1 = buildServiceRegistryBinding(1, vm1.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s2 = buildServiceRegistryBinding(2, vm2.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s3 = buildServiceRegistryBinding(3, vm1.getVMControllerID(), "Index"); //$NON-NLS-1$
		ServiceRegistryBinding s4 = buildServiceRegistryBinding(4, vm3.getVMControllerID(), "Query"); //$NON-NLS-1$
		ServiceRegistryBinding s5 = buildServiceRegistryBinding(5, vm3.getVMControllerID(), "Session"); //$NON-NLS-1$
		ServiceRegistryBinding s6 = buildServiceRegistryBinding(6, vm3.getVMControllerID(), "Auth"); //$NON-NLS-1$
		
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
		Node rootNode = cache.getRoot();
		Node n = rootNode.addChild(Fqn.fromString("test")); //$NON-NLS-1$
		n.put("x", new Foo()); //$NON-NLS-1$
		
		Foo f = (Foo)n.get("x"); //$NON-NLS-1$
		f.update = "updated"; //$NON-NLS-1$
		
		f = (Foo)n.get("x"); //$NON-NLS-1$
		assertEquals("updated", f.update); //$NON-NLS-1$
	}
	
	static class Foo{
		String update = "start"; //$NON-NLS-1$
	}
	
	
	static VMRegistryBinding buildVMRegistryBinding(String hostName, int vmID) {
	    VMControllerID vmID1 = new VMControllerIDImpl(vmID, hostName);             
	    HostID hostID1 = new HostID(hostName); 
	    VMComponentDefnID defnID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID1, "process1");  //$NON-NLS-1$
	    VMComponentDefn defn1 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID1, defnID1, new ComponentTypeID(VMComponentDefnType.COMPONENT_TYPE_NAME)); 
	    VMControllerInterface vmInterface1 = SimpleMock.createSimpleMock(VMControllerInterface.class);
	    VMRegistryBinding binding =  new VMRegistryBinding(hostName, vmID1, defn1,vmInterface1, new NoOpMessageBus());
	    binding.setAlive(true);
	    return binding;
	}

	static ServiceRegistryBinding buildServiceRegistryBinding(int id, VMControllerID vmId, String type) {
		ServiceID sid = new ServiceID(id, vmId);
		//ServiceInterface si = SimpleMock.createSimpleMock(ServiceInterface.class);
	    return new ServiceRegistryBinding(sid, null, type,"instance-"+id, null,"deployed-"+id, vmId.getHostName(), null, null, ServiceInterface.STATE_OPEN,new Date(), false, new NoOpMessageBus());	 //$NON-NLS-1$ //$NON-NLS-2$
	}
}
