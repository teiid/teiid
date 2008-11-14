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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jboss.cache.Cache;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.notifications.annotation.CacheListener;
import org.jboss.cache.notifications.annotation.NodeCreated;
import org.jboss.cache.notifications.annotation.NodeModified;
import org.jboss.cache.notifications.annotation.NodeMoved;
import org.jboss.cache.notifications.annotation.NodeRemoved;
import org.jboss.cache.notifications.event.NodeEvent;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceID;

@Singleton
@CacheListener
public class ClusteredRegistryState {

	private static final String REGISTRY = "Registry"; //$NON-NLS-1$
	private static final String VM_CONTROLLER = "VM_Controller"; //$NON-NLS-1$
	private static final String SERVICES = "Services"; //$NON-NLS-1$
	private static final String RESOURCE_POOL = "ResourcePool"; //$NON-NLS-1$
	private static final String NAME = "Name"; //$NON-NLS-1$
	
	private Node rootRegistryNode;
	private Cache cache;
	private List<RegistryListener> listeners = Collections.synchronizedList(new ArrayList<RegistryListener>());
	
	@Inject
	public ClusteredRegistryState(Cache cacheStore) {
		this.cache = cacheStore;
		Node rootNode = cacheStore.getRoot();
		this.rootRegistryNode = rootNode.addChild(Fqn.fromString(REGISTRY));
		cache.addCacheListener(this);
	}
	
	private Node addHost(String hostName) {
		Fqn fqn = Fqn.fromString(hostName);
		Node n = this.rootRegistryNode.addChild(fqn);
		n.put(NAME, hostName);
		return n;
	}
	
	private Node getHostNode(String hostName) throws NodeNotFoundException {
		Fqn fqn = Fqn.fromString(hostName);
		Node node =  this.rootRegistryNode.getChild(fqn);
		if (node == null) {
			throw new NodeNotFoundException("Host Node not found");	 //$NON-NLS-1$
		}
		return node;
	}
	
	private Node addVMNode(String hostName, String vmName) {
		Node hostNode = addHost(hostName);
		Fqn fqn = Fqn.fromString(vmName);
		Node n =  hostNode.addChild(fqn);
		n.put(NAME, vmName);
		return n;
	}
	
	private Node getVMNode(String hostName, String vmName) throws NodeNotFoundException {
		Node hostNode = getHostNode(hostName);
		Node vmNode = hostNode.getChild(Fqn.fromString(vmName));
		if (vmNode == null) {
			throw new NodeNotFoundException("VM Node not found"); //$NON-NLS-1$
		}
		
		// only return the active vms
		VMRegistryBinding binding = (VMRegistryBinding)vmNode.get(VM_CONTROLLER);
		if (!binding.isAlive()) {
			throw new NodeNotFoundException("VM Node not found"); //$NON-NLS-1$
		}
		return vmNode;
	}
		
	protected void addVM(String hostName, String vmName, VMRegistryBinding vmBinding) {
		Node vmNode = addVMNode(hostName, vmName);
		vmNode.put(VM_CONTROLLER, vmBinding);
	}
	
	protected void removeVM(String hostName, String vmName) {
		try {
			Node hostNode = getHostNode(hostName);
			hostNode.removeChild(Fqn.fromString(vmName));
		} catch (NodeNotFoundException e) {
			// this is OK, this is already gone.
		}
	}

	/**
	 * Returns the all the host that have been known to registry.
	 * @return
	 */
	public List<String> getHosts(){
		ArrayList<String> list = new ArrayList<String>();
		Set<Node> hostNodes = rootRegistryNode.getChildren();
		for(Node hostNode:hostNodes) {
			list.add((String)hostNode.get(NAME));
		}
		return list;
	}
	
	/**
	 * Gets all the VMs for a given Host. If 'null' is passed in for the host name
	 * it will return all the VMs in the cluster.
	 * @param hostName
	 * @return
	 */
	public List<VMRegistryBinding> getVMs(String hostName){
		return getVMs(hostName, new ArrayList());
	}
	
	private List<VMRegistryBinding> getVMs(String hostName, ArrayList list){
		
		if (hostName == null) {
			Set<Node> hostNodes = this.rootRegistryNode.getChildren();
			for (Node hostNode:hostNodes) {
				getVMs((String)hostNode.get(NAME), list);
			}
		}
		else {
			try {
				Node hostNode = getHostNode(hostName);
				Set<Node> vmNodes = hostNode.getChildren();
				for (Node vmNode:vmNodes) {
					list.add(vmNode.get(VM_CONTROLLER));
				}
			} catch (NodeNotFoundException e) {
				// OK, just return empty list.
			}
		}
		return list;
	}
	
	public VMRegistryBinding getVM(String hostName, String vmName) throws ResourceNotBoundException {
		try {
			Node vmNode = getVMNode(hostName, vmName);
			VMRegistryBinding binding = (VMRegistryBinding)vmNode.get(VM_CONTROLLER);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+vmName )); //$NON-NLS-1$				
			}
			return binding;
		} catch (NodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+vmName )); //$NON-NLS-1$
		} 
	}
	
	
	protected void addServiceBinding(String hostName, String vmName, ServiceRegistryBinding binding) throws ResourceAlreadyBoundException {
		Node vmNode = addVMNode(hostName, vmName);

		// get/add the services node
        Node services = vmNode.addChild(Fqn.fromString(SERVICES));
		
		// check if this service already exists
		ServiceRegistryBinding existing = (ServiceRegistryBinding)services.get(binding.getServiceID());
        if (existing != null && !existing.isServiceBad()) {
            throw new ResourceAlreadyBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0024, binding.getServiceID() ));
        }
		
        services.put(binding.getServiceID(), binding);
	}
	
	public ServiceRegistryBinding getServiceBinding(String hostName, String vmName, ServiceID serviceId ) throws ResourceNotBoundException {
		ServiceRegistryBinding binding;
		try {
			Node vmNode = getVMNode(hostName, vmName);
			Node services = vmNode.addChild(Fqn.fromString(SERVICES));
			binding = (ServiceRegistryBinding)services.get(serviceId);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, serviceId ));
			}
		} catch (NodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, serviceId ));			
		}
        return binding;
	}


	public List<ServiceRegistryBinding> getActiveServiceBindings(String hostName, String vmName, String serviceType) {
		return getServiceBindings(hostName, vmName, serviceType);
	}
	
	public List<ServiceRegistryBinding> getServiceBindings(String serviceType) {
		return getServiceBindings(null, null, serviceType);
	}

	public List<ServiceRegistryBinding> getServiceBindings(String hostName, String vmName, String serviceType) {
		return getServiceBindings(hostName, vmName, serviceType, false);
	}
	
	private List<ServiceRegistryBinding> getServiceBindings(String hostName, String vmName, String serviceType, boolean active) {
		List services = new ArrayList();
		List<ServiceRegistryBinding> bindings = getServiceBindings(hostName, vmName);
		for (ServiceRegistryBinding binding:bindings) {
            if (binding.getServiceType().equals(serviceType)) {
            	if (!active || (active && binding.isActive())) {
            		services.add(binding);
            	}
            }			
		}
		return services;
    }
    
	public List<ServiceRegistryBinding> getServiceBindings(String hostName, String vmName) {
		return getServiceBindings(hostName, vmName, new ArrayList());
	}	
	
	private List<ServiceRegistryBinding> getServiceBindings(String hostName, String vmName, ArrayList list) {
		
		try {
			if (hostName == null && vmName == null) {
				Set<Node> hostNodes = this.rootRegistryNode.getChildren();
				for(Node host:hostNodes) {
					getServiceBindings((String)host.get(NAME), null, list);
				}
			}
			else if (hostName != null && vmName == null) {
				Set<Node> vmNodes = getHostNode(hostName).getChildren();
				for(Node vm:vmNodes) {
					getServiceBindings(hostName, (String)vm.get(NAME), list);
				}
			}
			else if (hostName != null && vmName != null) {
				Node vmNode = getVMNode(hostName, vmName);
			    Node services = vmNode.addChild(Fqn.fromString(SERVICES));
			    
			    list.addAll(services.getData().values());
			}
		} catch (NodeNotFoundException e) {
			// this OK, this should not happen however in error return the empty list.
		}
		return list;
	}	
	
	protected void removeServiceBinding(String hostName, String vmName, ServiceID serviceId) {
		try {
			Node vmNode = getVMNode(hostName, vmName);
			Node services = vmNode.addChild(Fqn.fromString(SERVICES));
			services.remove(serviceId);
		} catch (NodeNotFoundException e) {
			// OK, this already gone.
		}
	}
	
	
	protected void addResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrBinding binding) throws ResourceAlreadyBoundException {
		Node vmNode = addVMNode(hostName, vmName);
        Node resources = vmNode.addChild(Fqn.fromString(RESOURCE_POOL));
		ResourcePoolMgrBinding existing = (ResourcePoolMgrBinding)resources.get(binding.getID());
		if (existing != null) {
			throw new ResourceAlreadyBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0025, binding.getID()));
		}
		resources.put(binding.getID(), binding);
	}
	
	public ResourcePoolMgrBinding getResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrID id) throws ResourceNotBoundException {
		try {
			Node vmNode = getVMNode(hostName, vmName);
			Node resources = vmNode.addChild(Fqn.fromString(RESOURCE_POOL));
			ResourcePoolMgrBinding binding =  (ResourcePoolMgrBinding)resources.get(id);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0010, id));				
			}
			return binding;
		} catch (NodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0010, id));
		}
	}
	
	public List<ResourcePoolMgrBinding> getResourcePoolManagerBindings(String hostName, String vmName) {
		return getResourcePoolManagerBindings(hostName, vmName, new ArrayList());
	}
	
	private List<ResourcePoolMgrBinding> getResourcePoolManagerBindings(String hostName, String vmName, ArrayList list) {	
		try {
			if (hostName == null && vmName == null) {
				Set<Node> hostNodes = this.rootRegistryNode.getChildren();
				for(Node host:hostNodes) {
					getResourcePoolManagerBindings((String)host.get(NAME), null, list);
				}
			}
			else if (hostName != null && vmName == null) {
				Set<Node> vmNodes = getHostNode(hostName).getChildren();
				for(Node vm:vmNodes) {
					getResourcePoolManagerBindings(hostName, (String)vm.get(NAME), list);
				}
			}
			else if (hostName != null && vmName != null) {
				Node vmNode = getVMNode(hostName, vmName);
				Node resources = vmNode.addChild(Fqn.fromString(RESOURCE_POOL));
				list.addAll(resources.getData().values());
			}
		} catch (NodeNotFoundException e) {
			// this is Ok, just return the empty list.
		}
		return list;
	}
	
	
	protected void removeResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrID managerId) {
		try {
			Node vmNode = getVMNode(hostName, vmName);
			Node resources = vmNode.addChild(Fqn.fromString(RESOURCE_POOL));
			resources.remove(managerId);
		} catch (NodeNotFoundException e) {
			// Ok, this is already gone.
		}
	}
	
	static class NodeNotFoundException extends Exception{
		public NodeNotFoundException(String msg) {
			super(msg);
		}
	}
	
	/**
	 * Add a listener for the registry node
	 * @param obj
	 */
	public void addListener(RegistryListener obj) {
		this.listeners.add(obj);
	}
	
    @NodeCreated
	@NodeRemoved
	@NodeModified
	@NodeMoved
	void registryChanged(NodeEvent ne) {
    	Fqn fqn = ne.getFqn();
    	if (fqn.isChildOf(rootRegistryNode.getFqn())) {
    		for(RegistryListener l:this.listeners) {
    			l.registryChanged();
    		}
    	}
	} 
}
