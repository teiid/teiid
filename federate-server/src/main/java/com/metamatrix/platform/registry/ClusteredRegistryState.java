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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.CacheListener;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceID;

@Singleton
public class ClusteredRegistryState implements CacheListener {

	private static final String VM_CONTROLLER = "VM_Controller"; //$NON-NLS-1$
	private static final String SERVICES = "Services"; //$NON-NLS-1$
	private static final String RESOURCE_POOL = "ResourcePool"; //$NON-NLS-1$
	private static final String NAME = "Name"; //$NON-NLS-1$
	
	Cache cache;
	private List<RegistryListener> listeners = Collections.synchronizedList(new ArrayList<RegistryListener>());
	
	@Inject
	public ClusteredRegistryState(CacheFactory cacheFactory) {
		this.cache = cacheFactory.get(Cache.Type.REGISTRY, new CacheConfiguration(Policy.LRU, 0, 20000));
		this.cache.addListener(this);
	}
	
	private Cache getHostNode(String hostName) throws CacheNodeNotFoundException {
		Cache node =  this.cache.getChild(hostName.toUpperCase());
		if (node == null) {
			throw new CacheNodeNotFoundException("Host Node not found="+hostName);	 //$NON-NLS-1$
		}
		return node;
	}
	
	private Cache addVMNode(String hostName, String vmName) throws CacheNodeNotFoundException {
		Cache hostNode = getHostNode(hostName);
		Cache n =  hostNode.addChild(vmName.toUpperCase());
		n.put(NAME, vmName);
		return n;
	}
	
	private Cache getVMNode(String hostName, String vmName) throws CacheNodeNotFoundException {
		Cache hostNode = getHostNode(hostName);
		Cache vmNode = hostNode.getChild(vmName.toUpperCase());
		if (vmNode == null) {
			throw new CacheNodeNotFoundException("VM Node not found="+vmName); //$NON-NLS-1$
		}
		
		// only return the active vms
		VMRegistryBinding binding = (VMRegistryBinding)vmNode.get(VM_CONTROLLER);
		if (!binding.isAlive()) {
			throw new CacheNodeNotFoundException("VM Node not found"); //$NON-NLS-1$
		}
		return vmNode;
	}
		
	protected void addHost(HostControllerRegistryBinding binding) {
		String hostName = binding.getHostName().toUpperCase();
		Cache n = this.cache.addChild(hostName);
		n.put(NAME, hostName);
		n.put(hostName, binding);
	}
	
	protected void removeHost(String hostName) {
		this.cache.removeChild(hostName.toUpperCase());
	}
	
	protected void addVM(String hostName, String vmName, VMRegistryBinding vmBinding) throws CacheNodeNotFoundException {
		Cache vmNode = addVMNode(hostName, vmName);
		vmNode.put(VM_CONTROLLER, vmBinding);
	}
	
	protected void removeVM(String hostName, String vmName) {
		try {
			Cache hostNode = getHostNode(hostName);
			hostNode.removeChild(vmName.toUpperCase());
		} catch (CacheNodeNotFoundException e) {
			// this is OK, this is already gone.
		}
	}

	/**
	 * Returns the all the host that have been known to registry.
	 * @return
	 */
	public List<HostControllerRegistryBinding> getHosts(){
		ArrayList<HostControllerRegistryBinding> list = new ArrayList<HostControllerRegistryBinding>();
		Collection<Cache> hostNodes = this.cache.getChildren();
		for(Cache hostNode:hostNodes) {
			String hostName = (String)hostNode.get(NAME);
			list.add((HostControllerRegistryBinding)hostNode.get(hostName));
		}
		return list;
	}
	
	public HostControllerRegistryBinding getHost(String hostName) {
		try {
			Cache node = getHostNode(hostName);
			return (HostControllerRegistryBinding)node.get(hostName.toUpperCase());
		} catch (CacheNodeNotFoundException e) {
			return null;
		}
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
			Collection<Cache> hostNodes = this.cache.getChildren();
			for (Cache hostNode:hostNodes) {
				getVMs((String)hostNode.get(NAME), list);
			}
		}
		else {
			try {
				Cache hostNode = getHostNode(hostName);
				Collection<Cache> vmNodes = hostNode.getChildren();
				for (Cache vmNode:vmNodes) {
					list.add(vmNode.get(VM_CONTROLLER));
				}
			} catch (CacheNodeNotFoundException e) {
				// OK, just return empty list.
			}
		}
		return list;
	}
	
	public VMRegistryBinding getVM(String hostName, String vmName) throws ResourceNotBoundException {
		try {
			Cache vmNode = getVMNode(hostName, vmName);
			VMRegistryBinding binding = (VMRegistryBinding)vmNode.get(VM_CONTROLLER);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+vmName )); //$NON-NLS-1$				
			}
			return binding;
		} catch (CacheNodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+vmName )); //$NON-NLS-1$
		} 
	}
	
	
	protected void addServiceBinding(String hostName, String vmName, ServiceRegistryBinding binding) throws ResourceAlreadyBoundException, CacheNodeNotFoundException {
		Cache services = getServices(hostName, vmName);

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
	        Cache services = getServices(hostName, vmName);
			binding = (ServiceRegistryBinding)services.get(serviceId);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, serviceId ));
			}
		} catch (CacheNodeNotFoundException e) {
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
				Collection<Cache> hostNodes = this.cache.getChildren();
				for(Cache host:hostNodes) {
					getServiceBindings((String)host.get(NAME), null, list);
				}
			}
			else if (hostName != null && vmName == null) {
				Collection<Cache> vmNodes = getHostNode(hostName).getChildren();
				for(Cache vm:vmNodes) {
					getServiceBindings(hostName, (String)vm.get(NAME), list);
				}
			}
			else if (hostName != null && vmName != null) {
				Cache services = getServices(hostName, vmName);
				list.addAll(services.values());
			}
		} catch (CacheNodeNotFoundException e) {
			// this OK, this should not happen however in error return the empty list.
		}
		return list;
	}	
	
	private Cache getServices(String hostName, String vmName) throws CacheNodeNotFoundException {
		Cache vmNode = getVMNode(hostName, vmName);
        Cache services = vmNode.getChild(SERVICES);
        if (services == null) {
        	services = vmNode.addChild(SERVICES);
        }		
        return services;
	}
	
	private Cache getResourcePool(String hostName, String vmName) throws CacheNodeNotFoundException {
		Cache vmNode = getVMNode(hostName, vmName);
        Cache services = vmNode.getChild(RESOURCE_POOL);
        if (services == null) {
        	services = vmNode.addChild(RESOURCE_POOL);
        }		
        return services;
	}	
	
	protected void removeServiceBinding(String hostName, String vmName, ServiceID serviceId) {
		try {
			Cache services = getServices(hostName, vmName);
			services.remove(serviceId);
		} catch (CacheNodeNotFoundException e) {
			// OK, this already gone.
		}
	}
	
	
	protected void addResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrBinding binding) throws ResourceAlreadyBoundException, CacheNodeNotFoundException {
        Cache resources = getResourcePool(hostName, vmName);
		ResourcePoolMgrBinding existing = (ResourcePoolMgrBinding)resources.get(binding.getID());
		if (existing != null) {
			throw new ResourceAlreadyBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0025, binding.getID()));
		}
		resources.put(binding.getID(), binding);
	}
	
	public ResourcePoolMgrBinding getResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrID id) throws ResourceNotBoundException {
		try {
	        Cache resources = getResourcePool(hostName, vmName);
			ResourcePoolMgrBinding binding =  (ResourcePoolMgrBinding)resources.get(id);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0010, id));				
			}
			return binding;
		} catch (CacheNodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0010, id));
		}
	}
	
	public List<ResourcePoolMgrBinding> getResourcePoolManagerBindings(String hostName, String vmName) {
		return getResourcePoolManagerBindings(hostName, vmName, new ArrayList());
	}
	
	private List<ResourcePoolMgrBinding> getResourcePoolManagerBindings(String hostName, String vmName, ArrayList list) {	
		try {
			if (hostName == null && vmName == null) {
				Collection<Cache> hostNodes = this.cache.getChildren();
				for(Cache host:hostNodes) {
					getResourcePoolManagerBindings((String)host.get(NAME), null, list);
				}
			}
			else if (hostName != null && vmName == null) {
				Collection<Cache> vmNodes = getHostNode(hostName).getChildren();
				for(Cache vm:vmNodes) {
					getResourcePoolManagerBindings(hostName, (String)vm.get(NAME), list);
				}
			}
			else if (hostName != null && vmName != null) {
		        Cache resources = getResourcePool(hostName, vmName);
				list.addAll(resources.values());
			}
		} catch (CacheNodeNotFoundException e) {
			// this is Ok, just return the empty list.
		}
		return list;
	}
	
	
	protected void removeResourcePoolManagerBinding(String hostName, String vmName, ResourcePoolMgrID managerId) {
		try {
	        Cache resources = getResourcePool(hostName, vmName);
			resources.remove(managerId);
		} catch (CacheNodeNotFoundException e) {
			// Ok, this is already gone.
		}
	}
	
	static class CacheNodeNotFoundException extends Exception{
		public CacheNodeNotFoundException(String msg) {
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
	
	@Override
	public void cacheChanged() {
		for(RegistryListener l:this.listeners) {
			l.registryChanged();
		}		
	} 
}
