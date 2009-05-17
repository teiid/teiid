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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
	private static final String NAME = "Name"; //$NON-NLS-1$
	
	Cache cache;
	private Queue<RegistryListener> listeners = new ConcurrentLinkedQueue<RegistryListener>();
	
	@Inject
	public ClusteredRegistryState(CacheFactory cacheFactory) {
		this.cache = cacheFactory.get(Cache.Type.REGISTRY, new CacheConfiguration(Policy.LRU, 0, 0));
		this.cache.addListener(this);
	}
	
	private Cache getHostNode(String hostName) throws CacheNodeNotFoundException {
		Cache node =  this.cache.getChild(hostName.toUpperCase());
		if (node == null) {
			throw new CacheNodeNotFoundException("Host Node not found="+hostName);	 //$NON-NLS-1$
		}
		return node;
	}
	
	private Cache addProcessNode(String hostName, String processName) throws CacheNodeNotFoundException {
		Cache hostNode = getHostNode(hostName);
		hostNode.removeChild(processName.toUpperCase());
		Cache n =  hostNode.addChild(processName.toUpperCase());
		n.put(NAME, processName);
		return n;
	}
	
	private Cache getProcessNode(String hostName, String processName) throws CacheNodeNotFoundException {
		Cache hostNode = getHostNode(hostName);
		Cache vmNode = hostNode.getChild(processName.toUpperCase());
		if (vmNode == null) {
			throw new CacheNodeNotFoundException("VM Node not found="+processName); //$NON-NLS-1$
		}
		
		// only return the active vms
		ProcessRegistryBinding binding = (ProcessRegistryBinding)vmNode.get(VM_CONTROLLER);
		if (binding == null || !binding.isAlive()) {
			throw new CacheNodeNotFoundException("VM Node's binding not found or not active"); //$NON-NLS-1$
		}
		return vmNode;
	}
		
	protected synchronized void addHost(HostControllerRegistryBinding binding) {
		String hostName = binding.getHostName().toUpperCase();
		Cache n = this.cache.addChild(hostName);
		n.put(NAME, hostName);
		n.put(hostName, binding);
	}
	
	protected void removeHost(String hostName) {
		this.cache.removeChild(hostName.toUpperCase());
	}
	
	protected synchronized void addProcess(String hostName, String processName, ProcessRegistryBinding vmBinding) throws CacheNodeNotFoundException {
		Cache vmNode = addProcessNode(hostName, processName);
		vmNode.put(VM_CONTROLLER, vmBinding);
	}
	
	protected void updateProcess(String hostName, String processName, ProcessRegistryBinding binding) throws ResourceNotBoundException, CacheNodeNotFoundException {
		Cache vmNode = getProcessNode(hostName, processName);
		if (vmNode.get(VM_CONTROLLER) == null) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+processName )); //$NON-NLS-1$
		}
		vmNode.put(VM_CONTROLLER, binding);		
	} 	
	
	protected void removeProcess(String hostName, String processName) {
		try {
			Cache hostNode = getHostNode(hostName);
			hostNode.removeChild(processName.toUpperCase());
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
	public List<ProcessRegistryBinding> getVMs(String hostName){
		return getVMs(hostName, new ArrayList());
	}
	
	private List<ProcessRegistryBinding> getVMs(String hostName, ArrayList list){
		
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
	
	public ProcessRegistryBinding getProcessBinding(String hostName, String processName) throws ResourceNotBoundException {
		try {
			Cache vmNode = getProcessNode(hostName, processName);
			ProcessRegistryBinding binding = (ProcessRegistryBinding)vmNode.get(VM_CONTROLLER);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+processName )); //$NON-NLS-1$				
			}
			return binding;
		} catch (CacheNodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0012, hostName+"/"+processName )); //$NON-NLS-1$
		} 
	}
	
	
	protected synchronized void addServiceBinding(String hostName, String processName, ServiceRegistryBinding binding) throws ResourceAlreadyBoundException, CacheNodeNotFoundException {
		Cache services = getServices(hostName, processName);

		// check if this service already exists
		ServiceRegistryBinding existing = (ServiceRegistryBinding)services.get(binding.getServiceID());
        if (existing != null && !existing.isServiceBad()) {
            throw new ResourceAlreadyBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0024, binding.getServiceID() ));
        }
		
        services.put(binding.getServiceID(), binding);
	}
	
	protected void updateServiceBinding(String hostName, String processName, ServiceRegistryBinding binding) throws ResourceNotBoundException, CacheNodeNotFoundException {
		Cache services = getServices(hostName, processName);

		// check if this service already exists
		ServiceRegistryBinding existing = (ServiceRegistryBinding)services.get(binding.getServiceID());
        if (existing == null) {
        	throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, binding.getServiceID() ));
        }
		
        services.put(binding.getServiceID(), binding);	
   }	
	
	public ServiceRegistryBinding getServiceBinding(String hostName, String processName, ServiceID serviceId ) throws ResourceNotBoundException {
		ServiceRegistryBinding binding;
		try {
	        Cache services = getServices(hostName, processName);
			binding = (ServiceRegistryBinding)services.get(serviceId);
			if (binding == null) {
				throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, serviceId ));
			}
		} catch (CacheNodeNotFoundException e) {
			throw new ResourceNotBoundException(ServicePlugin.Util.getString(ServiceMessages.REGISTRY_0011, serviceId ));			
		}
        return binding;
	}


	public List<ServiceRegistryBinding> getActiveServiceBindings(String hostName, String processName, String serviceType) {
		return getServiceBindings(hostName, processName, serviceType);
	}
	
	public List<ServiceRegistryBinding> getServiceBindings(String serviceType) {
		return getServiceBindings(null, null, serviceType);
	}

	public List<ServiceRegistryBinding> getServiceBindings(String hostName, String processName, String serviceType) {
		return getServiceBindings(hostName, processName, serviceType, false);
	}
	
	private List<ServiceRegistryBinding> getServiceBindings(String hostName, String processName, String serviceType, boolean active) {
		List services = new ArrayList();
		List<ServiceRegistryBinding> bindings = getServiceBindings(hostName, processName);
		for (ServiceRegistryBinding binding:bindings) {
            if (binding.getServiceType().equals(serviceType)) {
            	if (!active || (active && binding.isActive())) {
            		services.add(binding);
            	}
            }			
		}
		return services;
    }
    
	public List<ServiceRegistryBinding> getServiceBindings(String hostName, String processName) {
		return getServiceBindings(hostName, processName, new ArrayList());
	}	
	
	private List<ServiceRegistryBinding> getServiceBindings(String hostName, String processName, ArrayList list) {
		
		try {
			if (hostName == null && processName == null) {
				Collection<Cache> hostNodes = this.cache.getChildren();
				for(Cache host:hostNodes) {
					getServiceBindings((String)host.get(NAME), null, list);
				}
			}
			else if (hostName != null && processName == null) {
				Collection<Cache> vmNodes = getHostNode(hostName).getChildren();
				for(Cache vm:vmNodes) {
					getServiceBindings(hostName, (String)vm.get(NAME), list);
				}
			}
			else if (hostName != null && processName != null) {
				Cache services = getServices(hostName, processName);
				list.addAll(services.values());
			}
		} catch (CacheNodeNotFoundException e) {
			// this OK, this should not happen however in error return the empty list.
		}
		return list;
	}	
	
	private Cache getServices(String hostName, String processName) throws CacheNodeNotFoundException {
		Cache vmNode = getProcessNode(hostName, processName);
        Cache services = vmNode.getChild(SERVICES);
        if (services == null) {
        	services = vmNode.addChild(SERVICES);
        }		
        return services;
	}
	
	protected void removeServiceBinding(String hostName, String processName, ServiceID serviceId) {
		try {
			Cache services = getServices(hostName, processName);
			services.remove(serviceId);
		} catch (CacheNodeNotFoundException e) {
			// OK, this already gone.
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
	
	public void shutdown() {
		this.cache.removeListener();
		for(RegistryListener l:this.listeners) {
			l.registryShutdown();
		}
		this.listeners.clear();
	}
}
