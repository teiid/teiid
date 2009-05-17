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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.RemoteMessagingException;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.registry.ClusteredRegistryState.CacheNodeNotFoundException;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.platform.vm.controller.ServerEvents;
import com.metamatrix.server.Configuration;

/**
 * This really needs to be the part of/ or be the VMController. 
 *
 */
@Singleton
public class ProcessMonitor implements ServerEvents {

	private static final int POLLING_INTERVAL_DEFAULT = 1000*15;

	@Named("metamatrix.server.serviceMonitorInterval") // what is good usage of these kind ?? 
	int pollingIntervel = POLLING_INTERVAL_DEFAULT;
	
	ClusteredRegistryState registry;
	String hostName;
	String processName;
	
    // Monitors remote vm's to detect when they go down.
    private Timer vmPollingThread = new Timer("VMPollingThread", true); //$NON-NLS-1$
    private Timer servicePollingThread = new Timer("ServiceMonitoringThread", true); //$NON-NLS-1$
    
	@Inject
	public ProcessMonitor (@Named(Configuration.HOSTNAME)String hostName, @Named(Configuration.PROCESSNAME)String processname, ClusteredRegistryState registry) {
		
		this.hostName = hostName;
		this.processName = processname;
		this.registry = registry;
		
		// check the health of the other VMS
		startVMPollingThread();
		
		// check health of the local services
		startServicePollingThread();
	}
	
	/**
	 * Should be called when the local VM is going down.
	 */
	public void shutdown() {
		this.registry.removeProcess(this.hostName, this.processName);
	}
	
    private void startVMPollingThread() {
        // Poll vm's to detect when a vm crashes
    	vmPollingThread.schedule(new TimerTask() {
    		@Override
    		public void run() {
    			List<ProcessRegistryBinding> vmBindings = ProcessMonitor.this.registry.getVMs(null);

    			for (ProcessRegistryBinding binding: vmBindings) {
                    ProcessManagement vm = binding.getProcessController();
                    try {
	                    vm.ping();
	                    binding.setAlive(true);
	                } catch (ServiceException e) {
	                	// mark as not alive, then no services will be pinged from this vm
	                	binding.setAlive(false);
	                } catch (RemoteMessagingException e) {
	                	binding.setAlive(false);
	                }
	            }
    		}
    	}, POLLING_INTERVAL_DEFAULT, POLLING_INTERVAL_DEFAULT);
    }
    
    private void startServicePollingThread() {
        servicePollingThread.schedule(new TimerTask() {
        	@Override
        	public void run() {
        		List<ServiceRegistryBinding> bindings = registry.getServiceBindings(hostName, processName);
                for (ServiceRegistryBinding binding:bindings) {

    				// the state of the service changed, then update the cache.
    				binding.checkState();
    				
    				// the state of the service changed, then update the cache.
    				if (binding.isDirty()) {
    					serviceUpdated(binding);
    					binding.setDirty(false);
    				}
                }
        	}
        }, pollingIntervel, pollingIntervel);
    }

	public void serviceAdded(ServiceRegistryBinding binding) {
		try {
			this.registry.addServiceBinding(hostName, processName, binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "Service Added:"+binding.getServiceID()); //$NON-NLS-1$
		} catch (ResourceAlreadyBoundException e) {
			LogManager.logWarning(LogCommonConstants.CTX_CONTROLLER, "Service already exists:"+binding.getServiceID()); //$NON-NLS-1$
		} catch(CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add service:"+binding.getServiceID()); //$NON-NLS-1$
		}
	}

	public void serviceRemoved(ServiceID id) {
		this.registry.removeServiceBinding(hostName, processName, id);		
		LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "Service removed:"+id); //$NON-NLS-1$
	}

	public void processAdded(ProcessRegistryBinding binding) {
		binding.setAlive(true);
		try {
			this.registry.addProcess(binding.getHostName(), binding.getProcessName(), binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "VM Added:"+binding); //$NON-NLS-1$
		} catch (CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add VM:"+binding); //$NON-NLS-1$
			throw new MetaMatrixRuntimeException("Failed to add VM:"+binding); //$NON-NLS-1$
		}
		
	}

	public void processRemoved(String hostName, String processName) {
		this.registry.removeProcess(hostName, processName);		
		LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "VM Removed:<"+hostName+"."+processName+">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	@Override
	public void serviceUpdated(ServiceRegistryBinding binding) {
		try {
			this.registry.updateServiceBinding(binding.getHostName(), binding.getServiceID().getProcessName(), binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "Service updated:"+binding.getServiceID()); //$NON-NLS-1$
		} catch(CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add service:"+binding.getServiceID()); //$NON-NLS-1$
		} catch (ResourceNotBoundException e) {
			LogManager.logWarning(LogCommonConstants.CTX_CONTROLLER, "Service not exist:"+binding.getServiceID()); //$NON-NLS-1$
		}
	}

	@Override
	public void processUpdated(ProcessRegistryBinding binding) {
		try {
			this.registry.updateProcess(binding.getHostName(), binding.getProcessName(), binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "VM Added:" + binding); //$NON-NLS-1$
		} catch (CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add VM:"+ binding); //$NON-NLS-1$
			throw new MetaMatrixRuntimeException("Failed to add VM:" + binding); //$NON-NLS-1$
		} catch(ResourceNotBoundException e) {
			LogManager.logWarning(LogCommonConstants.CTX_CONTROLLER, "VM does not exist:"+binding); //$NON-NLS-1$
		}
	}
}
