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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.registry.ClusteredRegistryState.CacheNodeNotFoundException;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.ServerEvents;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.server.Configuration;

/**
 * This really needs to be the part of/ or be the VMController. 
 *
 */
@Singleton
public class VMMonitor implements ServerEvents {

	private static final int POLLING_INTERVAL_DEFAULT = 1000*15;

	@Named("metamatrix.server.serviceMonitorInterval") // what is good usage of these kind ?? 
	int pollingIntervel = POLLING_INTERVAL_DEFAULT;
	
	ClusteredRegistryState registry;
	String hostName;
	String vmName;
	VMControllerID vmId;
	
    // Monitors remote vm's to detect when they go down.
    private Timer vmPollingThread = new Timer("VMPollingThread", true); //$NON-NLS-1$
    private Timer servicePollingThread = new Timer("ServiceMonitoringThread", true); //$NON-NLS-1$
    
	@Inject
	public VMMonitor (@Named(Configuration.HOSTNAME)String hostName, @Named(Configuration.VMNAME)String vmName, VMControllerID vmId, ClusteredRegistryState registry) {
		
		this.hostName = hostName;
		this.vmName = vmName;
		this.vmId = vmId;
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
		this.registry.removeVM(this.hostName, this.vmId.toString());
	}
	
    private void startVMPollingThread() {
        // Poll vm's to detect when a vm crashes
    	vmPollingThread.schedule(new TimerTask() {
    		@Override
    		public void run() {
    			List<VMRegistryBinding> vmBindings = VMMonitor.this.registry.getVMs(null);
    			for (VMRegistryBinding binding: vmBindings) {
	                try {
	                    VMControllerInterface vm = binding.getVMController();
	                    vm.ping();
	                    binding.setAlive(true);
	                } catch (ServiceException e) {
	                	// mark as not alive, then no services will be pinged from this vm
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
        		List<ServiceRegistryBinding> bindings = registry.getServiceBindings(hostName, vmId.toString());
                for (ServiceRegistryBinding binding:bindings) {
            		try {
            			// when service in stopped state; this will be null
            			// if shut down there will not be a binding for it.
            			ServiceInterface si = binding.getService();
            			if(si != null) {
            				binding.getService().checkState();
            			}
    				} catch (ServiceStateException e) {
    					// OK to throw up, service will capture the error to logs.
    				}
                }
        	}
        }, pollingIntervel, pollingIntervel);
    }

	public void serviceAdded(ServiceRegistryBinding binding) {
		try {
			this.registry.addServiceBinding(hostName, vmId.toString(), binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "Service Added:"+binding.getServiceID()); //$NON-NLS-1$
		} catch (ResourceAlreadyBoundException e) {
			LogManager.logWarning(LogCommonConstants.CTX_CONTROLLER, "Service already exists:"+binding.getServiceID()); //$NON-NLS-1$
		} catch(CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add service:"+binding.getServiceID()); //$NON-NLS-1$
		}
	}

	public void serviceRemoved(ServiceID id) {
		this.registry.removeServiceBinding(hostName, vmId.toString(), id);		
		LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "Service removed:"+id); //$NON-NLS-1$
	}

	public void vmAdded(VMRegistryBinding binding) {
		binding.setAlive(true);
		try {
			this.registry.addVM(hostName, vmId.toString(), binding);
			LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "VM Added:"+binding.getVMControllerID()); //$NON-NLS-1$
		} catch (CacheNodeNotFoundException e) {
			LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to add VM:"+binding.getVMControllerID()); //$NON-NLS-1$
			throw new MetaMatrixRuntimeException("Failed to add VM:"+binding.getVMControllerID()); //$NON-NLS-1$
		}
		
	}

	public void vmRemoved(VMControllerID id) {
		this.registry.removeVM(hostName, vmId.toString());		
		LogManager.logDetail(LogCommonConstants.CTX_CONTROLLER, "VM Removed:"+id); //$NON-NLS-1$
	}
}
