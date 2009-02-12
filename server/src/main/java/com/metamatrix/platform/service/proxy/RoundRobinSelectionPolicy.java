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
import java.util.List;

import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

public class RoundRobinSelectionPolicy implements ServiceSelectionPolicy  {

    // Does this policy prefer local services?
    private boolean preferLocal = false;

    List<ServiceRegistryBinding> localServices;
	List<ServiceRegistryBinding> remoteServices;    

	// State necessary to implement round robin
    private int localCurrentIndex = 0;
    private int currentIndex = 0;
    
    /**
     * ctor
     */
    public RoundRobinSelectionPolicy(boolean preferLocal) {
        this.preferLocal = preferLocal;
    }

    
    /**
     * Get the next service instance from the given service selection policy.
     * @throws ServiceNotFoundException if there are no services available.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public synchronized ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException {
        ServiceRegistryBinding serviceBinding = null;

        List allServices = this.remoteServices;
        if (this.preferLocal) {
	        if ( localServices != null && ! localServices.isEmpty() ) {
		        this.localCurrentIndex = (this.localCurrentIndex+1) % localServices.size();
	    	    serviceBinding = localServices.get(localCurrentIndex);
	        }
        }
        else {
        	allServices = new ArrayList(this.localServices);
        	allServices.addAll(this.remoteServices);
        }

        if ( serviceBinding == null ) {
            if ( allServices != null && ! allServices.isEmpty() ) {
	            this.currentIndex = (this.currentIndex+1) % allServices.size();
    			serviceBinding = (ServiceRegistryBinding) allServices.get(currentIndex);
            }
        }

        if ( serviceBinding == null ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0053, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, getClass().getSimpleName()));
        }

        return serviceBinding;
    }

    /**
     * Get list of instances from the given service selection policy.
     * @return The List of <code>ServiceRegistryBinding</code> objects for the instances after
     * being operated on by the selection policy.
     * @see com.metamatrix.common.service.ServiceInterface
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public synchronized List getInstances() throws ServiceNotFoundException {

        List newList = new ArrayList();

        List allServices = this.remoteServices;

        if (this.preferLocal) {
            if (!this.localServices.isEmpty() ) {
    	        this.localCurrentIndex = (this.localCurrentIndex+1) % localServices.size();
    	        newList.addAll(localServices.subList(this.localCurrentIndex, localServices.size()));
    	        newList.addAll(localServices.subList(0,this.localCurrentIndex));
            }        	
        }
        else {
        	allServices = new ArrayList(this.localServices);
        	allServices.addAll(this.remoteServices);
        }
        
        if ( allServices != null && !allServices.isEmpty() )  {
	        this.currentIndex = (this.currentIndex+1) % allServices.size();
	        newList.addAll(allServices.subList(this.currentIndex, allServices.size()));
	        newList.addAll(allServices.subList(0,this.currentIndex));
        }

        if (newList.isEmpty() ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0053, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, getClass().getSimpleName()));
        }
        return newList;
    }


	@Override
	public void updateServices(List<ServiceRegistryBinding> localServices,List<ServiceRegistryBinding> remoteServices) {
		this.localServices = new ArrayList(localServices);
		this.remoteServices = new ArrayList(remoteServices);
	    this.localCurrentIndex = 0;
	    this.currentIndex = 0;
		
	}    
}

