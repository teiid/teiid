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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

public class RandomSelectionPolicy implements ServiceSelectionPolicy  {

    // Does this policy prefer local services?
    private boolean preferLocal = false;

    // State necessary to implement random
    private Random randomizer;
    
    List<ServiceRegistryBinding> localServices;
	List<ServiceRegistryBinding> remoteServices;    

    /**
     * ctor
     */
    public RandomSelectionPolicy(boolean preferLocal) {
        this.randomizer = new Random();
        this.preferLocal = preferLocal;
    }

    @Override
    public synchronized List<ServiceRegistryBinding> getInstances() throws ServiceNotFoundException {
        List allServices = new ArrayList(this.localServices);
        allServices.addAll(this.remoteServices);
        Collections.shuffle(allServices);
        if (allServices.isEmpty() ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0053, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, getClass().getSimpleName()));
        }
        return allServices;
    }
    
    @Override
    public synchronized ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException {
        ServiceRegistryBinding serviceBinding = null;

        List allServices = this.remoteServices;
        if (this.preferLocal) {
	        if ( localServices != null && ! localServices.isEmpty() ) {
		        // Get random value in range of list size
	    	    int index = randomizer.nextInt(localServices.size());
	            serviceBinding = (ServiceRegistryBinding) localServices.get(index);
		    }
        } else {
        	allServices = new ArrayList(localServices);
        	allServices.addAll(remoteServices);
        }

        if ( serviceBinding == null ) {
            if ( allServices != null && ! allServices.isEmpty() )  {
	            // Get random value in range of list size
    	        int index = randomizer.nextInt(allServices.size());
                serviceBinding = (ServiceRegistryBinding) allServices.get(index);
	        }
        }

        if ( serviceBinding == null ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0053, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, getClass().getSimpleName()));
        }

        return serviceBinding;
    }


	@Override
	public void updateServices(List<ServiceRegistryBinding> localServices, List<ServiceRegistryBinding> remoteServices) {
		this.localServices = new ArrayList(localServices);
		this.remoteServices = new ArrayList(remoteServices);
	}    
}



