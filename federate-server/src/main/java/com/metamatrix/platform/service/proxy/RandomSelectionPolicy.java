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

package com.metamatrix.platform.service.proxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

public class RandomSelectionPolicy extends ServiceSelectionPolicy implements AllServiceSelectionPolicy {

    // The name of the policy.
    private static final String NAME = ServiceSelectionPolicy.RANDOM_SELECTION_POLICY_NAME;
    // Does this policy prefer local services?
    private static final boolean PREFERS_LOCAL = false;

    // State necessary to implement random
    private Random randomizer;

    private List<ServiceRegistryBinding> allServices;

    /**
     * ctor
     */
    RandomSelectionPolicy() {
        super();
        randomizer = new Random();
    }

    /**
     * Update the service list that this selection policy uses.
     * @param allServices All the service instances of the type handled by this
     * selection policy that are known to the system.
     */
    public synchronized void updateServices(List allServices) {
        this.allServices = allServices;
    }

    /**
     * Get the name of the policy - useful for logging/debugging.
     * @return This policy's type name.
     * @see com.metamatrix.platform.service.proxy.ServiceSelectionPolicy
     */
    public String getServiceSelectionPolicyName() {
        return NAME;
    }

    /**
     * Return whether or not the policy preference is local.
     * @return the local preference of this policy.
     */
    public boolean prefersLocal() {
        return PREFERS_LOCAL;
    }

    /**
     * Get the next service instance from the given service selection policy.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public synchronized ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException {

        ServiceRegistryBinding serviceBinding = null;

        if ( allServices != null && ! allServices.isEmpty() ) {
	        // Get random value in range of list size
    	    int index = randomizer.nextInt(allServices.size());
            serviceBinding = (ServiceRegistryBinding) allServices.get(index);
	    }

        if ( serviceBinding == null ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0052, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, NAME));
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
    public List getInstances() throws ServiceNotFoundException {

        List newList = null;

        if ( allServices != null && ! allServices.isEmpty() ) {
	        newList = new ArrayList(allServices);
    	    Collections.shuffle(newList);
        }

        if ( newList == null || newList.isEmpty() ) {
            throw new ServiceNotFoundException(ServiceMessages.SERVICE_0052, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0053, NAME));
        }
        return newList;
    }
}



