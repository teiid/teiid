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

import java.util.List;

import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.exception.ServiceNotFoundException;

/**
 * Represents a selection policy for services of a given type.<br>
 */
public interface ServiceSelectionPolicy {

    /**
     * Get the next service instance from the given service selection policy.
     * @return The <code>ServiceRegistryBinding</code> for the next instance after
     * being operated on by the selection policy.
     * @see com.metamatrix.common.service.ServiceInterface
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public ServiceRegistryBinding getNextInstance() throws ServiceNotFoundException;

    /**
     * Get list of instances from the given service selection policy.
     * @return The List of <code>ServiceRegistryBinding</code> objects for the instances after
     * being operated on by the selection policy.
     * @see com.metamatrix.common.service.ServiceInterface
     * @throws ServiceNotFoundException if the policy has no more services to
     * hand out.
     */
    public List<ServiceRegistryBinding> getInstances() throws ServiceNotFoundException;
    
    
    void updateServices(List<ServiceRegistryBinding> localServices, List<ServiceRegistryBinding> remoteServices);

}





