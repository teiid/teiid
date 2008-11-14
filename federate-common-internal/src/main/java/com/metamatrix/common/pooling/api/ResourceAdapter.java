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

package com.metamatrix.common.pooling.api;

import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;


/**
* The ResourceAdapter is responsible for communicating to the physical
* resource.  This would include creating a new physical resource
* instance or closing a resource.
*/

 public interface ResourceAdapter {

    /**
     *@link dependency
     * @clientRole uses to create physical object
     */

    /*#ResourceDescriptor lnkResourceDescriptor;*/
    /**
     *@link dependency
     * @clientRole create
     */


/**
 *  Creates the {@link Resource Resource} specific wrapper that will contain
 *  the physicalResource.
 *  @param physicalResource that will be contained in the wrapper
 *  @throws ResourcePoolException if an error occurs wrapping the physical resource
 *
 */
  Resource createResource(Object physicalResource) throws ResourcePoolException;


/**
 * Creates a specific resource based on the descriptor.
 * @return Object created by the adapter
 * @throws ResourcePoolException if an error occurs creating the resource
 */
    Object createPhysicalResourceObject(ResourceDescriptor descriptor) throws ResourcePoolException ;

/**
 * Called to have the adpater have the resource object close itself.
 * @throws ResourcePoolException if an error occurs closing resource object.
 */
    void closePhyicalResourceObject(Resource resource) throws ResourcePoolException ;

}

