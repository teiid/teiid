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


import java.util.Properties;

import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;

/**
* The ResourceContainer represents one instance of a physical resource in the pool.
* For each physical resource created for the pool, a new container instance
* will be created to hold and manage the physical instance.  Each container instance
* will manage the statistics related to that instance.  This would include
* how many users are currently using this instance and who are they.
*
*/

public interface ResourceContainer {


    /**
     *@link
     * @supplierCardinality 1
     */

    /*#Resource lnkResource;*/
    /**
     *@link aggregationByValue
     */

    /*#ResourceStatistics lnkResourceStatistics;*/
    /**
     * This method is invoked by the pool to set a reference of itself on the resource
     * @param resourcePool is the resource pool from which the resource is managed.
     * @param physicalResourceObject is the physical object to be managed by the container
     * @param maxConcurrentUsers indicates the maximum number of users that can share
     *    the physicalResourceObject
     * @throws ResourcePoolException if an error occurs initializing resource.
     */

    void init(ResourcePool resourcePool, Object physicalResourceObject, int maxConcurrentUsers) throws ResourcePoolException ;

    /**
     * Returns the resource descriptor that describes this resource
     * @return ResourceDescriptor is the descriptor
     */
     ResourceDescriptor getResourceDescriptor();


    /**
     * Returns the properties
     * @return Properties for the resource
     */
     Properties getProperties();


     /**
      * Return <code>true</code> if the physical resource
      * is alive.
      * @return boolean true if the phsical resource is operational
      * @throws ResourcePoolException is an error ocurrs when checking the
      *   state of the physical resource
      */
     boolean isAlive() throws ResourcePoolException ;


     /**
      * Returns <code>true</code> if one or more rescources are
      * available for checkout in the specific container instance.
      * @return boolean true if a resource can be checked out.
      */
     boolean hasAvailableResource();

    /**
     * Returns the resource object request for the userName
     * @return Resource object
     */
    Resource checkOut(String userName) throws ResourcePoolException ;

    /**
     * Called to checkin the 
     * @return boolean <code>true</code> if the resource was checked in
     * @throws ResourcePoolException is an error ocurrs when checking in resource
     */
    boolean checkin(Resource resource, String userName)  throws ResourcePoolException ;


    /**
     * Returns the physical resource object
     * Do not call this method to checkout the resource.
     * @return Object object
     */
    Object getResourceObject();

    /**
     * Returns the statistics for this container
     * @return ResourceStatistics state
     */
    ResourceStatistics getStats();

    /**
     * Called to shutdown the container and cleanup resource references.
     * This should not call close on the <code>Resource</code>
     */
    void shutDown() throws ResourcePoolException ;


}
