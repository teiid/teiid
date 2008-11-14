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

package com.metamatrix.common.pooling.impl;

import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.*;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;

/**
* The ResourceContainer is what is used by the pool
* to contain and manage the number of {@link Resource Resources}
* that are made available.
*/

public class BasicResourceContainer implements ResourceContainer {

//    private long id;
    private Map checkedOutResources;

/**
 *  the physical resource object
 */
    private Object resourceObject = null;


/**
 * The holdResource is kept so that the physical resource can be
 * questioned as to its state.  This is due to when no one has this
 * resource checkout, but need to know the state.
 */
    private BaseResource holdResource;

/**
 * @label belongs to
 */
    private ResourcePool pool = null;
/**
 * @label tracks current users
 */
    private ResourceStatistics stats;

    private int maxUsers = 1;
    private boolean shutdown = false;

    protected static final String LOG_CONTEXT = LogCommonConstants.CTX_POOLING;
    private static final String HOLD_FOR_CONTAINER = "HoldForContainer"; //$NON-NLS-1$

    public BasicResourceContainer(long containerID) {
//        this.id = containerID;
        this.stats = new ResourceStatistics();
    }

    public synchronized void init(ResourcePool resourcePool, Object physicalResourceObject, int maxConcurrentUsers) throws ResourcePoolException {
        this.pool = resourcePool;
        this.resourceObject = physicalResourceObject;
        this.maxUsers = maxConcurrentUsers;
        this.checkedOutResources = new HashMap(this.maxUsers);

        this.holdResource = (BaseResource) pool.getResourceAdapter().createResource(physicalResourceObject);
        this.holdResource.init(this, HOLD_FOR_CONTAINER);


        LogManager.logTrace(LOG_CONTEXT, "ResourceContainer created for resource " + resourcePool.getResourceDescriptor().getName()); //$NON-NLS-1$

    }

    /**
     * Returns the resource descriptor that describes this resource
     * @return ResourceDescriptor is the descriptor
     */
     public ResourceDescriptor getResourceDescriptor() {
        return pool.getResourceDescriptor();
    }


    /**
     * Returns the resource properties
     * @return Properties for the resource
     */
     public Properties getProperties() {
        return pool.getResourceDescriptor().getProperties();
    }

    public boolean hasAvailableResource() {
        return (getStats().getConcurrentUserCount() < this.maxUsers);
    }

    public synchronized boolean isAlive()  throws ResourcePoolException {
        if (pool == null || resourceObject == null) {
            return false;
        }
        return this.holdResource.isResourceAlive();
    }



    /**
     * Returns the resource object request for the userName
     * @return Resource object
     */
    public synchronized Resource checkOut(String userName)  throws ResourcePoolException {

    // create the Resource that will manage the resourceObject
        BaseResource resource = (BaseResource) pool.getResourceAdapter().createResource(resourceObject);

        resource.init(this, userName);

        checkedOutResources.put(resource, userName);

        this.getStats().addConcurrentUser(userName);

        LogManager.logTrace(LOG_CONTEXT, "ResourceContainer checkout resource for user " + userName); //$NON-NLS-1$


        return resource;

    }

    /**
     * Returns the resource object request for the userName
     * @return Resource object
     */
    public synchronized boolean checkin(Resource resource, String userName)  throws ResourcePoolException  {

        if (shutdown) {
            return true;
        }
        if (!checkedOutResources.containsKey(resource)) {
            
            throw new ResourcePoolException(CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0013, new Object[] {userName, pool.getResourceDescriptorID()}));
        }

        checkedOutResources.remove(resource);

        ResourceStatistics stats = getStats();

        stats.removeConcurrentUser(userName);

        pool.checkIn(this, userName);

        LogManager.logTrace(LOG_CONTEXT, "ResourceContainer checkin resource for user " + userName); //$NON-NLS-1$


        return true;

    }

    public Object getResourceObject() {
        return this.resourceObject;
    }


    /**
     * This method is invoked by the pool when the resource will have all
     * its resources cleaned up.  This resource will no longer be available for use.
     * After calling this method, {@link #isAlive} should return <code>false</code>.
     */
    public final synchronized void shutDown()  throws ResourcePoolException  {
//        LogManager.logTrace(LOG_CONTEXT, "ResourceContainer " + resourcePool.getResourceDescriptor().getName() + " is being shut down");

        try {
            shutdown = true;
            // close only the physical object
            pool.getResourceAdapter().closePhyicalResourceObject(holdResource);

        } catch (Exception e) {
            // log the exception and keep closing
             throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0014, pool.getResourceDescriptor().getName()));
        } finally {
            pool = null;
            stats = null;
            resourceObject = null;
            holdResource = null;
            checkedOutResources.clear();

        }

    }


    /**
     * Returns the statistics for this resource
     * @return ResourceStatistics state
     */
    public ResourceStatistics getStats() {
        return this.stats;
    }


    protected ResourcePool getResourcePool() {
        return this.pool;
    }


}
