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

import java.util.Collection;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;


/**
 * The ResourcePool is the manager for a specific set of resource instances.  
 * A new ResourcePool is created for each different {@link ResourceDescriptor descriptor}
 * that is compared using the {@link ResourceDescriptor.equals equals} method.
 * Based on the minimum resource size, the pool will be initialized with that
 * many resource instances.  The maximum resource size controls the number
 * of resource instances that will be available at one time. 
 */
public interface ResourcePool  {
    
    /**
     * The environment property that indicates the name of the pool.
     */
    public static final String RESOURCE_POOL = "metamatrix.common.pooling.resource.name"; //$NON-NLS-1$

    //**********************************************
    // The following are the well know resource pools 
    //***********************************************/
   
    /**
     * The well-known pool that is commonly used for the majority of the internal
     * resources/services (i.e., DirectoryService, ConfigurationService, etc.).
     */
    public static final String JDBC_SHARED_CONNECTION_POOL = "JDBC Shared Connection Pool"; //$NON-NLS-1$



// defaults are set to ONE as so to be on the conversative side
    public interface Defaults {
        static final String DEFAULT_MINIMUM_RESOURCE_SIZE = "1"; //$NON-NLS-1$
        static final String DEFAULT_MAXIMUM_RESOURCE_SIZE = "5"; //$NON-NLS-1$
        static final String DEFAULT_NUM_OF_CONCURRENT_USERS = "1"; //$NON-NLS-1$
        static final String DEFAULT_LIVE_AND_UNUSED_TIME = "600000";  // in milliseconds, or 10 minutes //$NON-NLS-1$
        static final String DEFAULT_SHRINK_PERIOD = "300000";  // in milliseconds, or 5 minutes //$NON-NLS-1$
        static final String DEFAULT_ALLOW_SHRINKING = "true";  // in milliseconds, or 5 minutes //$NON-NLS-1$
        static final String DEFAULT_SHRINK_INCREMENT = "0";  // no limit //$NON-NLS-1$
        static final String DEFAULT_WAIT_FOR_RESOURCE_TIME = "30000";  // in milliseconds, or 30 seconds //$NON-NLS-1$
        static final String DEFAULT_EXTEND_MODE = "false";  // no expaned mode by default //$NON-NLS-1$
        static final String DEFAULT_EXTEND_PERCENT = "1.000";  // 100 percent doubles the size of max pool size //$NON-NLS-1$
        static final String UNLIMITED_NUM_OF_CONCURRENT_USERS = "-1"; //$NON-NLS-1$
    }


    /**
     *@link dependency
     * @label uses
     */

    /*#ResourcePoolInfo lnkPoolInfo;*/
    /**
     *@link dependency
     * @clientRole manages
     */

    /*#Resource lnkResource;*/
    /**
     *@link aggregationByValue
     * @supplierCardinality 1
     */

    /*#ResourcePoolStatistics lnkResourcePoolStatistics;*/
    /**
     *@link aggregationByValue
     * @label uses
     */

    /*#ResourceAdapter lnkResourceAdapter;*/
    /**
     *@link aggregationByValue
     */

    /*#ResourceContainer lnkResourceContainer;*/
/**
 * Call to initialize the pool based on the descriptor.  This is where the initial resources, 
 * which represent the minimum pool size, will be created and loaded into the pool.
 * @param descriptor which determines the type of resource and pool size parameters
 * @exception ResourcePoolException is thrown if an error ocurrs during initialization
 */
    void init(ResourceDescriptor descriptor) throws ResourcePoolException;


/**
 * Returns the resource descriptor ID for this pool that to identify the pool.
 * This id is used by the pool manager as another means of finding a
 * specific resource pool.
 * @return ResourceDescriptorID is the resource pool id
 */
    ResourceDescriptorID getResourceDescriptorID();
    
   /**
    * Returns the {@link com.metamatrix.common.config.ComponentTypeID} that
    * identifies the type of pool these statistics represent.
    * @return ComponentTypeID is the type id
    */
   ComponentTypeID getComponentTypeID();    


/**
 * Returns the current size of the resource pool.
 * @param userName of who is requesting the resource
 * @return int the number of {@link ResourceContainer ResourceContainers} in the pool
 */
    int getResourcePoolSize();
/**
 * Returns a resource from the pool.
 * @param userName of who is requesting the resource
 * @return Resource that is from the pool
 * @exception ResourcePoolException is thrown if an error occurs
 */
    Resource checkOut(String userName) throws ResourcePoolException;


/**
 * Called by the resource container to checkin itself to the pool.
 * @param resourceContainer represents the physical resource instance being removed from the pool
 * @param userName of who has the resource checked out
 * @exception ResourcePoolException is thrown if an error occurs
*/
    void checkIn(ResourceContainer resourceContainer, String userName) throws ResourcePoolException;

/**
 * Returns the {@link ResourceDescriptor descriptor} from which resources in the were created.
 * @return ResourceDescriptor used to create the resources in the pool 
 */
    ResourceDescriptor getResourceDescriptor();
/**
 * Returns the monitor for this pool.
 * @return ResourcePoolMonitor that monitors this pool
 */
   ResourcePoolStatistics getResourcePoolStatistics();

/**
 * Returns a collection of statistics for resources in this pool.
 * @return Collection of ResourceStatistics objects.
 */
   Collection getPoolResourceStatistics();

/**
 * Returns the resource adapter used to create the phyical resource objects.
 * @return ResourceAdapter used to create resource objects.
 */
   ResourceAdapter getResourceAdapter();
/**
 * Call to shutDown all the resources in the pool.  
 * Each resource will have the method {@link Resource.shutDown} called. 
 */
    void shutDown();
/**
 * Call to update the pool management parameters based on the resource descriptor.
 * This could include changing the size of the pool.
 * @param properties are the changes to be applied
 * @exception ResourcePoolException is thrown if an error ocurrs
 */
    void update(Properties properties) throws ResourcePoolException;

}

