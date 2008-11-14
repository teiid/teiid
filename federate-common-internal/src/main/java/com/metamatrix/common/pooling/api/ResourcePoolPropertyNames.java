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

import java.util.ArrayList;
import java.util.Collection;

public final class ResourcePoolPropertyNames {
    
        /**
    * The environment property name that defines the {@link ResourceAdapter ResourceAdapter}
    * class that will be instantiated to create resource object instances
    */
   public static final String RESOURCE_ADAPTER_CLASS_NAME = "pooling.resource.adapter.class.name"; //$NON-NLS-1$


    /**
    * The environment property name that defines the {@link ResourcePool ResourcePool}
    * class that will be instantiated to manage a specific resource pool.
    */
    public static final String RESOURCE_POOL_CLASS_NAME = "pooling.resource.pool.class.name"; //$NON-NLS-1$

    /**
     * The environment property name for the minimum number of resource instances (i.e., number of objects)
     * that will <b> always be active </b> in the pool.
     * <p>
     * This is an optional property and the value must be the string form of an integer.
     * The default value is "1" .
     */
    public static final String MINIMUM_RESOURCE_POOL_SIZE = "pooling.resource.pool.minimum.size"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of resource instances (i.e., number of objects)
     * that will <b> be active at one time </b> in the pool.
     * <p>
     * This is an optional property and the value must be the string form of an integer.
     * The default value is "5" .
     */
    public static final String MAXIMUM_RESOURCE_POOL_SIZE = "pooling.resource.pool.maximum.size"; //$NON-NLS-1$

    /**
     * The environment property name for the number of users that can share the
     * same resource at the same time (i.e., concurrent users).  This property,
     * in most cases, should be user specified.  This is because
     * each resource, unless it cannot be shared, may have it's
     * limitations as to the number of concurrent users (e.g., JDBC Connections)
     * and could impact performance.
     * <p>
     * This is an optional property and the value must be the string form of an integer.
     * The default value is "1" .
     */
    public static final String NUM_OF_CONCURRENT_USERS = "pooling.resource.pool.concurrent.users"; //$NON-NLS-1$

    /**
     * The environment property name for the intervals between when the clean-up thread
     * tries to shrink the pool.
     * <p>
     * This is an optional property and it corresponds to the number of milliseconds between checks.
     * The value must be the string form of an integer and the default value is "300000" or 5 minutes.
     */
    public static final String SHRINK_PERIOD = "pooling.resource.pool.shrink.period"; //$NON-NLS-1$

    /**
     * The environment property name that indicates if shrinking of the resource pool is allowed.
     * {@see #SHRINK_PERIOD}
     * <p>
     * This is an optional property and it corresponds to either <code>true</code> or  <code>false</code>.
     * The value must be the string form and the default value is "true".
     */
    public static final String ALLOW_SHRINKING = "pooling.resource.pool.allow.shrinking"; //$NON-NLS-1$

    /**
     * The environment property name that indicates the maximum number of resources in the
     * pool that will be removed at one time.  This is only applicable if shrinking is allowed.
     * {@see #ALLOW_SHRINKING}
     * {@see #SHRINK_PERIOD}
     * <p>
     * This is an optional property and and the value must be the string form of an integer.
     * The default value is "0", which indicates no limit to the number of resources
     * that can be removed in order to return to the {@link #MINIMUM_RESOURCE_POOL_SIZE minimum}
     * pool size.
     */
    public static final String SHRINK_INCREMENT = "pooling.resource.pool.shrink.increment"; //$NON-NLS-1$


    /**
     * The environment property name for the amount of time that will have to pass where a
     * resource has not been used and therefore should be considered for removal from the pool.
     * The property might be set higher in the case where the resource is one
     * that will be asked for over-and-over so that the resouce is not removed and
     * recreated as often.  However, in the case where the resource is not asked
     * for as often or rarely, then a higher setting would be appropriate so that
     * unneeded resources are not held in memory.
     * <p>
     * To assist in determining what the setting should be, try a multiple of
     * {@link #SHRINK_PERIOD SHRINK_PERIOD}.  If the resource pool is being
     * checked for shrinkage every 5 minutes, then every other time (i.e., 10 minutes)
     * the resource will be checked for being alive and unused.
     * <p>
     * This is an optional property and it corresponds to the number of milliseconds between checks.
     * The value must be the string form of an integer and the default value is "600000" or 10 minutes.
     */
    public static final String LIVE_AND_UNUSED_TIME = "pooling.resource.pool.liveandused.time"; //$NON-NLS-1$

    /**
     * The environment property name for how long a request for a resource will
     * wait for one to become available.
     * <p>
     * If the {@link #NUM_OF_CONCURRENT_USERS NUM_OF_CONCURRENT_USERS} cannot be increased,
     * nor can the {@link MAXIMUM_RESOURCE_POOL_SIZE MAXIMUM_RESOURCE_POOL_SIZE} be increased,
     * then there would be a possible need for increasing the wait time.  But try
     * tuning the other properties first, otherwise the wait time for the user
     * will be increased.
     * <p>
     * This is an optional property and it corresponds to the number of milliseconds between checks.
     * The value must be the string form of an integer and the default value is "30000" or 30 seconds.
     */
    public static final String WAIT_TIME_FOR_RESOURCE= "pooling.resource.pool.wait.time"; //$NON-NLS-1$

    /**
     * The environment property name for enabling the resource pool size to grow beyond
     * the {@link #MAXIMUM_RESOURCE_POOL_SIZE MAXIMUM_RESOURCE_POOL_SIZE}.  If the property
     * is set to <code>true></code>, this will allow the resource pool to grow
     * upto the {@link #EXTEND_MAXIMUM_POOL_SIZE EXTEND_MAXIMUM_POOL_SIZE}.  This feature is beneficial
     * for server side pools so that they can continue operating without crashing during
     * abnormal peak times.
     * <p>
     * This is an optional property and it corresponds to either <code>true</code> or <code>false</code>.
     * The value must be the string form and the default value is "false".
     */
    public static final String EXTEND_MAXIMUM_POOL_SIZE_MODE = "pooling.resource.extend.maximum.pool.size.mode"; //$NON-NLS-1$

    /**
     * The environment property name for the percentage of growth the resource pool should be allowed
     * to grow above the {@link MAXIMUM_RESOURCE_POOL_SIZE MAXIMUM_RESOURCE_POOL_SIZE}.
     * <p>
     * This is an optional property and the value must be the string form of an double.
     * There default value for this property is "1.000" such that the pool will
     * grow %100 percent in size above the maximum pool size.
     */
    public static final String EXTEND_MAXIMUM_POOL_SIZE_PERCENT = "pooling.resource.extend.maximum.pool.size.percent"; //$NON-NLS-1$


    private static Collection PROPERTY_NAMES;

    static {
            PROPERTY_NAMES = new ArrayList(12);
            PROPERTY_NAMES.add(RESOURCE_ADAPTER_CLASS_NAME);
            PROPERTY_NAMES.add(RESOURCE_POOL_CLASS_NAME);
            PROPERTY_NAMES.add(MINIMUM_RESOURCE_POOL_SIZE);
            PROPERTY_NAMES.add(MAXIMUM_RESOURCE_POOL_SIZE);
       //     PROPERTY_NAMES.add(NUM_OF_CONCURRENT_USERS);
            PROPERTY_NAMES.add(SHRINK_PERIOD);
            PROPERTY_NAMES.add(ALLOW_SHRINKING);
            PROPERTY_NAMES.add(SHRINK_INCREMENT);
            PROPERTY_NAMES.add(LIVE_AND_UNUSED_TIME);
            PROPERTY_NAMES.add(WAIT_TIME_FOR_RESOURCE);
            PROPERTY_NAMES.add(EXTEND_MAXIMUM_POOL_SIZE_MODE);
            PROPERTY_NAMES.add(EXTEND_MAXIMUM_POOL_SIZE_PERCENT);                          
        
    }
    
    public static Collection getNames() {
            return PROPERTY_NAMES;
    }

}
