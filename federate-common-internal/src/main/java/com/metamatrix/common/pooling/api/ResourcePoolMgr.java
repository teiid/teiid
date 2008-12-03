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

import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;


/**
 * <p>
 * The ResourcePoolMgr is the center point for managing and sharing a common
 * object type among common users.  The benefit of managing an object as a
 * resource is so that common users can share the object without the overhead
 * of reoccurring object creation, desctruction and cleanup.  To utilize the
 * resource pooling feature the following needs to be done:
 * <ol>
 *    <li><ul>Create {@link ResourceType}:</ul> the resource type categorically
 *        identifies the resource (e.g., JDBCConnectionResourceType).
 *        The resource type can provide defaults that will be applied to all the specific
 *        {@link ResourceDescriptor} implementations that are of that type.
 *        Therefore, there is a one-to-many relationship between the
 *        resource type and descriptor, respectfully.</li>
 *    <li><ul>Create {@link ResourceDescriptor}:/<ul> the descriptor describes the
 *        the resource and how it should be managed in the pool.  The descriptor
 *        also represents the identifier the ResourcePoolMgr uses to identify
 *        each pool.  The combination of the descriptor's name and
 *        {@link ResourceType} provides the unique identifier.</li>
 *    <li><ul>Create {@link Resource}:</ul> the resource is a wrapper class that will
 *        expose the necessary methods of the physical resource object.
 *        <b>The resource should be implemented by extending {@link
 *        com.metamatrix.common.pooling.impl.BaseResource BaseResource}.</b></li>
 *    <li><ul>Create {@link ResourceAdapter}:</ul> the adpater will be used by
 *        the {@link ResourcePool} to create new resource object instances, as
 *        well as, close existing instances. </li>
 * </ol>
 * </p>
 * <p>
 *  Once the appropriate set of classes have been implemented for a resource, the
 *  following are the steps for using a resource:
 *  <li>Instantiate the specific {@link ResourceDescriptor}
 *      and call {@link ResourcePoolMgr#getResource} to obtain a resource instance from the pool. </li>
 *  <li><b>When the user is finished with the resource, {@link Resource#closeResource} method
 *      must be called in order to release the resource back to the pool for reuse.</b></li>
 * </p>
 *
 *
 */
public interface ResourcePoolMgr {

	/**
	 * Call to get a resource from the pool defined by descriptor.
	 * @param descriptor that describes the resource to obtain
	 * @param userName of the one requesting the resource
	 * @return Resource that is defined by the descriptor
	 * @exception ResourcePoolException is thrown if an error occurs.
	 *    Check for type {@link ResourceWaitTimeOutException}
	 */
	Resource getResource(ResourceDescriptor descriptor, String userName) throws ResourcePoolException ;
	
	
	/**
	 * Call to get id's for all the current resource pools.
	 * @return Collection of type ResourceDescriptorID for all the resource pools.
	 * @exception ResourcePoolException is thrown if an error occurs.
	 *    Check for type {@link ResourceWaitTimeOutException}
	 * {@see com.metamatrix.common.config.api.ResourceDescriptorID}
	 */
	Collection getAllResourceDescriptorIDs() throws ResourcePoolException;
	
	/**
	 * Call to get all the resource descriptors for the current pools.
	 * @return Collection of type ResourceDescriptor for all the resource pools.
	 * @exception ResourcePoolException is thrown if an error occurs.
	 * {@see com.metamatrix.common.config.api.ResourceDescriptor} 
	 */
	Collection getAllResourceDescriptors() throws ResourcePoolException;
	
	
	/**
	 * Call to get the resource descriptor for the specified id
	 * @param descriptorID is the id that identifies the resource descriptor
	 * @return ResourceDescriptor identified by the id
	 * @exception ResourcePoolException is thrown if an error occurs.
	 */
	ResourceDescriptor getResourceDescriptor(ResourceDescriptorID descriptorID) throws ResourcePoolException;
	
	
	/**
	 * Call to update the pool management parameters based on the resource descriptor.
	 * This could include changing the size of the pool.
	 * @param resourceDescriptorID identifies the pool to update
	 * @param properties are the changes to be applied
	 * @exception ResourcePoolException is thrown if an error occurs.  
	 */
	void updateResourcePool(final ResourceDescriptorID resourceDescriptorID, final Properties properties) throws ResourcePoolException;
	
	/**
	 * Returns all the {@link ResourcePoolStatistics ResourcePoolStatistics} currently
	 * active.  An empty collection will be returned if no resource pools are active.
	 * @return Collection of type ResourcePoolStatistics
	 * @exception ResourcePoolException is thrown if an error occurs. 
	 */
	Collection getResourcePoolStatistics() throws ResourcePoolException;
	
	/**
	 * Returns all the {@link ResourceStatistics ResourceStatistics} for the pool.
	 * An empty collection will be returned if no resources exist in the pool.
	 * @return Collection of type ResourceStatistics
	 * @exception ResourcePoolException is thrown if an error occurs.
	 */
	Collection getResourcesStatisticsForPool(ResourceDescriptorID descriptorID) throws ResourcePoolException;
	
	/**
	 * Returns the statistics for a specific resource descriptor id.  If
	 * the {@link ResourcePool ResourcePool} does not exist for the
	 * <code>descriptorID</code>, then a null will be returned.
	 * @return ResourcePoolStatistics for a specific resource pool; null if
	 *	    resource pool does not exit.
	 * @exception ResourcePoolException is thrown if an error occurs. 
	 */
	ResourcePoolStatistics getResourcePoolStatistics(ResourceDescriptorID descriptorID) throws ResourcePoolException;
	
	/**
	 * Call to shutdown all resources in the specific pool identified by the descriptor ID.
	 * @param descriptorID for the pool to be shutdown
	 * @exception ResourcePoolException is thrown if an error occurs.
	 */
	void shutDown(ResourceDescriptorID descriptorID) throws ResourcePoolException;
	
	/**
	 * Call to shutdown all resource pools and all the resources within those pools.
	 */
	void shutDown() throws ResourcePoolException;
}