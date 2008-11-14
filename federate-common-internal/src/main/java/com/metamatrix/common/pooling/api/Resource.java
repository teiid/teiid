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

import com.metamatrix.common.pooling.api.exception.ResourcePoolException;

/**
* The Resource is used as a wrapper for the physical resource object.  This interface
* dictates the behavior the resource pooling framework requires in order
* to provide, share and manage resources in a pool.  Therefore, any physical resource
* must implement this interface in order to be managed in a resource pool.
*
* The implementor should extend {@link BaseResource BaseResource} to create
* a wrapper class for their specific business object.  This wrapper will be
* created when it is being checked out so that a reference can be kept to
* the {@link ResourceContainer ResourceContainer} from which it came and also
* track the user who currently has it checkedout.
*/

public interface Resource {

	/**
	 * Called by the adapter for the resource to initialize itself and
	 * validate it should be valid resource for use.
	 * @param checkedOutBy is the name of the user for which this resource
	 * instance is being used
	 * @throws ResourcePoolException if not a valid resource
	 */
	void init(String checkedOutBy) throws ResourcePoolException;

    /**
     * Returns the name of the user that has checked out this resource.
     * @return String user name
     */

    String getCheckedOutBy();

    /**
     * This method should be invoked by user to indicate
     * the use of the resource is not longer needed.  This is where the resource
     * will be returned back to the pool for possible reuse.
     * @throws ResourcePoolConnection if there is an error closing the resource
     */
    void closeResource() throws ResourcePoolException ;


    /**
     * isResourceAlive is called to determine if the resource is operating normally. This
     * method is called when the resource is checked in and during shrink processing.
     * A return of <code>false</code> indicates the resource should not be used.
     * If the resource is unusable, it will be shutdown and possibly another
     * instance will take its place.
     * @return boolean true if the resource is operating normally
     */
     boolean isResourceAlive()  throws ResourcePoolException ;


}

