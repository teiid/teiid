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

/**
 * Created on May 14, 2002
 *
 * The JDBCResourcePool is used to obtain a JDBC Connection from the
 * resource pool.
 */

import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.impl.ResourcePoolMgrImpl;
import com.metamatrix.common.util.ErrorMessageKeys;

public final class ResourceHelper {



    private static final ResourcePoolMgrImpl mgr = new ResourcePoolMgrImpl();
    private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();

    /**
     * Returns a {@link Resource} from a resource pool based on the
     * specified {@link ResourcePool.RESOURCE_POOL} property.  If
     * the requested pool does not exist, then a new pool will be created based on the given
     * pool name.  However, in order to create the new pool, the pool may expect certain properties
     * to be passed in.  Otherwise, an exception may be thrown when the resource is requested
     * from the pool.
     * @param properties that define the pool to obtain the connection from
     * @param userName is the requestor of the connection
     * @returns Resource containing the physical
     * @throws ResourcePoolException if an error occurs obtaining the connection
     */
    public static Resource getResource(Properties properties, String userName) throws ResourcePoolException {

        // 1st find the descriptor for the given pool name
        return getResource(ResourcePool.JDBC_SHARED_CONNECTION_POOL, userName);

    }

    /**
     * Returns a {@link Resource} from the specified {@link ResourcePool.RESOURCE_POOL} based on the
     * specified <code>poolName</code>.
     * @param poolName is the name of the pool to obtain the resource from
     * @param userName is the requestor of the connection
     * @returns Resource containing the physical
     * @throws ResourcePoolException if an error occurs obtaining the connection
     */
    public static Resource getResource(String poolName, String userName) throws ResourcePoolException {

       ResourceDescriptor descriptor = getDescriptor(poolName);

       return getResourceFromPool(descriptor, userName);

    }



    static ResourceDescriptor getDescriptor(String poolName) throws ResourcePoolException {
        ResourceDescriptor descriptor = null;
        try {

             descriptor = CurrentConfiguration.getResourceDescriptor(ResourcePool.JDBC_SHARED_CONNECTION_POOL);

             if (descriptor == null) {
             	throw new ResourcePoolException(ErrorMessageKeys.POOLING_ERR_0002);
             }

        } catch (Exception e) {
            throw new ResourcePoolException(e, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0003, poolName));
        }
        return descriptor;
    }



   static ResourceDescriptor createDescriptor(ConfigurationID configID, ComponentTypeID resourceTypeID, String poolName, Properties properties) throws ResourcePoolException {
	    ResourceDescriptor descriptor = editor.createResourceDescriptor(configID,resourceTypeID,poolName);
	    descriptor = (ResourceDescriptor) editor.modifyProperties(descriptor, properties, ConfigurationObjectEditor.ADD);
	    return descriptor;
    }

    public static Resource getResourceFromPool(ResourceDescriptor descriptor, String userName) throws ResourcePoolException {
	    return mgr.getResource(descriptor, userName);
    }
}
