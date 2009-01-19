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

package com.metamatrix.common.config;

/**
 * Created on May 14, 2002
 *
 * The JDBCResourcePool is used to obtain a JDBC Connection from the
 * resource pool.
 */

import java.sql.Connection;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.pooling.api.*;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.jdbc.JDBCConnectionResource;
import com.metamatrix.common.util.ErrorMessageKeys;

public final class JDBCConnectionPoolHelper {



    private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();

    private static final String DEFAULT_ADAPTER = "com.metamatrix.common.pooling.jdbc.JDBCConnectionResourceAdapter"; //$NON-NLS-1$
    private static final String DEFAULT_POOL = "com.metamatrix.common.pooling.impl.BasicResourcePool"; //$NON-NLS-1$

    /**
     * Returns a connection based on the {@link ResourcePool.RESOURCE_POOL} property.  If
     * the property does not exist, then a connection will be returned from the
     * default pool {@link ResourceDescriptor.JDBC_SHARED_CONNECTION} descriptor.
     * <br>
     * If the pool property name does not exist as an existing resource descriptor
     * in the configuration, then a descriptor will tried to be created for
     * the pool using the connection properties contained in the <code>properties</code>
     * argument.  If a new descriptor cannot be created, then an exception
     * will be thrown.
     * @param properties that define the pool to obtain the connection from
     * @param userName is the requestor of the connection
     * @throws ResourcePoolException if an error occurs obtaining the connection
     */
    public static Connection getConnection(Properties properties, String userName) throws ResourcePoolException {

        String poolName=ResourcePool.JDBC_SHARED_CONNECTION_POOL;


        // 1st find the descriptor for the given pool name
        ResourceDescriptor descriptor = getDescriptor(poolName);


        // if the descriptor doesnt exist (it may not exist if current configuration
        //  is not pointed to a database) then create a descriptor
        if (descriptor == null) {
                descriptor = createDescriptor(Configuration.NEXT_STARTUP_ID, poolName, properties);
        }


        if (descriptor != null) {
            return getConnection(descriptor, userName);
        }

        throw new ResourcePoolException(ErrorMessageKeys.CONFIG_ERR_0027, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0027, new Object[]
        		{poolName, userName}));


    }

    private static Connection getConnection(ResourceDescriptor descriptor, String userName) throws ResourcePoolException {

        try {


            Resource resource = ResourceHelper.getResourceFromPool(descriptor, userName);

            if (resource instanceof Connection) {
                    return (Connection) resource;
            }
         throw new ResourcePoolException(ErrorMessageKeys.CONFIG_ERR_0029, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0029,
             	new Object[] {descriptor.getName(), userName, resource.getClass().getName()} ) );

        } catch (Exception e) {
            throw new ResourcePoolException(e, ErrorMessageKeys.CONFIG_ERR_0028, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0028,
            	new Object[] {descriptor.getName(), userName} ) );
        }


    }

    private static ResourceDescriptor getDescriptor(String poolName) throws ResourcePoolException {
        ResourceDescriptor descriptor = null;
        try {

             descriptor = CurrentConfiguration.getResourceDescriptor(poolName);
        } catch (Exception e) {
            throw new ResourcePoolException(e, ErrorMessageKeys.CONFIG_ERR_0030, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0030, poolName ));
        }


        return descriptor;

    }


   public static ResourceDescriptor createDescriptor(ConfigurationID configID, String poolName, Properties properties) throws ResourcePoolException {

            String driver = properties.getProperty(JDBCConnectionResource.DRIVER);

            if (driver == null || driver.length() == 0) {
                return null;
            }


        // create the descriptor used to get the resource
            ResourceDescriptor descriptor = editor.createResourceDescriptor(configID,
                                            SharedResource.JDBC_COMPONENT_TYPE_ID,
                                            poolName);

            descriptor = (ResourceDescriptor) editor.modifyProperties(descriptor, properties, ConfigurationObjectEditor.ADD);

        // set the pool name to the name of the descriptor
			Properties def = new Properties();
          	def.setProperty(ResourcePool.RESOURCE_POOL, ResourcePool.JDBC_SHARED_CONNECTION_POOL);

            if (!properties.containsKey(ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME)) {
                def.setProperty(ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME, DEFAULT_ADAPTER);
            }
            if (!properties.containsKey(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME)) {
                def.setProperty(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME, DEFAULT_POOL);
            }

			if (!def.isEmpty()) {
	            descriptor = (ResourceDescriptor) editor.modifyProperties(descriptor, def, ConfigurationObjectEditor.ADD);
			}
            return descriptor;

    }

}
