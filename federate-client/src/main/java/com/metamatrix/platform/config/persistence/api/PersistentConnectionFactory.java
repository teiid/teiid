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

package com.metamatrix.platform.config.persistence.api;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.pooling.jdbc.JDBCConnectionResource;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public abstract class PersistentConnectionFactory {

	public static final String PERSISTENT_FACTORY_NAME = "metamatrix.config.persistent.factory"; //$NON-NLS-1$


	public static final String FILE_FACTORY_NAME = "com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnectionFactory"; //$NON-NLS-1$
	public static final String JDBC_FACTORY_NAME = "com.metamatrix.platform.config.persistence.impl.jdbc.JDBCPersistentConnectionFactory"; //$NON-NLS-1$

	private Properties properties;

	public PersistentConnectionFactory(Properties factoryProperties) {
		this.properties = factoryProperties;
	}

	public Properties getProperties() {
		return properties;
	}
    /**
     * createPersistentConnectionFactory is used for bootstrapping the system.
     * The connection is normally only used for starting the system, then
     * then {@link #createPersistentConnectionFactory} is used.   
     * @param props
     * @return
     * @throws ConfigurationException
     */
    public static final PersistentConnectionFactory createPersistentConnectionFactory(Properties props) throws ConfigurationException {

        Collection args = new ArrayList(1);

        Properties properties = PropertiesUtils.clone(props, false);
        args.add(properties);
        PersistentConnectionFactory factory = null;
        String factoryName = properties.getProperty(PERSISTENT_FACTORY_NAME)    ;

//      System.out.println("Repository Persistence Factory: " + factoryName);

        if (factoryName == null || factoryName.trim().length() == 0) {
            // if no factory name, then check if this a file connection
            if (isFileFactory(properties)) {

                factory = (PersistentConnectionFactory) create(FILE_FACTORY_NAME, args);

                return factory;
            }

            if (isJDBCFactory(properties)) {
                factory = (PersistentConnectionFactory) create(JDBC_FACTORY_NAME, args);
                return factory;
            }

            throw new ConfigurationException(ConfigMessages.CONFIG_0009, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0009, PERSISTENT_FACTORY_NAME));

        }
        try {
            factory = (PersistentConnectionFactory) ReflectionHelper.create(factoryName, args, Thread.currentThread().getContextClassLoader()); 
            //create(factoryName, args);
        } catch (MetaMatrixCoreException err) {
            throw new ConfigurationException(err, ConfigMessages.CONFIG_0013, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0013, factoryName));
            
        }

        return factory;

    
    
    }
    /**
     * createPersistentResourceConnectionFactory is used  after a system
     * has been boostrapped.  This is so the resourcepooling connections
     * can be used incase the database goes down.
     * @param props
     * @return
     * @throws ConfigurationException
     */
	public static final PersistentConnectionFactory createPersistentResourceConnectionFactory(Properties props) throws ConfigurationException {
		Collection args = new ArrayList(1);

		Properties properties = PropertiesUtils.clone(props, false);
		args.add(properties);
		PersistentConnectionFactory factory = null;
		String factoryName = properties.getProperty(PERSISTENT_FACTORY_NAME)	;

//		System.out.println("Repository Persistence Factory: " + factoryName);

		if (factoryName == null || factoryName.trim().length() == 0) {
			// if no factory name, then check if this a file connection
			if (isFileFactory(properties)) {

				factory = (PersistentConnectionFactory) create(FILE_FACTORY_NAME, args);

				return factory;
			}

			if (isJDBCFactory(properties)) {
                args.add(Boolean.TRUE);
				factory = (PersistentConnectionFactory) create(JDBC_FACTORY_NAME, args);
				return factory;
			}

			throw new ConfigurationException(ConfigMessages.CONFIG_0009, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0009, PERSISTENT_FACTORY_NAME));

		}
		factory = (PersistentConnectionFactory) create(factoryName, args);

		return factory;

	}

	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public abstract PersistentConnection createPersistentConnection()
		throws ConfigurationException;


	private static boolean isFileFactory(Properties props) {
		String configFileName = props.getProperty("metamatrix.config.ns.filename");

		if (configFileName != null && configFileName.length() > 0) {
			return true;
		}
		return false;

	}

	private static boolean isJDBCFactory(Properties props) {
		String driver = props.getProperty("metamatrix.config.jdbc.persistent.readerDriver");

		if (driver != null && driver.length() > 0) {
			return true;
		}
		// for backwards compatibility reasons,
		// this will allow the use of either the persistent connection properties
		// or the pooling connection properties
    	driver = props.getProperty(JDBCConnectionResource.DRIVER);
		if (driver != null && driver.length() > 0) {
			return true;
		}
		return false;

	}

        /**
         * Helper method to create an instance of the class using the appropriate
         * constructor based on the ctorObjs passed.
         * @param className is the class to instantiate
         * @param ctorObjs are the objects to pass to the constructor; optional, nullable
         * @return Object is the instance of the class
         * @throws ConfigurationException if an error occurrs instantiating the class
         */

    	private static final Object create(String className, Collection ctorObjs) throws ConfigurationException {
        try {
            int size = (ctorObjs == null ? 0 : ctorObjs.size());
            Class[] names = new Class[size];
            Object[] objArray = new Object[size];
            int i = 0;

            if (size > 0) {
                for (Iterator it=ctorObjs.iterator(); it.hasNext(); ) {
                    Object obj = it.next();
//                    System.out.println("Argument to create class name: " + obj.getClass().getName());
                    names[i] = Class.forName(obj.getClass().getName());
                    objArray[i] = obj;
                    i++;
                }
            }

            Class cls = Class.forName(className.trim());

            Constructor ctor = cls.getDeclaredConstructor(names);


            return ctor.newInstance(objArray);

        } catch(NullPointerException e) {
            throw new ConfigurationException(e, ConfigMessages.CONFIG_0010, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0010));

        } catch (ClassNotFoundException exp ) {
               throw new ConfigurationException(exp, ConfigMessages.CONFIG_0011, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0011, className ));

        } catch(LinkageError e) {
            throw new ConfigurationException(e, ConfigMessages.CONFIG_0012, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0012, className));
        } catch(InstantiationException e) {
            throw new ConfigurationException(e, ConfigMessages.CONFIG_0013, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0013, className));
        } catch(IllegalAccessException e) {
            throw new ConfigurationException(e, ConfigMessages.CONFIG_0014, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0014, className));
        }  catch (Exception exp) {
        	if (exp instanceof ConfigurationException) {
        		throw (ConfigurationException) exp;
        	}
               throw new ConfigurationException(exp, ConfigMessages.CONFIG_0015, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0015, className));
        }


    }



/*

		String configFileName = properties.getProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY);

		PersistentConnection conn;
		if (configFileName == null || configFileName.trim().length() == 0) {
//conn = createFilePersistentConnection(properties);
			conn = createJDBCPersistentConnection(properties);
		} else {
			conn = createFilePersistentConnection(properties);
		}

		conn.init();

		return conn;
	}

	private PersistentConnection createFilePersistentConnection(Properties properties)
		throws ConfigurationException {

		ConfigurationModelAdapterImpl adapter =
			new ConfigurationModelAdapterImpl();

		FilePersistentConnection fps =
			new FilePersistentConnection(properties, adapter);
		return fps;

	}

	private PersistentConnection createJDBCPersistentConnection(Properties properties)
		throws ConfigurationException {

		ConfigurationModelAdapterImpl adapter =
			new ConfigurationModelAdapterImpl();

		JDBCPersistentConnection fps =
			new JDBCPersistentConnection(properties, adapter);
		return fps;

	}
*/

}
