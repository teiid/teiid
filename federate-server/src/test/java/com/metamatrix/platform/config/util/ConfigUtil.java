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

package com.metamatrix.platform.config.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentUtil;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class ConfigUtil {


    /**
     * Imports into next startup configuration
     */
    public static void importConfiguration(String fileName, Properties properties, String principal) throws ConfigurationException {
		ConfigurationModelContainer nsModel = null;

 		try {
        	 nsModel = FilePersistentUtil.readModel(fileName, properties.getProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY), Configuration.NEXT_STARTUP_ID);
 		} catch (Exception e) {
 			throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0186, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0186, fileName));
 		}

		ConfigUtil.importConfiguration(nsModel, properties, principal);

    }

    /**
     * Imports into next startup configuration model
     */
    public static void importConfiguration(ConfigurationModelContainer model, Properties properties, String principal) throws ConfigurationException {
		Properties props = PropertiesUtils.clone(properties, false);

        PersistentConnectionFactory pf = PersistentConnectionFactory.createPersistentConnectionFactory(props);

//		PersistentConnectionFactory pf = new PersistentConnectionFactory();


        PersistentConnection pc = pf.createPersistentConnection();

 //       System.out.println("Props: " + PropertiesUtils.prettyPrint(resourcePoolProperties));

        // write the models out
        pc.delete(Configuration.NEXT_STARTUP_ID, principal);
      	pc.write(model, principal);



    }


    public static byte[] exportConfiguration(Properties properties, String principal, String application, String version) throws ConfigurationException {
		Properties props = PropertiesUtils.clone(properties, false);

        PersistentConnectionFactory pf = PersistentConnectionFactory.createPersistentConnectionFactory(props);

//		PersistentConnectionFactory pf = new PersistentConnectionFactory();
        PersistentConnection pc = pf.createPersistentConnection();
        ConfigurationModelContainer nsModel = pc.read(Configuration.NEXT_STARTUP_ID);
        byte[] data = null;
        try {
        	ByteArrayOutputStream out = new ByteArrayOutputStream();
        	BufferedOutputStream bos = new BufferedOutputStream(out);

	        XMLConfigurationImportExportUtility xmlUtil =
	                    new XMLConfigurationImportExportUtility();

	       	Properties versionprops = new Properties();
	                versionprops.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, application);
	                versionprops.put(
	                    ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY,
	                    version);
	                versionprops.put(ConfigurationPropertyNames.USER_CREATED_BY, principal);
	        xmlUtil.exportConfiguration(bos, nsModel.getAllObjects(), versionprops);

        	data = out.toByteArray();

        } catch (Exception e) {
        	throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0187, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0187, principal));
        }

    	return data;
    }


}
