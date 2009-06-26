/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package com.metamatrix.common.config.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * <p>
 * This class implements a self-contained reader for the current configuration,
 * and should be used <i>only</i> by the {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}
 * framework.  As such, this is an extremely low-level implementation that may
 * <i>not</i> use anything but <code>com.metamatrix.common.util</code> components
 * and only components that do not use {@link com.metamatrix.common.logging.LogManager LogManager}.
 * </p>
 * <p>
 * Each class that implements this interface must supply a no-arg constructor.
 * </p>
 */
public class PropertiesConfigurationReader implements CurrentConfigurationReader {

	private ConfigurationModelContainer c;
	
    /**
     * The environment property name for the property file that contains the configuration.
     */
    public static final String FILENAME = "metamatrix.config.readerFile"; //$NON-NLS-1$

    /**
     * Default, no-arg constructor
     * @throws ConfigurationException 
     * @throws ConfigurationConnectionException 
     */
    public PropertiesConfigurationReader() throws ConfigurationConnectionException, ConfigurationException{
    	Properties env = CurrentConfiguration.getInstance().getBootStrapProperties();
    	String filename = env.getProperty(FILENAME);
        Properties p = null;
        if (filename != null) {
        	File f = new File(filename);
        	InputStream is = null;
        	try {
	        	if (f.exists()) {
					is = new FileInputStream(f);
	        	} else {
	        		is = this.getClass().getResourceAsStream(filename);
	        	}
	        	if (is == null) {
	        		throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_ERR_0064, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0064, filename));
	        	}
        		p = new Properties(env);
        		p.load(is);
			} catch (IOException e) {
				throw new ConfigurationConnectionException(e);
        	} finally {
        		if (is != null) {
        			try {
						is.close();
					} catch (IOException e) {
					}
        		}
        	}
        } else {
        	p = env;
        }
        
        // Use the properties from the file to create a new Configuration object ...
        ConfigurationObjectEditor coe = new BasicConfigurationObjectEditor(false);
        Configuration currentConfiguration = coe.createConfiguration(PropertiesConfigurationReader.class.getSimpleName() + ':' + filename);
        currentConfiguration = (Configuration)coe.modifyProperties(currentConfiguration, p, ConfigurationObjectEditor.SET);
        c = new ConfigurationModelContainerImpl(currentConfiguration);
    }

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
    	return c;
    }

}

