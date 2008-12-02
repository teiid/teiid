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

package com.metamatrix.common.config.bootstrap;

import java.io.InputStream;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * Interface to abstract how the bootstrap properties are obtained
 * for the current configuration.
 */
public class PropertyFileCurrentConfigBootstrap implements CurrentConfigBootstrap {

    public static final String BOOTSTRAP_FILE_NAME = "metamatrix.properties"; //$NON-NLS-1$
    
    //noconfig is used so that in cases where the noconfig=true (which means no configuration exist in the datasource)
    // the obtaining of properties can still be tried without causing an exception to be thrown
    private boolean noconfig = true;
    
    public PropertyFileCurrentConfigBootstrap() {
    	
    }
    
    public PropertyFileCurrentConfigBootstrap(boolean noconfig_avail) {
    	this.noconfig = noconfig_avail;
    }

    /**
     * Get the properties that should be used to bootstrap the current configuration.
     * @return the bootstrap properties.
     */
    public Properties getBootstrapProperties() throws Exception {
        return this.getBootstrapProperties(null);
    }

    /**
     * Get the properties that should be used to bootstrap the current configuration,
     * using the specified default properties.
     * @return defaults the default bootstrap properties; may be null
     * @return the bootstrap properties.
     */
    public Properties getBootstrapProperties( Properties defaults ) throws Exception {
        Properties bootstrapProps = null;
        if ( defaults != null ) {
            bootstrapProps = new Properties(defaults);
        } else {
            bootstrapProps = new Properties();
        }
        InputStream bootstrapPropStream = null;
        try {
        	bootstrapPropStream = this.getClass().getClassLoader().getResourceAsStream(BOOTSTRAP_FILE_NAME);
        	if (bootstrapPropStream != null) {
        		bootstrapProps.load(bootstrapPropStream);
        	}
        } catch (Exception e) {
        	
        	// only throw the exception when noconfig=true is not set and there was no bootstrap found
        	if (!noconfig) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0069, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0069, BOOTSTRAP_FILE_NAME));
        	}
        	
        } finally {

	        try {
	            bootstrapPropStream.close();
	        } catch ( Exception e ) {
	        }
        }
        if (bootstrapProps == null) {
        	bootstrapProps = new Properties();
        }
        return new UnmodifiableProperties(bootstrapProps);

    }
}
