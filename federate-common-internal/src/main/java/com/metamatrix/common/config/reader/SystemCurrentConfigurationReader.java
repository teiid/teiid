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

package com.metamatrix.common.config.reader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.NullConfiguration;

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
public class SystemCurrentConfigurationReader implements CurrentConfigurationReader{

    /**
     * Default, no-arg constructor
     */
    public SystemCurrentConfigurationReader(){
    }

    /**
     * This method does nothing.
     * @param env the environment properties - not used
     */
    public void connect( Properties env ) throws ConfigurationConnectionException{
    	// nothing
    }
    
    /**
     * This method does nothing
     */
    public void close() throws Exception {
    	// nothing
    }

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Returns System.getProperties()
     * @return the properties
     */
    public Properties getConfigurationProperties() throws ConfigurationException{
        return System.getProperties();
    }

    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfigurationProperties()
     * @since 4.2.1
     */
    public Properties getStartupConfigurationProperties() throws ConfigurationException {
        return System.getProperties();
    }
    
    /**
    * Returns the value of a property value - the specified configuration object is ignored.
    * @param configID This parameter is ignored
    * @return property value, or null if there was no value for the specified
    * property name on the system properties
    */
    public String getComponentPropertyValue(ConfigurationID configID, String propertyName) {
        return System.getProperties().getProperty(propertyName);
    }

    /**
     * Obtain the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
//    public Configuration getConfiguration() throws ConfigurationException{
//        return new NullConfiguration();
//    }
    
    /**
     * Obtain the next startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * 
     * This implementation will perform the same as calling getConfiguration.
     * 
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    
    public Configuration getNextStartupConfiguration() throws ConfigurationException {
        return new NullConfiguration();    
        
    }
    
   /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
    	
        throw new UnsupportedOperationException("Method getConfigurationModel is not implemented in SystemCurrentConfigurationReader"); //$NON-NLS-1$
    	
    }
    
    

    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfiguration()
     * @since 4.2
     */
    public Configuration getStartupConfiguration() throws ConfigurationException {
        return new NullConfiguration();    
    }
    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfigurationModel()
     * @since 4.2
     */
    public ConfigurationModelContainer getStartupConfigurationModel() throws ConfigurationException {
        throw new UnsupportedOperationException("Method getStartupConfigurationModel is not implemented in SystemCurrentConfigurationReader"); //$NON-NLS-1$
    }
    /**
    * Returns empty list.
    * @param includeDeprecated Parameter ignored
    * @return Empty collection
    */
/*    
    public Collection getMonitoredComponentTypes(boolean includeDeprecated) {
        return Collections.EMPTY_LIST;
    }
*/
    /**
    * Returns empty list.
    * @param includeDeprecated Parameter ignored
    * @return Empty collection
    */
    public Collection getComponentTypes(boolean includeDeprecated) {
        return Collections.EMPTY_LIST;
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ProductType</code> that represents
     * all the ComponentTypes defined.
     * @return List of type <code>ProductType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
   public Collection getProductTypes() throws ConfigurationException {
       return Collections.EMPTY_LIST;
   }     

    // ------------------------------------------------------------------------------------
    //             O P T I O N A L    S U P P O R T
    // ------------------------------------------------------------------------------------

    /**
     * Returns the full Host impl for a HostID.  This method is not implemented,
     * since it is currently only possible to read configuration properties
     * from a file.
     * @param hostID ID of the Host that is wanted
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    public Host getHost(HostID hostID) throws ConfigurationException{
        throw new UnsupportedOperationException("Method getHost is not implemented in SystemCurrentConfigurationReader"); //$NON-NLS-1$
    }

    /**
     * Should Return an appropriate initializer for the system configuration(s).
     * This method is not implemented, since it is currently only possible
     * to read configuration properties from a file.
     * @return an appropriate initializer for the system configuration(s)
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    public CurrentConfigurationInitializer getInitializer() throws ConfigurationException{
        throw new UnsupportedOperationException("Method getInitializer is not implemented in SystemCurrentConfigurationReader"); //$NON-NLS-1$
    	
    }

    public Map getResourceProperties() throws ConfigurationException {
        return new HashMap();
    }
    
    public SharedResource getResource(String resourceName ) throws ConfigurationException {
		return null;    	
    }
    
}

