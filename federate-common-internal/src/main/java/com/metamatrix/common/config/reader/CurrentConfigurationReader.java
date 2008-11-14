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

import java.util.Properties;
import java.util.Collection;
import java.util.Map;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;

/**
 * <p>
 * This interface defines a self-contained reader for the current configuration,
 * and should be used <i>only</i> by the {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}
 * framework.  As such, this is an extremely low-level implementation that may
 * <i>not</i> use anything but <code>com.metamatrix.common.util</code> components
 * and only components that do not use {@link com.metamatrix.common.logging.LogManager LogManager}.
 * </p>
 * <p>
 * Each class that implements this interface must supply a no-arg constructor.
 * </p>
 */
public interface CurrentConfigurationReader {

    /**
     * This method should connect to the repository that holds the current
     * configuration, using the specified properties.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @param env the environment properties that define the information
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */
    void connect( Properties env ) throws ConfigurationConnectionException;

    /**
     * This method should close the connection to the repository that holds the current
     * configuration.  The implementation may <i>not</i> use logging but
     * instead should rely upon returning an exception in the case of any errors.
     * @throws Exception if there is an error establishing the connection.
     */
    void close() throws Exception;

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------


    /**
     * Obtain the value for a specific property name
     * @param configID is the id for the Configuration
     * @param propertyName is the name of the property to obtain
     * @return the property value
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    String getComponentPropertyValue(ConfigurationID configID, String propertyName) throws ConfigurationException;

    /**
     * Obtain the properties for the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the properties
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    Properties getConfigurationProperties() throws ConfigurationException;
    
    /**
     * Obtain the properties for the startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the properties
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    Properties getStartupConfigurationProperties() throws ConfigurationException;

    /**
     * Obtain the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
 //   Configuration getConfiguration() throws ConfigurationException;
    
    /**
     * Obtain the next startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    Configuration getNextStartupConfiguration() throws ConfigurationException;
    
    
    /**
     * Obtain the startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    Configuration getStartupConfiguration() throws ConfigurationException;
        

    /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    ConfigurationModelContainer getConfigurationModel() throws ConfigurationException;


    /**
     * Obtain the startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    ConfigurationModelContainer getStartupConfigurationModel() throws ConfigurationException;
    
    
    /**
     * Returns a <code>Collection</code> of type <code>ComponentType</code> .
     * that are flagged as being monitored.  A component of this type is considered
     * to be available for monitoring statistics.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return List of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ComponentType
     */
 //   Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException;

    /**
     * Returns a <code>Collection</code> of type <code>ComponentType</code> that represents
     * all the ComponentTypes defined.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return List of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ComponentType
     */
    Collection getComponentTypes(boolean includeDeprecated) throws ConfigurationException;

    /**
     * Returns a <code>Collection</code> of type <code>ProductType</code> that represents
     * all the ComponentTypes defined.
     * @return List of type <code>ProductType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
    Collection getProductTypes() throws ConfigurationException;
    
    
    
    // ------------------------------------------------------------------------------------
    //             O P T I O N A L    S U P P O R T
    // ------------------------------------------------------------------------------------

    /**
     * Returns the full Host impl for a HostID.  <i>Optional method.</i>
     * @param hostID ID of the Host that is wanted
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    Host getHost(HostID hostID) throws ConfigurationException;

    /**
     * Returns an appropriate initializer for the system configuration(s).
     * <i>Optional method.</i>
     * @return an appropriate initializer for the system configuration(s)
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    CurrentConfigurationInitializer getInitializer() throws ConfigurationException;

    /**
    * Returns a map of all the resource properties defined.  These resources
    * are the connection properties required to connect to a specific resource.
    * The key is the resource name, the value is a Property object.
    * @return Map of resource properties.
    */
     Map getResourceProperties() throws ConfigurationException ;
    
    /**
     * Returns the resource for the specified resourceName.  This is an alternate
     * method for {@see #getResourceProperties}
     */
    SharedResource getResource(String resourceName ) throws ConfigurationException;
    

}

