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

package com.metamatrix.platform.config.spi;

import java.util.*;

import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionInterface;

public interface ConfigurationTransaction extends TransactionInterface {

    public static class ComponentTypeSearch {

        /* Identifies certain ComponentTypes as Deployable */
        public static final int DEPLOYABLE_COMPONENT_TYPE = 0;
        /* Identifies certain ComponentTypes as Monitored */
        public static final int MONITORED_COMPONENT_TYPE = 1;

    }


    /**
     * Make all changes made during this transaction's lifetime
     * and release any data source locks currently held by the associated Connection.
     * A transaction can be committed or rolled back any number of times throughout its lifetime,
     * and throughout its lifetime the transaction is guaranteed to have the same connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with the associated connection.
     */
    void commit() throws ManagedConnectionException;

    /**
     * Drops all changes made during this transaction's lifetime
     * and release any data source locks currently held by the associated Connection.
     * Once this method is executed, the transaction (after rolling back) becomes invalid, and the connection
     * referenced by this transaction is returned to the pool.
     * <p>
     * Calling this method on a read-only transaction is unneccessary (and discouraged, since
     * the implementation does nothing in that case anyway).
     * @throws ManagedConnectionException if an error occurred within or during communication with the associated connection.
     */
    void rollback() throws ManagedConnectionException;

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Returns the current deployed <code>Configuration</code>.  Note, this configuration
     * may not match the actual configuration the system is currently executing under due
     * to administrative task that can be done to tune the system.  Those administrative
     * task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * @return Configuration that is currently in use
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @deprecated as of v 2.0 beta 1 use {@link #getDesignatedConfiguration}
     */
    Configuration getCurrentConfiguration() throws ConfigurationException;

    /**
     * Returns one of the well-known
     * {@link SystemConfigurationNames system configurations}, either
     * the
     * {@link SystemConfigurationNames#OPERATIONAL operational configuration},
     * the
     * {@link SystemConfigurationNames#NEXT_STARTUP next startup configuration},
     * or the
     * {@link SystemConfigurationNames#STARTUP startup configuration}.  Use
     * {@link SystemConfigurationNames} to supply the String parameter.
     * @param designation String indicating which of the system configurations
     * is desired; use one of the {@link SystemConfigurationNames} constants
     * @return the desired Configuration  
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    Configuration getDesignatedConfiguration(String designation) throws ConfigurationException;

    /**
     * Obtain a configuration that contains all its components and
     * the deployed components.
     * @param configurationName
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Configuration getConfiguration(String configurationName) throws ConfigurationException;


    /**
     * Returns the configuration model that contains everything a server needs 
     * to start.
     * param configName is the name of the configuration model to return
     * @return ConfigurationModelContainer
     */
    ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException;


    /**
     * Returns a <code>ComponentDefn</code> for the specified 
     * <code>ComponentDefnID</code> and <code>ConfigurationID</code>.
     * If the configuration is null the parent name from the 
     * componentID will be used.
     * </br>
     * The reason for allowing the configurationID to be optionally
     * specified is so that the same componentID can be used
     * to obtain a componentDefn from different configurations.
     * Otherwise, the requestor would have to create a new 
     * of componetDefnID for each configuration.
     * <br>
     * @param componentDefnID contains all the ids for which componet defns to be returned
     * @param configurationID is the configuration from which the component defns are to
     * be derived; optional, nullalble
     * @return Collection of ComponentDefn objects
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ComponentDefn getComponentDefinition(ComponentDefnID componentDefnID, ConfigurationID configurationID) throws ConfigurationException;

    /**
     * Obtain the list of component definition instances that makeup the configuration.
     * @return the list of Component instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Map getComponentDefinitions(ConfigurationID configurationID) throws ConfigurationException;


   /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all resource pools defined to the system.
     * @param configurationID is the configuration from which the component defns are to
     * be derived
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Collection getConnectionPools(ConfigurationID configurationID) throws ConfigurationException;

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResources() throws ConfigurationException;

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * that are of the specified resource type.
     * @param componentTypeID that identifies the type of resources to be returned
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResources(ComponentTypeID componentTypeID) throws ConfigurationException;


   /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          * 
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    void saveResources(Collection resourceDescriptors, String principalName) throws ConfigurationException;


    /**
     * Obtain the value for a specific property name
     * @param componentObjectID is the component for which the value is to be retrieved for
     * @param typeID is the type of the component the object represents
     * @param propertyName is the name of the property to obtain
     * @return the property value
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    String getComponentPropertyValue(ComponentObjectID componentObjectID, ComponentTypeID typeID, String propertyName) throws ConfigurationException;

     /**
     * Returns a Map of component type definitions for the <code>ComponentTypeID</code> specified,
     * keyed by the ComponentTypeDefnID
     * @param componentTypeID for the ComponentTypeID that has definitions defined.
     * @return Collection of component type difinitions
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException;


    /**
     * Obtain the list of deployed components that represent the configuration
     * when deployed.
     * @return the list of DeployedComponents
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    List getDeployedComponents(ConfigurationID configrationID) throws ConfigurationException;

    /**
     *  Returns a <code>ComponentType</code> for the specified <code>ComponentTypeID</code>
     *  @param id is for the requested component type.
     *  @return ComponentType based on the id
     *  @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException;

    /**
     * Returns a <code>Map</code> of type <code>ComponentType</code> keyed by ComponentTypeID.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     *
     */
    Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException;

    /**
     * Returns a <code>Map</code> of type <code>ProductType</code> keyed by ProductTypeID.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @deprecated as of v 2.0 beta 1 use {@link #getDesignatedConfigurationID}
     *
     */
    Collection getProductTypes(boolean includeDeprecated) throws ConfigurationException;

    /**
     * Returns the current configurationID
     * @return ConfigurationID for the current configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @deprecated as of v 2.0 beta 1 use {@link #getDesignatedConfigurationID}
     */
    ConfigurationID getCurrentConfigurationID() throws ConfigurationException;

    /**
     * Returns the ID of one of the well-known
     * {@link SystemConfigurationNames system configurations}, either
     * the
     * {@link SystemConfigurationNames#OPERATIONAL operational configuration},
     * the
     * {@link SystemConfigurationNames#NEXT_STARTUP next startup configuration},
     * or the
     * {@link SystemConfigurationNames#STARTUP startup configuration}.  Use
     * {@link SystemConfigurationNames} to supply the String parameter.
     * @param designation String indicating which of the system configurations
     * is desired; use one of the {@link SystemConfigurationNames} constants
     * @return the desired Configuration
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    ConfigurationID getDesignatedConfigurationID(String designation) throws ConfigurationException;

    /**
     * Returns a <code>Map</code> of type <code>ComponentType</code>  keyed by ComponentTypeID
     * that are flagged as being monitored.  A component of this type is considered
     * to be available for monitoring statistics.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code> keyed by ComponentTypeID
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException;

    /**
     * Obtain the list of registered host
     * @return Collection of Hosts
     * @throws ConfigurationException if an error occurred within or during communication with the Metadata Service.
     */
    Collection getHosts() throws ConfigurationException;

    /**
     * Return the time the server was started. If the state of the server is not "Started"
     * then a null is returned.
     *
     * @return Date Time server was started.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    Date getServerStartupTime() throws ConfigurationException;
    

    /**
     * <p>Returns a Collection containing the Configuration object for the specified
     * ConfigurationID id, and also any dependant objects needed to fully
     * define this configuration, such as Host objects, ComponentType
     * objects, and ComponentTypeDefn objects.</p>
     *
     * <p>A Configuration instance contains all of the
     * <code>ComponentDefn</code> objects that "belong" to just that
     * Configuration model: VM component definitions, service
     * component definitions, product service configurations, and
     * deployed components.  Objects such as Host objects,
     * ComponentType objects, ComponentTypeDefn, Resources, and
     * ConnectorBinding objects describe or support
     * ComponentDefns, but are not contained by a Configuration.  Therefore,
     * they are included in this Collection for convenience.</p>
     *
     * <p>The Collection will contain instances of
     * {@link com.metamatrix.common.namedobject.BaseObject}.
     * Specifically, this Map should contain the objects for:
     * one configuration object, one or more Host objects, one or more
     * ComponentType objects, and one or more ComponentTypeDefn objects.</p>
     *
     * <p>This method is intended to facilitate exporting a configuration
     * to XML.</p>
     *
     * <p>Here is what the Collection would contain at runtime:
     * <pre>
     * Configuration instance
     * Host instance1
     * Host instance2
     * ...
     * ConnectorBinding instance1
     * ConnectorBinding instance2
     * ...
     * SharedResource intance1
     * SharedResource instance
     * ...
     * ComponentType instance1
     * ComponentType instance2
     * ...
     * ComponentTypeDefn instance1
     * ComponentTypeDefn instance2
     * </pre></p>
     *
     * @param configID ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    
    Collection getAllObjectsForConfigurationModel(ConfigurationID configID) throws ConfigurationException;

    /**
     * Returns a boolean indicating if the configuration already exist or not.
     * @param configurationName the identifier of the configuration
     * @return boolean of false if the configuration is found
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public boolean doesConfigurationExist( String configurationName )
        throws ConfigurationException;


    // ----------------------------------------------------------------------------------------
    //                 C O N F I G U R A T I O N    U P D A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * Execute the specified actions.
     * @param List of actions to be performed 
     * @param principalName the name of the principal that is requesting the lock
     * @return the set of BaseID objects denoting which objects were affected
     * by these actions
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    Set executeActions( List actions, String principalName ) throws InvalidComponentException, ConfigurationException;

    /**
     * Execute the specified actions.
	 * @param doAdjust boolean to turn ConnectorBinding adjustments on/off
     * @param List of actions to be performed 
     * @param principalName the name of the principal that is requesting the lock
     * @return the set of BaseID objects denoting which objects were affected
     * by these actions
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
 //   Set executeActions( boolean doAdjust, List actions, String principalName ) throws InvalidComponentException, ConfigurationException;

    /**
     * Overwrite the specified configuration by copying another configuration
     * over it.  This includes assigning any
     * {@link #getDesignatedConfiguration designations}
     * of the configuration to be overwritten to the configuration to
     * be copied.  Both configurations must already be in the data source.
     * (This method is needed to implement baselining).
     * @param configToCopy the ConfigurationID of the Configuration to be
     * copied
     * @param configToCopy the ConfigurationID of the Configuration to be
     * deleted - the "configToCopy" will be overwritten in its place.
     * @param principalName the name of the principal that is requesting the
     * modification
     * @return the new ID of the newly-copied Configuration
     * @throws InvalidConfigurationException if either ConfigurationID is invalid.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    ConfigurationID overwriteConfiguration(ConfigurationID configToCopy, ConfigurationID configToOverwrite, String principalName) throws InvalidConfigurationException, ConfigurationException;

}

