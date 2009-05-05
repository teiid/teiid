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

package com.metamatrix.platform.config.api.service;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.platform.service.api.ServiceInterface;


public interface ConfigurationServiceInterface extends ServiceInterface {

    public static String NAME ="ConfigurationService"; //$NON-NLS-1$

    /**
     * Returns a <code>ConfigurationObjectEditor</code> to perform editing operations on a configuration type object. The
     * editing process will create actions for each specific type of editing operation. Those actions are what need to be
     * submitted to the <code>ConfigurationService</code> for actual updates to occur.
     * 
     * @return ConfigurationObjectEditor
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ConfigurationObjectEditor createEditor() throws ConfigurationException;

    /**
     * Returns the <code>ConfigurationID</code> for the operational configuration.
     * 
     * @return ConfigurationID for current configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ConfigurationID getCurrentConfigurationID() throws ConfigurationException;

    /**
     * Returns the ID of the next startup <code>Configuration</code>, which should reflect the desired runtime state of the
     * system.
     * 
     * @return ID of next startup configuration
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ConfigurationID getNextStartupConfigurationID() throws ConfigurationException;

    /**
     * Returns the current deployed <code>Configuration</code>. Note, this configuration may not match the actual configuration
     * the system is currently executing under due to administrative task that can be done to tune the system. Those
     * administrative task <b>do not</b> change the actual <code>Configuration</code> stored in the
     * <code>ConfigurationService</code>.
     * 
     * @return Configuration that is currently in use
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Configuration getCurrentConfiguration() throws ConfigurationException;

    /**
     * Returns the next startup <code>Configuration</code>, the Configuration that the system will next boot up with (once it
     * is entirely shut down).
     * 
     * @return Configuration that the system will next start up with.
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Configuration getNextStartupConfiguration() throws ConfigurationException;

    /**
     * Returns the <code>ConfigurationModelContainer</code> that contains everything (i.e., ComponentTypes, Shared Resources and
     * ComponentDefns) that the server needs to start.
     * 
     * @param configName
     *            if the name of the configuration model to obtain
     * @return ConfigurationModelContainer
     */
    ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException;

    /**
     * <p>
     * Returns a Collection containing the Configuration object for the specified ConfigurationID id, and also any dependant
     * objects needed to fully define this configuration, such as Host objects, ComponentType objects, and ComponentTypeDefn
     * objects.
     * </p>
     * <p>
     * A Configuration instance contains all of the <code>ComponentDefn</code> objects that "belong" to just that Configuration:
     * VM component definitions, service component definitions, product service configurations, and deployed components. Objects
     * such as Host objects, ComponentType objects, and ComponentTypeDefn objects describe ComponentDefns, but are not contained
     * by a Configuration. Therefore, they are included in this Collection for convenience.
     * </p>
     * <p>
     * The Collection will contain instances of {@link com.metamatrix.common.namedobject.BaseObject}. Specifically, this Map
     * should contain the objects for: one configuration object, one or more Host objects, one or more ComponentType objects, and
     * one or more ComponentTypeDefn objects.
     * </p>
     * <p>
     * This method is intended to facilitate exporting a configuration to XML.
     * </p>
     * <p>
     * Here is what the Collection would contain at runtime:
     * 
     * <pre>
     * 
     *  Configuration instance
     *  Host instance1
     *  Host instance2
     *  ...
     *  ComponentType instance1
     *  ComponentType instance2
     *  ...
     *  ComponentTypeDefn instance1
     *  ComponentTypeDefn instance2
     *  
     * </pre>
     * 
     * </p>
     * 
     * @param configID
     *            ID Of a Configuration
     * @return Collection of BaseObject instances
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getConfigurationAndDependents(ConfigurationID configID) throws ConfigurationException;

    /**
     * Returns the component type definitions for the specified <code>ComponentTypeID</code>. This does not return the
     * dependent definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException;

    /**
     * Returns the all component type definitions for the specified <code>ComponentTypeID</code>. This includes the dependent
     * definitions for service type components.
     * 
     * @param componentTypeID
     *            is a ComponentTypeID
     * @return Collection of ComponentTypeDefns
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @see getDependentComponentTypeDefinitions(ComponentTypeID)
     */
    Collection getAllComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException;

    /**
     * Returns a <code>ComponentType</code> for the specified <code>ComponentTypeID</code>
     * 
     * @param id
     *            is for the requested component type.
     * @return ComponentType based on the id
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException;

    /**
     * Returns a <code>List</code> of type <code>ComponentType</code> that represents all the ComponentTypes defined.
     * 
     * @param includeDeprecated
     *            true if class names that have been deprecated should be included in the returned list, or false if only
     *            non-deprecated constants should be returned.
     * @return Collection of type <code>ComponentType</code>
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     * @see #ComponentType
     */
    Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException;


    /**
     * Returns a <code>Host</code> for the specified <code>HostID</code>. </br>
     * 
     * @return Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Host getHost(HostID hostID) throws ConfigurationException;

    /**
     * Returns a <code>Collection</code> of currently defined hosts. This method does not cache, it re-retrieves the data
     * every time. </br>
     * 
     * @return Collection of type Host
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getHosts() throws ConfigurationException;


    /**
     * Returns a <code>ComponentDefn</code> for the specified <code>ComponentDefnID</code>. </br>
     * 
     * @param configurationID
     *            is the configuration for which the component exist
     * @param componentDefnID
     *            is the component being requested
     * @return ComponentDefn
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    ComponentDefn getComponentDefn(ConfigurationID configurationID, ComponentDefnID componentDefnID)
    throws ConfigurationException;
    
    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} for all internal
     * resources defined to the system. The internal resources are not managed with the other configuration related information.
     * They are not dictated based on which configuration they will operate (i.e., next startup or operational);
     * 
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Collection getResources() throws ConfigurationException;

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor} in
     * the collection.
     * 
     * @param resourceDescriptors
     *            for the resources to be changed *
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    void saveResources(Collection resourceDescriptors, String principalName) throws ConfigurationException;

    // --------------------------------------------------------------
    // A C T I O N M E T H O D S
    // --------------------------------------------------------------

    /**
     * Execute as a single transaction the specified action, and optionally return the set of objects or object IDs that were
     * affected/modified by the action.
     * 
     * @param action
     *            the definition of the action to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Set executeTransaction(ActionDefinition action, String principalName ) 
    	throws ConfigurationException;

    /**
     * Execute a list of actions, and optionally return the set of objects or object IDs that were affected/modified by the
     * action.
     * 
     * @param actions
     *            the ordered list of actions that are to be performed on data within the repository.
     * @param principalName
     *            of the person executing the transaction
     * @return the set of objects that were affected by this transaction.
     * @throws IllegalArgumentException
     *             if the action is null or if the result specification is invalid
     * @throws ConfigurationException
     *             if an error occurred within or during communication with the Configuration Service.
     */
    Set executeTransaction(List actions, String principalName) 
    	throws ConfigurationException;


    /**
     * Add Host to Configuration Add a new Host to the System (MetaMatrix Cluster)
     * 
     * @param hostName
     *            Host Name of new Host being added to Configuration
     * @param principalName
     * @param properties
     * @return Host
     * @throws ConfigurationException
     * @since 4.3
     */
    Host addHost(String hostName, String principalName, Properties properties) throws ConfigurationException;
    
    /**
     * Add Process for the specified Host to Configuration Add a new Virtuial Machine to the System (MetaMatrix Cluster)
     * 
     * @param processName
     *            Name of the New Process being Added to Configuration
     * @param hostName
     *            Name of the Host that the new Process is being added
     * @param principalName
     *            User Name of user who is making the change
     * @param properties
     *            name, value need to setup a Host
     * @return VMComponentDefn
     * @throws ConfigurationException
     * @since 4.3
     */
    VMComponentDefn addProcess(String processName, String hostName, String principalName, Properties properties) throws ConfigurationException;
    
    /**
     * Set System Property Value in Configuration
     * 
     * @param propertyName
     *            Name of Property
     * @param propertyValue
     *            Value of Property
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @since 4.3
     */
    void setSystemPropertyValue(String propertyName,
                                       String propertyValue,
                                       String principalName) throws ConfigurationException;
    
    
    /**
     * Set System Property Values in Configuration.
     * Any properties not set will be left unchanged.
     * 
     * @param properties
     *            Properties to set
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @since 4.3
     */
    void updateSystemPropertyValues(Properties properties,
                                       String principalName) throws ConfigurationException;
    
    /**
     * Deploy a new Connector Binding into Configuration
     * 
     * @param connectorBindingName
     * @param connectorType
     *            Connector Type for this Connector Binding
     * @param vmName Name of the PSC to deploy the Connector Binding to.  
     *         If pscName is null, this method does not deploy the Connector Binding to a PSC.
     * @param principalName
     *            User Name of user who is making the change
     * @param properties
     * @return ConnectorBinding object
     * @throws ConfigurationException
     * @since 4.3
     */
    ConnectorBinding createConnectorBinding(String connectorBindingName,
                                                   String connectorType,
                                                   String vmName,
                                                   String principalName,
                                                   Properties properties) throws ConfigurationException;
    
    /**
     * Modify a Component in Configuration
     * 
     * @param theObject
     * @param theProperties
     * @param principalName
     * @return
     * @throws Exception
     * @since 4.3
     */
    public Object modify(ComponentObject theObject,
                         Properties theProperties,
                         String principalName) throws ConfigurationException;
    
    
    /**
     * Import a Connector Type
     * 
     * @param inputStream
     * @param name
     *            Name of the Connector Type to import
     * @param principalName
     *            User Name of user who is making the change
     * @return ComponentType
     * @throws ConfigurationException
     * @since 4.3
     */
    public ComponentType importConnectorType(InputStream inputStream,
                                             String name,
                                             String principalName) throws ConfigurationException;
    
    /**
     * Import a connector Binding from InputStream, and deploy it to a PSC.
     *  
     * @param inputStream 
     * @param name Name of Connector Binding to import
     * @param vmName Name of the VM to deploy the Connector Binding to.  
     * If vmName is null, this method does not deploy the Connector Binding .
     * @param principalName
     * @return ConnectorBinding
     * @throws ConfigurationException
     * @since 4.3
     */
    public ConnectorBinding importConnectorBinding(InputStream inputStream,
                                                   String name,
                                                   String vmName,
                                                   String principalName) throws ConfigurationException;
        
    /**
     * Deletes a component object.
     * 
     * @param theObject
     *            the object being deleted
     * @param theDeleteDependenciesFlag
     *            boolean flag for deleting dependencies
     * @param principalName
     *            User Name of user who is making the change
     * @throws ConfigurationException
     * @since 4.3
     */
    public void delete(ComponentObject theObject, boolean theDeleteDependenciesFlag,String principalName) 
    	throws ConfigurationException; 
    
    /**
     * Delete a Component Type
     *  
     * @param componentType Component Type Object being deleted
     * @param principalName User Name who is making the change
     * @throws ConfigurationException
     * @since 4.3
     */
    public void delete(ComponentType componentType, String principalName) 
    	throws ConfigurationException;
    
    
    /**
     * Deploys the ServiceComponentDefns 
     * contained by the Configuration, onto the specified Host and VM.
     * 
     * @param theProcessID for the VM on which the services will be deployed
     * @param serviceName Name of the ServiceComponentDefn
     * @param principalName User Name deploying the Services
     * 
     * @return DeployedComponent of the ServiceComponentDefns that was deployed
     * 
     * @throws ConfigurationException
     * @since 6.1
     */
    
    public DeployedComponent deployService(VMComponentDefnID theProcessID,
                                String serviceName,
                                String principalName) throws ConfigurationException;

    
    
    /**
     * Check whether the encrypted properties for the specified ComponentDefns can be decrypted.
     * @param defns List<ComponentDefn>
     * @return List<Boolean> in the same order as the parameter <code>defns</code>.
     * For each, true if the properties could be decrypted for that defn.
     * @throws ConfigurationException
     * @since 4.3
     */
    public List checkPropertiesDecryptable(List defns) throws ConfigurationException;
    
}
