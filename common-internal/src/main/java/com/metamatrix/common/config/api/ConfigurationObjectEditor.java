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

package com.metamatrix.common.config.api;

import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.actions.ObjectEditor;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.object.PropertyDefinition;

public interface ConfigurationObjectEditor extends ObjectEditor {

    /**
     * Modify the specified target object by undoing the specified action.
     * <p>
     * In general, the client user should obtain the target Relationship from
     * the action, and pass this object in as the target argument.
     * In the case of a DestroyObject action being undone, the corresponding
     * MetadataObject may not exist in the client's universe - it has already been
     * destroyed; passing a null as the target argument in this case is acceptable.
     * For all other actions, the target argument may not be null.
     * <p>
     * The result of this object will be the target after being modified by
     * the specified action.  In the case of a CreateObject action being undone,
     * this method 'destroys' the object, so a null result is returned; the
     * client should ensure this object is removed from its context.
     * In all other cases, this method returns the modified target.
     * @param action the action to be undone (may not be null).
     * @param target the Configuration that is the target of the action.
     * @return the modified Configuration, or null if the object is to be
     * removed as a result of undoing the action.
     * @throws IllegalArgumentException if the action reference is null, or
     * if the target is null and the action is not a DestroyObject.
     */
//   Configuration undoAction( ActionDefinition action, BaseObject target );


    // ----------------------------------------------------------------------------------
    //                  C R E A T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Create a new Host instance with the specified ID.
     * @param hostName for the new Host (may not be null).
     * @return the Host instance
     * @throws IllegalArgumentException if the hostName is null     
     */
    Host createHost(String hostName );

    Host createHost(ConfigurationID configurationID, String hostName );



    Host createHost(Configuration configuration, String hostName );


	/**
	 * This method is used only for importing a complete configurtion model.
	 * The objects should include all object types (e.g., Configuration, Host,
	 * ComponentDefn, DeployedComponent, ComponentTypes, PSCs, etc.);
	 */
	void createConfiguration(ConfigurationID configID, Collection configObjects);
	
    /**
     * Create a new Configuration instance with the specified name ID.
     * @param configurationName for the new configuration (may not be null).
     * @return the Configuration instance
     * @throws IllegalArgumentException if the configurationName is null
     */
    Configuration createConfiguration(String configurationName );

    /**
     * Create a new Configuration instance with the specified name ID, and the
     * specified Date parameters.  This method is only useful for
     * <i>re</i>-creating a serialized configuration; typically a client will
     * want to use {@link #createConfiguration(String)}
     * @param configurationName for the new configuration (may not be null).
     * @param creationDate Date the configuration was created.
     * @param creationDate Date the configuration was last modified.
     * @return the Configuration instance
     * @throws IllegalArgumentException if the configurationName is null
     */
    Configuration createConfiguration(String configurationName, Date creationDate, Date lastChangedDate);

    /**
     *  Create a new ComponentType instance with the specified name.
     *  @param classTypeCode identifies the type of class this component type should represent
     *      @see ComponentType for type codes
     *  @param name is the name of the ComponentType
     *  @param parentID is the component type that is the parent, can be null
     *  @param superID is the component type that is a super type of this component type, can be null
     *  @param deployable is a boolean indicating if the type can be deployed in a configuration
     *  @param monitored is a boolean insdicating if the type is to be monitored
     *  @return ComponentType
     */
    ComponentType createComponentType(int classTypeCode, String name, ComponentTypeID parentID, ComponentTypeID superID, boolean deployable, boolean monitored);

    /**
     *  Create a new ComponentType instance from a copy of the specified ComponentType.
     *  @param componentType is the original component type to copy from
     *  @param name is the name of the ComponentType
     *  @return ComponentType
     */
    ComponentType createComponentType(ComponentType componentType, String name);

    /**
     * Create a new ComponentTypeDefn and update the ComponentType with the new defintion.
     * @param type is the ComponentType the definition will be added to
     * @param propertyDefinition are the attributes describing this definition
     * @param isEffectiveImmediately indicates that a change to a property
     * value for this definition can have an immediate effect (as opposed to
     * not taking effect until server reboots)
     * @return the ComponentTypeDefn instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    ComponentTypeDefn createComponentTypeDefn(ComponentType type, PropertyDefinition propertyDefinition, boolean isEffectiveImmediately) ;

 
    /**
     * Create a new ComponentTypeDefn instance with the specified name.  To create
     * a basic PropertyDefinition, do the following:
     *
     *      PropertyDefinition defn = new PropertyDefinition();
     *      defn.setPropertyType(PropertyType.BOOLEAN);
     *
     * @param typeID for the parent component type this definitions is used by
     * @param propertyDefinition are the attributes describing this definition
     * @param isEffectiveImmediately indicates that a change to a property
     * value for this definition can have an immediate effect (as opposed to
     * not taking effect until server reboots)
     * @return the ComponentTypeDefn instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     *
     * @see createComponentTypeDefn(ComponentType, PropertyDefinition)
     */
    ComponentTypeDefn createComponentTypeDefn(ComponentTypeID typeID, PropertyDefinition propertyDefinition, boolean isEffectiveImmediately) ;

    /**
     *  Create a new PropDefnAllowedValue that will be added to the modifiable (cloned)
     *  PropertyDefinition that was obtained from the ComponentTypeDefn.
     *  @param typeDefn is the ComponentTypeDefn the allowed value will be added to
     *  @param propDefn is the PropertyDefinition that the new create PropDefnAllowedValue will
     *  be added to.
     *  @param value is the allowed value
     *  @return PropDefnAllowedValue
     *  @throws InvalidComponentException if the existing allowed values in the typeDefn are not
     *          of type PropDefnAllowedValue
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    PropDefnAllowedValue createPropDefnAllowedValue(ComponentTypeDefn typeDefn, PropertyDefinition propDefn, String value) throws InvalidComponentException;

    /**
     * Create a new VM Component Definition instance with a specified configuration
     * @param id the ID for this object (may not be null).
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     *
     * @see createVMComponentDefn(Configuration, ComponentTypeID, String)
     */
    VMComponentDefn createVMComponentDefn(ConfigurationID configurationID, HostID hostID, ComponentTypeID typeID, String componentName);

    /**
     * Create a new VM Component Definition instance with a specified configuration
     * @param configuration that will contain the new created component definition
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    VMComponentDefn createVMComponentDefn(Configuration configuration, HostID hostID, ComponentTypeID typeID, String componentName);

    /**
     * Create a new Service Component Definition instance with a specified configuration
     * @param id the ID for this object (may not be null).
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     *
     * @see createServiceComponentDefn(Configuration, ComponentTypeID, String)
     */
    ServiceComponentDefn createServiceComponentDefn(ConfigurationID configurationID, ComponentTypeID typeID, String componentName);

    /**
     * Create a new Service Component Definition instance with a specified configuration
     * @param id the ID for this object (may not be null).
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @param routingUUID the globally unique routing UUID of the Service Component Defn
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     *
     * @see createServiceComponentDefn(Configuration, ComponentTypeID, String)
     */
    ServiceComponentDefn createServiceComponentDefn(ConfigurationID configurationID, ComponentTypeID typeID, String componentName, String routingUUID);

    /**
     * Create a new Service Component Definition instance with a specified configuration
     * @param configuration that will contain the new created component definition
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ComponentTypeID typeID, String componentName);

    /**
     * Create a new Service Component Definition instance with a specified configuration
     * @param configuration that will contain the new created component definition
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @param routingUUID the globally unique routing UUID of the Service Component Defn
     * @return the Component instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ComponentTypeID typeID, String componentName, String routingUUID);

    /**
     * Create a new ResourceDescriptor Component Definition instance with a specified configuration
     * @param configurationID that the new created component definition will be a part of
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the ResourceDescriptor instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    ResourceDescriptor createResourceDescriptor(ConfigurationID configurationID, ComponentTypeID typeID, String descriptorName);


    /**
     * Create a new ResourceDescriptor Component Definition instance with a specified configuration
     * @param configuration that will contain the new created component definition
     * @param typeID is the type of component definition to create
     * @param componentName is the name of the component
     * @return the ResourceDescriptor instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    ResourceDescriptor createResourceDescriptor(Configuration configuration, ComponentTypeID typeID, String descriptorName);

    /**
     * Create a new SharedResource  instance 
     * @param typeID is the type of component definition to create
     * @param resourceName is the name of the shared resource
     * @return the ResourceDescriptor instance with the specified ID.
     * @throws IllegalArgumentException if either of the ID or data source ID is null
     */
    SharedResource createSharedResource(ComponentTypeID typeID, String resourceName) ;

 
    /**
     * Create a new DeployedComponent instance respresenting a deployed Service.
     * This method has the advantage of allowing the creation of
     * a deployed component entirely with ID objects, without needing any
     * of the full objects themselves.  However, it is the responsibility of
     * the client that the ComponentTypeID parameter and the ServiceComponentDefnID
     * parameter both indicate the same service type.  Also, the local
     * Configuration object represented by the ConfigurationID will <i>not</i>
     * be updated.
     * @param instanceName is the name assigned to this instance
     * @param configurationID the configuration the vm is deployed within.
     * @param hostId the host the vm is deployed in.
     * @param vmId the VM that the service is deployed in.
     * @param serviceComponentDefnID is the service component definition to be deployed
     * @param serviceComponentTypeID is type of the service to be deployed
     * @return the DeployedComponent instance with the specified ID.
     * @throws IllegalArgumentException if either of the IDs  null
     *
     * @see createDeployedServiceComponent(String, Configuration, HostID, VMComponentDefn)
     */
    DeployedComponent createDeployedServiceComponent(String instanceName, ConfigurationID configurationID, HostID hostId, VMComponentDefnID vmId, ServiceComponentDefnID serviceComponentDefnID, ComponentTypeID serviceComponentTypeID);

    /**
     * Deploys a ServiceComponentDefn to the specified VM.
     * This method is harmless to call if the
     * ServiceComponentDefn is already deployed.  It is
     * also harmless to call if the VM has not been started at
     * all.   A Collection of any newly-created
     * DeployedComponent objects is returned.
     * @param configuration must be the Configuration containing both
     * the ServiceComponentDefn and VM ID parameters (but this is not
     * checked for in this method)
     * @param serviceComponentDefn to be deployed
     * @param vmID VMComponentDefn ID that may already be deployed somewhere in the
     * Configuration parameter
     * @return DeployedComponent of newly-created DeployedComponent object
     */
    DeployedComponent deployServiceDefn(Configuration configuration, ServiceComponentDefn serviceComponentDefn, VMComponentDefnID vmID);
    
    // ----------------------------------------------------------------------------------
    //                  M O D I F I C A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Sets this ServiceComponentDefn's String routing UUID.  This method
     * will modify the ServiceComponentDefn parameter, and also create the
     * action to set the routing UUID at the remote server.
     * @param serviceComponentDefn ServiceComponentDefn to have it's routing
     * UUID modified - this instance will be locally modified, and an action
     * will also be created for execution as a transaction later on
     * @param newRoutingUUID new String routing UUID for the indicated
     * ServiceComponentDefn
     */
    void setRoutingUUID(ServiceComponentDefn serviceComponentDefn, String newRoutingUUID);

    /**
     * <p>Sets whether the indicated ServiceComponentDefn is enabled for starting
     * (when the PSC which contains it is
     * {@link #deployProductServiceConfig deployed}) or not.  This method
     * can also either automatically create the necessary
     * {@link DeployedComponent DeployedComponents} (if the service
     * is being enabled) or (optionally) delete the related
     * DeployedComponents (if the service defn is being disabled).</p>
     *
     * If the {@link ServiceComponentDefn ServiceComponentDefn} is being
     * <i>enabled</i>:
     * <ul>
     * <li>This method will set its <code>enabled</code> attribute to
     * true.</li>
     * <li>This method will check to see if the ProductServiceConfig
     * {@link ProductServiceConfig PSC} has been deployed on any vms, and, if
     * so, will automatically create a DeployedComponent object for this
     * newly-enabled ServiceComponentDefn for each different VM the psc is deployed;
     * these objects will be returned in the returned Collection</li>
     * </ul></p>
     *
     * If the ServiceComponentDefn is being <i>disabled</i>:
     * <ul>
     * <li>This method will set its <code>enabled</code> attribute to
     * false.</li>
 	 *<li>If the deleteDeployedComps parameter is true, and the
     * ServiceComponentDefn has been deployed anywhere, all of those
     * {@link DeployedComponent DeployedComponents} will be deleted
     * from the Configuration; these same objects will be returned in the
     * returned Collection. </li>
     * <li>If the deleteDeployedComps is false, and the ServiceComponentDefn
     * has been deployed anywhere, a ConfigurationException will be thrown,
     * indicating that the ServiceComponentDefn cannot be deleted while it
     * has dependant DeployedComponents that exist.</li>
     * </ul></p>
     *
     * <p><b>Note:</b> This method will do nothing and return an
     * empty Collection if the <code>enabled</code> parameter supplied is
     * already the same as the enabled status of the ServiceComponentDefn
     * parameter.  This prevents unnecessary work for nothing.</p>
     *
     * @param configuration Configuration which the ServiceComponentDefn
     * belongs to.  This object will be updated with the changed
     * ServiceComponentDefn, and if any DeployedComponent objects are deleted.
     * @param deployedComponent to have its enabled attribute set; it
     * must belong to the indicated Configuration parameter.  This object
     * will have it's <code>enabled</code> field updated.
     * @param enabled whether this service definition should be enabled for
     * deployment or not.
     * @throws ConfigurationException if <code>false</code> was passed in for
     * the deleteDeployedComps parameter, and any
     * {@link DeployedComponent DeployedComponents} exist for the
     * ServiceComponentDefn parameter.
     *
     * @see #deployProductServiceConfig deployProductServiceConfig
     */

//
//    DeployedComponent setEnabled(Configuration configuration, ServiceComponentDefn serviceComponentDefn, VMComponentDefn vm, boolean enabled)
//    throws ConfigurationException;


    /**
     * It updates the DeployedComponent by setting its enabled flag.
     */
    DeployedComponent setEnabled(DeployedComponent deployComponent, boolean enabled);


    /**
     * This method will update a PSC by adding the new service list of ID's and removing
     * the service ID's from the PSC that are not in the service list.
     * @param config
     * @param psc
     * @param newServiceIDList is the new list of ID's that the psc will contain
     * @return updated ProductServiceConfig
     * @throws ConfigurationException
     */
//    ProductServiceConfig updateProductServiceConfig(Configuration config, ProductServiceConfig psc, Collection newServiceIDList)
 //        throws ConfigurationException;


    /**
     * Adds an existing ServiceComponentDefn to indicated PSC; the
     * ServiceComponentDefn is assumed to already belong to the indicated
     * Configuration.  The ServiceComponentDefn will be removed from
     * any PSC it previously belonged to.
     * @param configuration the Configuration containing the
     * ServiceComponentDefn
     * @param psc ProductServiceConfig to have service comp defn added to
     * @param serviceComponentDefnID will be added to the indicated
     * ProductServiceConfiguration (and removed from any PSC it previously
     * belonged to).
     */
//    ProductServiceConfig addServiceComponentDefn(Configuration configuration, ProductServiceConfig psc, ServiceComponentDefnID serviceComponentDefnID);

    /**
     * Adds an existing ServiceComponentDefn to indicated PSC.
     * The ServiceComponentDefn will be removed from
     * any PSC it previously belonged to.
     * @param psc ProductServiceConfig to have service comp defn added to
     * @param serviceComponentDefnID will be added to the indicated
     * ProductServiceConfiguration (and removed from any PSC it previously
     * belonged to).
     */
 //   ProductServiceConfig addServiceComponentDefn(ProductServiceConfig psc, ServiceComponentDefnID serviceComponentDefnID);

    /**
     * Deletes the ServiceComponentDefn from the indicated PSC, and from the
     * entire Configuration
     * @param configuration the ServiceComponentDefn is removed from this object
     * @param psc ProductServiceConfig to have service comp defn deleted from
     * @param serviceComponentDefnID will be deleted from the Configuration
     * @return updated ProductServiceConfig
     */
//    ProductServiceConfig removeServiceComponentDefn(Configuration configuration, ProductServiceConfig psc, ServiceComponentDefnID serviceComponentDefnID);

    /**
     * Adds the service type represented by the indicated ComponentType to
     * the list of legal service types of the indicated ProductType.
     * @param productType ProductType to have a new service type added to
     * @param serviceComponentType ComponentType to be added to the
     * ProductType
     */
 //   ProductType addServiceComponentType(ProductType productType, ComponentType serviceComponentType);

    /**
     * Removes the service type represented by the indicated ComponentType from
     * the list of legal service types of the indicated ProductType.
     * @param productType ProductType to have the service type taken from
     * @param serviceComponentType ComponentType to be taken from the
     * ProductType
     */
 //   ProductType removeServiceComponentType(ProductType productType, ComponentType serviceComponentType);

    /**
     * Returns a modifiable properties object for the specified ComponentObject.
     * If calling directly on the component object to obtain the properties, an
     * unmodifiable properties objects will be returned.
     * @param t is the component object to obtain the properties from
     * @return modifiable Properties
     */
    Properties getEditableProperties(ComponentObject t);

    /**
     * Change the name of a previously defined PSC.
     * @param psc The psc whose name to change.
     * @param name The new name.
     * @return The PSC with its name changed.
     */
//    ProductServiceConfig renamePSC(ProductServiceConfig psc, String name) throws ConfigurationException;

    /**
     * Change the name of a previously defined VM.
     * @param vm The VM whose name to change.
     * @param name The new name.
     * @return The VM with its name changed.
     */
    VMComponentDefn renameVM(VMComponentDefn vm, String name) throws ConfigurationException;

    // Configuration methods
    Configuration setIsReleased( Configuration t, boolean newValue );

    Configuration setIsDeployed( Configuration t, boolean newValue );

// ComponentObject methods

    /**
     * Set the property value for the specified name on the <code>ComponentObject</code>.
     * This method will replace the current property by the same name.
     * @param t is the <code>ComponentObject</code> to set the value on
     * @param name is the name of the property
     * @param value is the value for the property
     * @return updated <code>ComponentObject</code>
     */
    ComponentObject setProperty( ComponentObject t,  String name, String value );

    /**
     * Adds the property value for the specified name on the <code>ComponentObject</code>.
     * This method assumes the named property does not exist.
     * @param t is the <code>ComponentObject</code> to set the value on
     * @param name is the name of the property
     * @param value is the value for the property
     * @return updated <code>ComponentObject</code>
     */
    ComponentObject addProperty( ComponentObject t,  String name, String value );

    /**
     * Removes the property value for the specified name on the <code>ComponentObject</code>.
     * @param t is the <code>ComponentObject</code> to set the value on
     * @param name is the name of the property
     * @param value is the value for the property
     * @return updated <code>ComponentObject</code>
     */
    ComponentObject removeProperty( ComponentObject t,  String name );


    /**
     * call modifyProperties
     * @param t is the <code>ComponentObject</code> to set the value on
     * @param props are the properties that will be used based on the command
     * @param command the object editor command (ADD, REMOVE, or SET) that defines the operation to be
     * performed.
     * @return updated <code>ComponentObject</code>
     */
    ComponentObject modifyProperties( ComponentObject t, Properties props, int command );

    /**
     * Updates the target <code>ComponentTypeDefn</code> with the updated <code>PropertyDefinition</code>.
     * @param original is the original ComponentTypeDefn to be updated.
     * @param updated is the updated ComponentTypeDefn to replace the original.
     * @param defn is the PropertyDefinition
     * @return an updated ComponentTypeDefn
     */
    ComponentTypeDefn modifyComponentTypeDefn(ComponentTypeDefn original, ComponentTypeDefn updated );

    /**
     *   Sets the parent component type, null is allowed;
     *   @param t is the ComponentType
     *   @param parentID is the parent component type (null allowed)
     *   @return ComponentType updated
     */
    ComponentType setParentComponentTypeID(ComponentType t, ComponentTypeID parentID);

    /**
     *   Sets the super component type, null is allowed;
     *   @param t is the ComponentType
     *   @param superID is the super component type (null allowed)
     *   @return ComponentType updated
     */
    ComponentType setSuperComponentTypeID(ComponentType t, ComponentTypeID superID);
    ComponentType setIsDeployable( ComponentType t, boolean newValue );
    ComponentType setIsDeprecated( ComponentType t, boolean newValue );
    ComponentType setIsMonitored( ComponentType t, boolean newValue );
    
    ComponentType setLastChangedHistory(ComponentType type, String lastChangedBy, String lastChangedDate);
    ComponentType setCreationChangedHistory(ComponentType type, String createdBy, String creationDate);    


    ComponentObject setLastChangedHistory(ComponentObject compObject, String lastChangedBy, String lastChangedDate);
    ComponentObject setCreationChangedHistory(ComponentObject compObject, String createdBy, String creationDate);    


    /**
     * Sets the component type on the specifice component object.
     * @param t is the ComponentObject to have its type set
     * @param componentType is the ComponentType to set
     * @return ComponentObject that had its type set
     */
    ComponentObject setComponentType(ComponentObject t, ComponentTypeID componentType);

    /**
     * Set the list of <code>ComponentTypeDefn</code>s for a local ComponentType
     * object.  This method will not generate actions because the definitions
     * should have been created using the
     * {@link #createComponentTypeDefn(ComponentTypeID, PropertyDefinition, boolean) createComponentTypeDefn}
     * method, which would set the ComponentType properly on the remote system;
     * it is intended only to modify a <i>local</i> copy of a ComponentType object.
     * @param t is the ComponentType to set on
     * @param defns Collection of new definitions
     * @return ComponentType that is modified
     * @see #createComponentTypeDefn(ComponentTypeID, PropertyDefinition, boolean)
     */
    ComponentType setComponentTypeDefinitions(ComponentType t, Collection defns);

    /**
     * Set the <code>PropertyDefinition</code> on the <code>ComponentTypeDefn</code>
     * @param t is the component type definition to update with the defintion
     * @param defn is the property defn that will be applied.
     * @return updated ComponentTypeDefn
     */
    ComponentTypeDefn setPropertyDefinition(ComponentTypeDefn t, PropertyDefinition defn);


    // ----------------------------------------------------------------------------------
    //                  D E L E T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    /**
     * Call to delete a component type and all its definitions.
     * @param target ComponentType and all its ComponentTypeDefns to delete
     * @throws ConfigurationException if unable to delete the ComponentType
     * @throws IllegalArgumentException if target is null
     */
    void delete(ComponentType target) throws ConfigurationException;

    /**
     * Call to delete a host
     * @param target Host to delete
     * @throws ConfigurationException if unable to delete the host
     * @throws IllegalArgumentException if target is null
     */
    void delete(Host target) throws ConfigurationException;

    /**
     * Call to delete a ComponentTypeDefn and remove it from the ComponentType.
     * The type will changed by having the target ComponentTypeDefn removed.
     * @param target is the ComponentTypeDefn to delete
     * @param type is the ComponentType that it should be removed from.
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    void delete(ComponentTypeDefn target, ComponentType type) throws ConfigurationException;

    /**
     * Call to delete a configuration, which is assumed to have no dependencies.
     * @see delete(Configuration, boolean) to delete a configuration and all its
     * dependencies.
     * @param target Configuration to delete
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    void delete(Configuration target) throws ConfigurationException;
    
    
    /**
     * Call to delete a configuration and all its dependencies.
     * @param targetID ConfigurationID to delete
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    void delete(ConfigurationID targetID) throws ConfigurationException;
    

    /**
     * Deletes the configuration if there exist no dependent objects.  To delete
     * the configuration and all its dependencies, pass the boolean argument of true.
     * @param target configuration to be deleted.
     * @param boolean true will also delete all other objects that depend on the target object
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    void delete(Configuration target, boolean deleteDependencies)  throws ConfigurationException;


    /**
     * Deletes the shared resource.
     * @param target configuration to be deleted.
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     * @returns SharedResource that was deleted
     */
    SharedResource delete( SharedResource target ) throws ConfigurationException;

    /**
     * Removes the component object from the specified configuration if no dependent
     * objects exist and returns an updated configuration.
     * @param target component object to be removed
     * @param configuration that the object is to be removed from
     * @return Configuration with the target object removed
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    Configuration delete( ComponentObject target, Configuration configuration ) throws ConfigurationException;

    /**
     * Removes the component object from the specified configuration if no dependent
     * objects exist.  If deleteDependencies is true, then the component object and
     * all dependencies will be deleted.
     * @param target component object to be removed.
     * @param configuration that the object is to be removed from
     * @param boolean true will delete all dependent objects
     * @return Configuration with the target object removed
     * @throws ConfigurationException if unable to delete the configuration
     * @throws IllegalArgumentException if target is null
     */
    Configuration delete( ComponentObject target, Configuration configuration,  boolean deleteDependencies ) throws ConfigurationException;

    // ----------------------------------------------------------------------------------
    //      H E L P E R    M E T H O D S     F O R    T H E  CDK
    // ----------------------------------------------------------------------------------

	/**
	 * This method will create a connector component based on the specific component type ID
     * @param typeID is the component type of the definition
     * @param componentName is the name of the definition
     * @return ServiceComponentDefn
     */
	ConnectorBinding createConnectorComponent(ConfigurationID configuID, ComponentTypeID typeID, String descriptorName, String routingUUID);

	/**
	 * This method will create a connector component from the original connector and give
	 * a new name.
     * @param configurationID Identifies which configuration this belongs to
     * @param original is the connector to copy the new connector from
     * @param newName is the name to be given to the new connector
     * @param routingUUID is the routingUUID to use in the copied connector
     * @return ConnectorBinding
     */
    ConnectorBinding createConnectorComponent(ConfigurationID configurationID, ConnectorBinding original, String newName, String routingUUID);


    /**
     * Creates a new ConnectorBinding for a given Configuration and
     * ProductServiceConfig; also automatically "deploys" the service anywhere
     * that this PSC is already deployed, by creating the necessary
     * DeployedComponents (this will only work if the PSC parameter belongs
     * to the Configuration parameter).
     * @param configuration the Configuration containing the PSC; this will
     * have the new ServiceComponentDefn added to it, plus any DeployedComponents
     * created for the ServiceComponentDefn
     * @param typeID type of the new ServiceComponentDefn
     * @param componentName name for the new ServiceComponentDefn
     * @param pscID ID of the ProductServiceConfig which this ServiceComponentDefn
     * will belong to
     * @return new ConnectorBinding
     */
 //   ConnectorBinding createConnectorComponent(Configuration configuration, ComponentTypeID typeID, String componentName, ProductServiceConfigID pscID) ;

    void addAuthenticationProvider(Configuration configuration, AuthenticationProvider provider);

    /**
	 * This method will create a provider component based on the specific component type ID
     * @param typeID is the component type of the definition
     * @param componentName is the name of the definition
     * @return ServiceComponentDefn
     */
	AuthenticationProvider createAuthenticationProviderComponent(ConfigurationID configuID, ComponentTypeID typeID, String descriptorName);

	/**
	 * This method will create a provider component from the original provider and give
	 * a new name.
     * @param configurationID Identifies which configuration this belongs to
     * @param original is the connector to copy the new provider from
     * @param newName is the name to be given to the new provider
     * @return AuthenticationProvider
     */
	AuthenticationProvider createAuthenticationProviderComponent(ConfigurationID configurationID, AuthenticationProvider original, String newName);

	/**
	 * This method will create a provider component from the original provider and give
	 * a new name.
     * @param configurationID Identifies which configuration this belongs to
     * @param original is the connector to copy the new provider from
     * @param newName is the name to be given to the new provider
     * @return AuthenticationProvider
     */
	AuthenticationProvider createAuthenticationProviderComponent(Configuration configuration, AuthenticationProvider original, String newName);

	/**
     * This method is used by the CDK so that it can create service component definitions
     * without having to assign it to a configuration.  The step of assignment will occur
     * when the actions created from this method are imported via the console.
     *
     * @param typeID is the component type of the definition
     * @param componentName is the name of the definition
     * @return ServiceComponentDefn
     */
//    ServiceComponentDefn createServiceComponentDefn(ComponentTypeID typeID, String componentName);

    /**
     * This method is used by the CDK so that it can create service component definitions
     * without having to assign it to a configuration.  The step of assignment will occur
     * when the actions created from this method are imported via the console.
     *
     * @param typeID is the component type of the definition
     * @param componentName is the name of the definition
     * @param routingUUID globally unique routing UUID of the ServiceComponentDefn
     * @return ServiceComponentDefn
     */
//    ServiceComponentDefn createServiceComponentDefn(ComponentTypeID typeID, String componentName, String routingUUID);

}



