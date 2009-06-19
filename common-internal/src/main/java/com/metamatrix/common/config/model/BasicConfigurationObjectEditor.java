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

package com.metamatrix.common.config.model;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.actions.AbstractObjectEditor;
import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.AddObject;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.actions.ExchangeBoolean;
import com.metamatrix.common.actions.ExchangeObject;
import com.metamatrix.common.actions.RemoveObject;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentObjectID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.PropDefnAllowedValue;
import com.metamatrix.common.config.api.PropDefnAllowedValueID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.RuntimeMetadataServiceComponentType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.namedobject.BaseObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.core.id.UUIDFactory;

/**
*   BasicConfigurationObjectEditor provides the update capabiltiy to the implemented versions
*   of the config.api package.
*
*
*   NOTE: - in all the methods that take a non-mutable object and returns it,
*           a clone of the object is returned instead of the passed object.
*           This is done so that any future change to the object is not
*           made to the same instance that has a prior change (action) waiting to
*           be executed.
*/

public class BasicConfigurationObjectEditor extends AbstractObjectEditor implements ConfigurationObjectEditor {

    private final static long serialVersionUID = -51562836234358446L; 
    private ObjectIDFactory factory = null;

    /**
     * Lazy getter for private ObjectIDFactory member
     */
    private ObjectIDFactory getObjectIDFactory(){
        if (factory == null){
            factory = new UUIDFactory();
        }
        return factory;
    }


	/**
     * Create an instance of this editor, and specify whether actions are to be created
     * during modifications.  If actions are created, then each action is sent directly
     * to the destination at the time the action is created.
     * @param createActions flag specifying whether modification actions should be created
     * for each invocation to <code>modifyObject</code>
     * @throws IllegalArgumentException if the defaults reference is null
     */
    public BasicConfigurationObjectEditor(boolean createActions ) {
        super(createActions);

        if ( doCreateActions() ) {
            this.setDestination(new com.metamatrix.common.actions.BasicModificationActionQueue());
        }
    }
    /**
     * Create an instance of this editor that does not create any actions.
     * @throws IllegalArgumentException if the defaults reference is null
     */
    public BasicConfigurationObjectEditor( ) {
        this(false);
    }

    /**
     * Modify the specified target object by undoing the specified action.
     * <p>
     * In general, the client user of this class should (prior to invoking this
     * method) obtain the target MetadataID from the action and find the corresponding
     * Component, and pass the resulting Component in as the target argument.
     * In the case of a DestroyObject action being undone, the corresponding
     * Component may not exist in the client's universe - it has already been
     * destroyed; passing a null as the target argument in this case is acceptable.
     * For all other actions, the target argument may not be null.
     * <p>
     * The result of this object will be the target after being modified by
     * the specified action.  In the case of a CreateObject action being undone,
     * this method 'destroys' the object, so a null result is returned; the
     * client should ensure this object is removed from its context.
     * In all other cases, this method returns the modified target.
     * @param action the action to be undone (may not be null).
     * @param target the Component that is the target of the action.
     * @return the modified Component, or null if the object is to be
     * removed as a result of undoing the action.
     * @throws IllegalArgumentException if the action reference is null, or
     * if the target is null and the action is not a DestroyObject.
     */
    public ComponentObject undoAction( ActionDefinition action, ComponentObject target ) {
        if ( action == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0088));
        }
        ComponentDefn result = null;

        if ( action instanceof CreateObject ) {
            return result;
        } else if ( action instanceof DestroyObject ) {
            Object args[] = action.getArguments();
            result = (ComponentDefn) args[0];
        } else {
            result = (ComponentDefn) target;
        }

        return result;
    }

    /**
     * Modify the specified target object by undoing the specified action.
     * <p>
     * In general, the client user of this class should (prior to invoking this
     * method) obtain the target ComponentID from the action and find the corresponding
     * Component, and pass the resulting Component in as the target argument.
     * In the case of a DestroyObject action being undone, the corresponding
     * Component may not exist in the client's universe - it has already been
     * destroyed; passing a null as the target argument in this case is acceptable.
     * For all other actions, the target argument may not be null.
     * <p>
     * The result of this object will be the target after being modified by
     * the specified action.  In the case of a CreateObject action being undone,
     * this method 'destroys' the object, so a null result is returned; the
     * client should ensure this object is removed from its context.
     * In all other cases, this method returns the modified target.
     * @param action the action to be undone (may not be null).
     * @param target the DeployedComponent that is the target of the action.
     * @return the modified DeployedComponent, or null if the object is to be
     * removed as a result of undoing the action.
     * @throws IllegalArgumentException if the action reference is null, or
     * if the target is null and the action is not a DestroyObject.
     */
    DeployedComponent undoAction( ActionDefinition action, DeployedComponent target ) {
        if ( action == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0088));
        }
        DeployedComponent result = null;

        if ( action instanceof CreateObject ) {
            return result;
        } else if ( action instanceof DestroyObject ) {

        }

        return result;

    }

    public Configuration undoAction( ActionDefinition action, BaseObject target ) {
        if ( action == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0088));
        }
        Configuration result = null;

        if ( action instanceof CreateObject ) {
            return result;
        } else if ( action instanceof DestroyObject ) {
            Object args[] = action.getArguments();
            result = (Configuration) args[0];
        } else {
            result = (Configuration) target;
            if ( action instanceof ExchangeObject ) {

                ExchangeObject anAction = (ExchangeObject) action;

                if ( target instanceof BasicConfiguration ) {
//                    BasicConfiguration config = (BasicConfiguration) target;
//                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
//                        config.setName((String)anAction.getPreviousValue());
//                    }

                } else if ( target instanceof BasicServiceComponentDefn ) {
                    BasicServiceComponentDefn bc = (BasicServiceComponentDefn) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        bc.setProperties((Properties)anAction.getPreviousValue());
/*                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
//                        bc.set((String)anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME_IN_SOURCE.getCode()) {
                        de.setNameInSource((String)anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATA_RECORD_TYPE.getCode()) {
                        de.setDataRecordTypeID((DataRecordTypeID)anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATA_VALUE_TYPE.getCode()) {
                        de.setDataValueTypeID((DataValueTypeID)anAction.getPreviousValue());
*/                    }

                } else if ( target instanceof BasicVMComponentDefn ) {
                    BasicVMComponentDefn dg = (BasicVMComponentDefn) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        dg.setProperties((Properties)anAction.getPreviousValue());
/*                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.KEYWORDS.getCode()) {
                        dg.setKeywords((Collection)anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME_IN_SOURCE.getCode()) {
                        dg.setNameInSource((String)anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATABASE.getCode()) {
                        dg.setDatabaseID((DatabaseID)anAction.getPreviousValue());
*/                    }
                } else if ( target instanceof BasicDeployedComponent ) {
//                    BasicDeployedComponent key = (BasicDeployedComponent) target;
//                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATA_ELEMENTS.getCode()) {
//                        key.setDataElements((List)anAction.getPreviousValue());
//                    }
                } else if ( target instanceof BasicHost ) {
//                    BasicHost key = (BasicHost) target;
//                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATA_ELEMENTS.getCode()) {
//                        key.setDataElements((List)anAction.getPreviousValue());
//                    }
                } else if ( target instanceof BasicConfigurationInfo ) {
//                    BasicConfigurationInfo role = (BasicConfigurationInfo) target;
//                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ID.getCode()) {
//                        role.setKeyID((KeyID)anAction.getPreviousValue());
//                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.DATA_GROUP.getCode()) {
//                        role.setDataGroupID((DataGroupID)anAction.getPreviousValue());
//                    }
                }
            } else if ( action instanceof AddObject ) {
//                AddObject anAction = (AddObject) action;


/*
                if ( target instanceof BasicDataCategory ) {
                    BasicDataCategory dc = (BasicDataCategory) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( dc.getProperties() );
                        Properties removed = (Properties) anAction.getArguments()[0];
                        Enumeration enum = removed.propertyNames();
                        while ( enum.hasMoreElements() ) {
                            p.remove(enum.nextElement());
                        }
                        dc.setProperties(p);
                    }
                } else if ( target instanceof BasicDataElement ) {
                    BasicDataElement de = (BasicDataElement) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( de.getProperties() );
                        Properties removed = (Properties) anAction.getArguments()[0];
                        Enumeration enum = removed.propertyNames();
                        while ( enum.hasMoreElements() ) {
                            p.remove(enum.nextElement());
                        }
                        de.setProperties(p);
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.KEYWORDS.getCode()) {
                        Set keywords = (Set) anAction.getArguments()[0];
                        Set existing = de.getKeywords();
                        existing.removeAll(keywords);
                        de.setKeywords(existing);
                    }
                } else if ( target instanceof BasicDataGroup ) {
                    BasicDataGroup dg = (BasicDataGroup) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( dg.getProperties() );
                        Properties removed = (Properties) anAction.getArguments()[0];
                        Enumeration enum = removed.propertyNames();
                        while ( enum.hasMoreElements() ) {
                            p.remove(enum.nextElement());
                        }
                        dg.setProperties(p);
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.KEYWORDS.getCode()) {
                        Set keywords = (Set) anAction.getArguments()[0];
                        Set existing = dg.getKeywords();
                        existing.removeAll(keywords);
                        dg.setKeywords(existing);
                    }
                }
 */

            } else if ( action instanceof RemoveObject ) {
//                RemoveObject anAction = (RemoveObject) action;
                if ( target instanceof BasicConfiguration ) {
                } else if ( target instanceof BasicServiceComponentDefn ) {
                } else if ( target instanceof BasicVMComponentDefn )  {
                } else if ( target instanceof BasicDeployedComponent ) {
                } else if ( target instanceof BasicHost ) {
                } else if ( target instanceof BasicConfigurationInfo ) {

               }
/*
                if ( target instanceof BasicDataCategory ) {
                    BasicDataCategory dc = (BasicDataCategory) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( dc.getProperties() );
                        p.putAll( (Properties) anAction.getArguments()[0] );
                        dc.setProperties(p);
                    }
                } else if ( target instanceof BasicDataElement ) {
                    BasicDataElement de = (BasicDataElement) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( de.getProperties() );
                        p.putAll( (Properties) anAction.getArguments()[0] );
                        de.setProperties(p);
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.KEYWORDS.getCode()) {
                        Set keywords = (Set) anAction.getArguments()[0];
                        Set existing = de.getKeywords();
                        existing.addAll(keywords);
                        de.setKeywords(existing);
                    }
                } else if ( target instanceof BasicDataGroup ) {
                    BasicDataGroup dg = (BasicDataGroup) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode()) {
                        Properties p = new Properties();
                        p.putAll( dg.getProperties() );
                        p.putAll( (Properties) anAction.getArguments()[0] );
                        dg.setProperties(p);
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.KEYWORDS.getCode()) {
                        Set keywords = (Set) anAction.getArguments()[0];
                        Set existing = dg.getKeywords();
                        existing.addAll(keywords);
                        dg.setKeywords(existing);
                    }
                }
*/
            } else if ( action instanceof ExchangeBoolean ) {
                ExchangeBoolean anAction = (ExchangeBoolean) action;
                if ( target instanceof BasicConfiguration ) {
                    BasicConfiguration config = (BasicConfiguration) target;
                    if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_RELEASED.getCode()) {
                        config.setIsRelease(anAction.getPreviousValue());
                    } else if (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_DEPLOYED.getCode()) {
                        config.setIsDeployed(anAction.getPreviousValue());
                    }
                }

            }

        }

        return result;
    }



    // ----------------------------------------------------------------------------------
    //                  C R E A T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    public Host createHost(String hostName ) {

        if (hostName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0077));
        }

//        HostID id = new HostID(hostName);

        ComponentDefn bh = BasicUtil.createComponentDefn(ComponentDefn.HOST_COMPONENT_CODE, Configuration.NEXT_STARTUP_ID, Host.HOST_COMPONENT_TYPE_ID , hostName);
//        BasicHost bh = new BasicHost(Configuration.NEXT_STARTUP_ID, id, Host.HOST_COMPONENT_TYPE_ID);
        this.createCreationAction(bh.getID(), bh);

        Host newHost = (Host) bh.clone();
        return newHost;
    }
    
    public Host createHost(ConfigurationID configurationID, String hostName ) {

        if (hostName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0077));
        }
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
        }

//        HostID id = new HostID(hostName);

//        BasicHost bh = new BasicHost((ConfigurationID) configuration.getID(), id, Host.HOST_COMPONENT_TYPE_ID);

        ComponentDefn bh = BasicUtil.createComponentDefn(ComponentDefn.HOST_COMPONENT_CODE, configurationID, Host.HOST_COMPONENT_TYPE_ID , hostName);
        
        this.createCreationAction(bh.getID(), bh);

        Host newHost = (Host) bh.clone();

        return newHost;
    }
    

   public Host createHost(Configuration configuration, String hostName ) {

        if (hostName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0077));
        }
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
        }

//        HostID id = new HostID(hostName);

//        BasicHost bh = new BasicHost((ConfigurationID) configuration.getID(), id, Host.HOST_COMPONENT_TYPE_ID);

        ComponentDefn bh = BasicUtil.createComponentDefn(ComponentDefn.HOST_COMPONENT_CODE, (ConfigurationID) configuration.getID(), Host.HOST_COMPONENT_TYPE_ID , hostName);
        
        this.createCreationAction(bh.getID(), bh);

        Host newHost = (Host) bh.clone();


        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addHost(newHost);

        return newHost;
    }
   
   

    public Host createHost(Configuration configuration, Host original, String newName) {
        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0077));
        }
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
        }

        if (original == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0080, newName));
        }

//        HostID id = new HostID(newName);
//        BasicHost bh = new BasicHost((ConfigurationID) configuration.getID(),id, Host.HOST_COMPONENT_TYPE_ID);

 //       this.createCreationAction(id, bh);
        
        ComponentDefn bh = BasicUtil.createComponentDefn(ComponentDefn.HOST_COMPONENT_CODE, (ConfigurationID) configuration.getID(), Host.HOST_COMPONENT_TYPE_ID , newName);
        
        this.createCreationAction(bh.getID(), bh);
        

        this.modifyProperties(bh, original.getProperties(), ADD);

        Host newHost = (Host) bh.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addHost(newHost);

        return newHost;
    }



    public Configuration createConfiguration(String configurationName) {
        if (configurationName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0081));
        }
        ConfigurationID id = null;
        id = new ConfigurationID(configurationName);

        BasicConfigurationInfo info = new BasicConfigurationInfo(id);
        BasicConfiguration config = new BasicConfiguration(info, Configuration.CONFIG_COMPONENT_TYPE_ID);
        this.createCreationAction(id, config);

        Configuration newConfig = (Configuration) config.clone();
        return newConfig;
    }

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
    public Configuration createConfiguration(String configurationName, Date creationDate, Date lastChangedDate) {
        if (configurationName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0081));
        }
        ConfigurationID id = null;
        id = new ConfigurationID(configurationName);

        BasicConfigurationInfo info = new BasicConfigurationInfo(id);
        info.setCreationDate(creationDate);
        info.setLastChangedDate(lastChangedDate);
        BasicConfiguration config = new BasicConfiguration(info, Configuration.CONFIG_COMPONENT_TYPE_ID);
        this.createCreationAction(id, config);

        Configuration newConfig = (Configuration) config.clone();
        return newConfig;
    }


	public void createConfiguration(ConfigurationID configID, Collection configObjects) {
        if (configID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0082));
        }
        if (configObjects == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0083));
        }

        if (configObjects.isEmpty() ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0083));
        }

        this.createCreationAction(configID, configObjects);


	}


    /**
     * <p>Copy-create method - creates a deep-copy of the original configuration,
     * with the new name.  The new name cannot be the same as the original
     * configuration's name.</p>
     *
     * <p>All service component definitions, all deployed components, and all
     * property values will also be copied.  The copies will all retain their original
     * short names; their full names will reflect the new configuration name.</p>
     */
//    public Configuration createConfiguration(Configuration original, String newName){
//        if (original == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
//        }
//        if (newName == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0081));
//        }
//        if (original.getName().equals(newName) ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0084, newName));
//        }
//
// 		BasicConfiguration config = ( BasicConfiguration) createConfiguration(newName);
// 		
//
//
//        //copy the configuration properties
//        this.modifyProperties(config, original.getProperties(), ADD);
//
//        if (original.getHosts() != null) {
//            for (Iterator hIt=original.getHosts().iterator(); hIt.hasNext(); ) {
//                Host h = (Host) hIt.next();
//                this.createHost(config, h, h.getName());
//             }
//        }
//
//		Iterator svcDefns = original.getServiceComponentDefns().iterator();
//		while (svcDefns.hasNext()) {
//			ServiceComponentDefn sDefn = (ServiceComponentDefn) svcDefns.next();
//            this.createServiceComponentDefn(config, sDefn, sDefn.getName());
//		}
//
//		Iterator vmsDefns = original.getVMComponentDefns().iterator();
//		while (vmsDefns.hasNext()) {
//			VMComponentDefn vDefn = (VMComponentDefn) vmsDefns.next();
//            this.createVMComponentDefn(config, vDefn, vDefn.getName());
// 		}
//
//		Iterator bindings = original.getConnectorBindings().iterator();
//		while (bindings.hasNext()) {
//			ConnectorBinding cc = (ConnectorBinding) bindings.next();
//
//            this.createConnectorComponent(config, cc, cc.getName());
//
// 		}
//		
//		Iterator authProviders = original.getAuthenticationProviders().iterator();
//		while (authProviders.hasNext()) {
//			AuthenticationProvider provider = (AuthenticationProvider) authProviders.next();
//
//            this.createAuthenticationProviderComponent(config, provider, provider.getName());
//
// 		}
//
//
//        //copy all internal deployed components
//        ConfigurationID configID = (ConfigurationID)config.getID();
//        VMComponentDefnID vmID = null;
//        ProductServiceConfigID pscID = null;
//        DeployedComponent originalDeployed = null;
//        ServiceComponentDefn service = null;
////        VMComponentDefn vm = null;
//        for (Iterator iter=original.getDeployedComponents().iterator(); iter.hasNext(); ){
//            originalDeployed = (DeployedComponent)iter.next();
//			if (originalDeployed.isDeployedConnector()) {
//                ConnectorBindingID serviceID = new ConnectorBindingID(configID, originalDeployed.getServiceComponentDefnID().getName());
//                pscID     = new ProductServiceConfigID(configID, originalDeployed.getProductServiceConfigID().getName());
//                vmID      = new VMComponentDefnID(configID, originalDeployed.getHostID(),  originalDeployed.getVMComponentDefnID().getName());
//                service = config.getConnectorBinding(serviceID);
//
//                
//                if (service==null) {
//                	throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0086,
//                                new Object[] { serviceID, originalDeployed.getName()}));
//                }
//                this.createDeployedServiceComponent(originalDeployed.getName(), config, originalDeployed.getHostID(), vmID, service, pscID);
//
//
//			} else if (originalDeployed.isDeployedService()){
//
//                ServiceComponentDefnID serviceID = new ServiceComponentDefnID(configID, originalDeployed.getServiceComponentDefnID().getName());
//                pscID     = new ProductServiceConfigID(configID, originalDeployed.getProductServiceConfigID().getName());
//                vmID      = new VMComponentDefnID(configID, originalDeployed.getHostID(), originalDeployed.getVMComponentDefnID().getName());
//                service = config.getServiceComponentDefn(serviceID);
//
//                if (service==null) {
//                    throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0085,
//                                new Object[] { serviceID, originalDeployed.getName()}));
//                 }
//                this.createDeployedServiceComponent(originalDeployed.getName(), config, originalDeployed.getHostID(), vmID, service, pscID);
//            } 
////            else {
////                
////                this.createVMComponentDefn(config, originalDeployed.getHostID(), originalDeployed.getComponentTypeID(), originalDeployed.getName());
////                
////                vmID      = new VMComponentDefnID(configID, originalDeployed.getHostID(), originalDeployed.getVMComponentDefnID().getName());
////                vm = config.getVMComponentDefn(vmID);
////                this.createDeployedVMComponent(originalDeployed.getName(), config, originalDeployed.getHostID(), vm);
////            }
//        }
//
//        Configuration newConfig = (Configuration) config.clone();
//        return newConfig;
//    }

    public ComponentType createComponentType(int classTypeCode, String name, ComponentTypeID parentID, ComponentTypeID superID, boolean deployable, boolean monitored) {
        if (name == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ComponentType.class.getName()));
        }

//        ComponentTypeID typeID = new ComponentTypeID(name);

        BasicComponentType type = BasicUtil.createComponentType(classTypeCode, name, parentID, superID, deployable, monitored) ;
            //BasicComponentType.getInstance(classTypeCode, typeID, parentID, superID, deployable, monitored);

        createCreationAction(type.getID(), type);

        ComponentType newType = (ComponentType) type.clone();
        return newType;
    }
    
    public ComponentType createComponentType(ComponentType componentType, String name) {
        
        if (componentType == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }
        
        if (name == null) {
            name = componentType.getFullName();
        }
        BasicComponentType basicType = (BasicComponentType) verifyTargetClass(componentType,BasicComponentType.class);

        BasicComponentType newType = BasicUtil.createComponentType(basicType.getComponentTypeCode(), name, basicType.getParentComponentTypeID(),
                                          basicType.getSuperComponentTypeID(), basicType.isDeployable(), basicType.isMonitored());
        newType.setComponentTypeDefinitions(basicType.getComponentTypeDefinitions());
        
        createCreationAction(newType.getID(), newType);

        ComponentType nType = (ComponentType) newType.clone();
        return nType;
    }


    /**
     * Create a new ProductType instance with the specified name.
     * @param name is the name of the ProductType
     * @param serviceComponentTypeIDs Collection of ComponentType objects which
     * indicate the service types that belong to this product type
     * @param deployable is a boolean indicating if the type can be deployed in a configuration
     * @param monitored is a boolean insdicating if the type is to be monitored
     * @return ComponentType
     */
//    public ProductType createProductType(String name, Collection serviceComponentTypes, boolean deployable, boolean monitored) {
////        BasicProductTypeType productType = (BasicProductTypeType)this.createComponentType(ComponentObject.PRODUCT_COMPONENT_TYPE_CODE, name, null, ProductTypeType.PRODUCT_SUPER_TYPE_ID, deployable, monitored);
//
//        BasicProductType productType = (BasicProductType) createProductType(name, deployable, monitored); 
//            //(BasicProductType) BasicUtil.createComponentObject(ComponentObject.PRODUCT_COMPONENT_TYPE_CODE, ProductType.PRODUCT_TYPE_ID, name);
// 
//        //this code sets up the legal service types for the product type
////        ComponentTypeID productTypeID = productType.getComponentTypeID();
//        ComponentType serviceComponentType = null;
//        Iterator iter = serviceComponentTypes.iterator();
//        while (iter.hasNext()){
//
//            serviceComponentType = (ComponentType)iter.next();
////            this.setParentComponentTypeID(serviceComponentType, productTypeID);
//            //add the service ComponentTypeID to the BasicProductType
//            productType.addServiceTypeID((ComponentTypeID)serviceComponentType.getID());
//        }
//        return productType;
//    }

    /**
     * Create a new ProductType instance with the specified name.  Use
     * {@link createProductType(String, Collection, boolean, boolean)} to
     * also assign legal service types to this product type.
     * @param name is the name of the ProductType
     * @param deployable is a boolean indicating if the type can be deployed in a configuration
     * @param monitored is a boolean insdicating if the type is to be monitored
     * @return ComponentType
     */
//    public ProductType createProductType(String name, boolean deployable, boolean monitored) {
////        ProductType productType = (ProductType)this.createComponentType(ComponentType.PRODUCT_COMPONENT_TYPE_CODE, name, null, ProductTypeType.PRODUCT_SUPER_TYPE_ID, deployable, monitored);
//
//        BasicProductType productType = (BasicProductType) BasicUtil.createComponentType(ComponentType.PRODUCT_COMPONENT_TYPE_CODE, name, null, ProductType.PRODUCT_SUPER_TYPE_ID, deployable, monitored);
////        createComponentObject(ComponentDefn.PRODUCT_COMPONENT_CODE, ProductType.PRODUCT_TYPE_ID, name);
//
//        return productType;
//    }

/**
 * This method is not provided in the interface, it is only used by the
 * spi implementation for ease of creating the correct type of component defn.
 */

    public BasicComponentDefn createComponentDefn(int defnTypeCode, ConfigurationID configID, ComponentTypeID typeID, String defnName) {
        if (defnName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ComponentType.class.getName()));
        }

        BasicComponentDefn defn = BasicUtil.createComponentDefn(defnTypeCode, configID, typeID, defnName);
            //BasicComponentDefn.getInstance(defnTypeCode, configID, typeID, defnName);
        return defn;
    }

    public ComponentTypeDefn createComponentTypeDefn(ComponentType type, PropertyDefinition propertyDefinition, boolean isEffectiveImmediately) {
        if (type == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        if (propertyDefinition == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ComponentTypeDefn.class.getName()));
        }
        ComponentTypeDefnID defnID = new ComponentTypeDefnID(propertyDefinition.getName());

        BasicComponentTypeDefn defn = new BasicComponentTypeDefn(defnID, (ComponentTypeID) type.getID(), propertyDefinition, false, isEffectiveImmediately);

        createCreationAction(defnID, defn);


        ComponentTypeDefn newDefn = (ComponentTypeDefn) defn.clone();

        BasicComponentType basicType = (BasicComponentType) verifyTargetClass(type,BasicComponentType.class);
        basicType.addComponentTypeDefinition(newDefn);
        return newDefn;
    }

    /**
     * @deprecated as of v 2.0 beta 1, use {@link #createComponentTypeDefn(ComponentType, PropertyDefinition, boolean)}
     */
    public ComponentTypeDefn createComponentTypeDefn(ComponentType type, PropertyDefinition propertyDefinition) {
        return this.createComponentTypeDefn(type, propertyDefinition, false);
    }

    /**
     * @see createComponentTypeDefn(ComponentType, PropertyDefinition, boolean)
     */
    public ComponentTypeDefn createComponentTypeDefn(ComponentTypeID typeID, PropertyDefinition propertyDefinition, boolean isEffectiveImmediately ) {
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (propertyDefinition == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, PropertyDefinition.class.getName()));
        }
        ComponentTypeDefnID defnID = new ComponentTypeDefnID(propertyDefinition.getName());

        BasicComponentTypeDefn defn = new BasicComponentTypeDefn(defnID, typeID, propertyDefinition, false, isEffectiveImmediately);

        createCreationAction(defnID, defn);

        ComponentTypeDefn newDefn = (ComponentTypeDefn) defn.clone();
        return newDefn;
    }

    /**
     * @deprecated as of v 2.0 beta 1, use {@link #createComponentTypeDefn(ComponentTypeID, PropertyDefinition, boolean)}
     */
    public ComponentTypeDefn createComponentTypeDefn(ComponentTypeID typeID, PropertyDefinition propertyDefinition ) {
        return this.createComponentTypeDefn(typeID, propertyDefinition, false);
    }

    public PropDefnAllowedValue createPropDefnAllowedValue(ComponentTypeDefn typeDefn, PropertyDefinition propDefn, String value) throws InvalidComponentException {

        BasicPropDefnAllowedValue defnValue;
        Object obj;
        int maxCode = Integer.MIN_VALUE;
        int allowedCode;

        // obtain the maximum code currently used so that the next value can be assigned
        for (Iterator it = typeDefn.getAllowedValues().iterator(); it.hasNext(); ) {
            obj = it.next();
            defnValue = (BasicPropDefnAllowedValue) obj;
            allowedCode = defnValue.getAllowedCode();
            if (allowedCode > maxCode) {
                maxCode = allowedCode;
            }

        }

        if (maxCode == Integer.MIN_VALUE) {
            maxCode = 1;
        } else {
            ++maxCode;
        }

        StringBuffer sb = new StringBuffer(typeDefn.getFullName());
        sb.append("."); //$NON-NLS-1$
        sb.append(new Integer(maxCode).toString());

        PropDefnAllowedValueID id = new PropDefnAllowedValueID(sb.toString());

        BasicPropDefnAllowedValue bv = new BasicPropDefnAllowedValue( (ComponentTypeDefnID) typeDefn.getID(),
                                                                        typeDefn.getComponentTypeID(),
                                                                        id,
                                                                        value);

        createCreationAction(id, bv);

        propDefn.getAllowedValues().add(bv);

        return bv;
    }

    public DeployedComponent createDeployedServiceComponent(String name, Configuration configuration, HostID hostId, VMComponentDefnID vmId, ServiceComponentDefn serviceComponentDefn) {
        if ( name == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, DeployedComponent.class.getName()));
         }
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }
        if ( hostId == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, HostID.class.getName()));
        }
        if ( vmId == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefnID.class.getName()));
        }
        if (serviceComponentDefn == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ServiceComponentDefn.class.getName()));
        }

        ConfigurationID configID = (ConfigurationID) configuration.getID();


//        DeployedComponentID id = new DeployedComponentID(name, configID,  hostId, vmId, pscID, (ServiceComponentDefnID) serviceComponentDefn.getID());

//        DeployedComponentID id = new DeployedComponentID(name, configID,  hostId, vmId, (ServiceComponentDefnID) serviceComponentDefn.getID());
        BasicDeployedComponent deployComponent = BasicUtil.createDeployedComponent(name,
                                                                configID,
                                                                hostId,
                                                                vmId,
                                                                (ServiceComponentDefnID) serviceComponentDefn.getID(),                                            
                                                                serviceComponentDefn.getComponentTypeID());


        createCreationAction(deployComponent.getID(), deployComponent );

        DeployedComponent newDefn = (DeployedComponent) deployComponent.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addDeployedComponent(newDefn);

        return newDefn;
    }


    /*
     * @see createDeployedServiceComponent(String, Configuration, HostID, VMComponentDefn)
     */
    public DeployedComponent createDeployedServiceComponent(String name, ConfigurationID configurationID, HostID hostId, VMComponentDefnID vmId, ServiceComponentDefnID serviceComponentDefnID, ComponentTypeID serviceComponentTypeID) {
        if ( name == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, DeployedComponent.class.getName()));
        }
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }
        if ( hostId == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, HostID.class.getName()));
        }
        if ( vmId == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefnID.class.getName()));
        }
        if (serviceComponentDefnID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ServiceComponentDefnID.class.getName()));
        }
        if (serviceComponentTypeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }


 //       DeployedComponentID id = new DeployedComponentID(name, configurationID,  hostId, vmId, pscID, serviceComponentDefnID);

//        DeployedComponentID id = new DeployedComponentID(name, configurationID,  hostId, vmId, serviceComponentDefnID);

        BasicDeployedComponent deployComponent = BasicUtil.createDeployedComponent(name,
                                                                                   configurationID,
                                                                                   hostId,
                                                                                   vmId,
                                                                                   serviceComponentDefnID,
                                                                                   serviceComponentTypeID);
        
//        BasicDeployedComponent deployComponent = new BasicDeployedComponent(id,
//                                                                configurationID,
//                                                                hostId,
//                                                                vmId,
//                                                                serviceComponentDefnID,
//                                                                pscID,
//                                                                serviceComponentTypeID);

        createCreationAction(deployComponent.getID(), deployComponent );

        DeployedComponent newDefn = (DeployedComponent) deployComponent.clone();
        return newDefn;
    }

    public DeployedComponent createDeployedVMComponentx(String name, Configuration configuration,VMComponentDefn vmComponentDefn) {
        if ( name == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, DeployedComponent.class.getName()));
        }
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }
        if ( vmComponentDefn == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefn.class.getName()));
        }

        ConfigurationID configID = (ConfigurationID) configuration.getID();


//        DeployedComponentID id = new DeployedComponentID(name, configID,  hostId, (VMComponentDefnID) vmComponentDefn.getID());
        
           
        BasicDeployedComponent deployComponent = BasicUtil.createDeployedVMComponent(name, configID, vmComponentDefn.getHostID(), (VMComponentDefnID) vmComponentDefn.getID(), vmComponentDefn.getComponentTypeID());
//            new BasicDeployedComponent(id,
//                                                                configID,
//                                                                hostId,
//                                                                (VMComponentDefnID) vmComponentDefn.getID(),
//                                                                vmComponentDefn.getComponentTypeID());


        createCreationAction(deployComponent.getID(), deployComponent );

        DeployedComponent newDefn = (DeployedComponent) deployComponent.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addDeployedComponent(newDefn);

        return newDefn;
    }

    /*
     * @see createDeployedVMComponent(String, Configuration, HostID, VMComponentDefn)
     */
    public DeployedComponent createDeployedVMComponentx(String instanceName, ConfigurationID configurationID, HostID hostId, VMComponentDefnID vmComponentDefnID, ComponentTypeID componentTypeID){
        if ( instanceName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, DeployedComponent.class.getName()));
        }
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }
        if ( hostId == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, HostID.class.getName()));
        }
        if ( vmComponentDefnID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefnID.class.getName()));
        }
        if ( componentTypeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }


//        DeployedComponentID id = new DeployedComponentID(instanceName, configurationID,  hostId, vmComponentDefnID);

        BasicDeployedComponent deployComponent = BasicUtil.createDeployedVMComponent(instanceName, configurationID, hostId, vmComponentDefnID, componentTypeID);
//        BasicDeployedComponent deployComponent = new BasicDeployedComponent(id,
//                                                                configurationID,
//                                                                hostId,
//                                                                vmComponentDefnID,
//                                                                componentTypeID);
//

        createCreationAction(deployComponent.getID(), deployComponent );

        DeployedComponent newDefn = (DeployedComponent) deployComponent.clone();
        return newDefn;
    }

    /*
     * @see createServiceComponentDefn(Configuration, ComponentTypeID, String)
     */
    public ServiceComponentDefn createServiceComponentDefn(ConfigurationID configurationID, ComponentTypeID typeID, String componentName) {
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }

        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (componentName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ServiceComponentDefn.class.getName()));
        }

        String routingUUID;
        if (typeID.getName().equals(RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_TYPE_NAME)) {
			/**
			* This is done because the RuntimeMetadataService is a Service
			* that must be treated as a Connector for only the RuntimeMetadata
			* database.  Its routing UUID is the same for all RuntimeMetadata
			* Service instances.  The RuntimeMetadata VDB is created at server
			* install time and is also set to use 'Connectors' with this
			* routing UUID.
			*/
			routingUUID = RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_ROUTING_ID;
        } else {
	        routingUUID = getObjectIDFactory().create().toString() ;
        }

        return createServiceComponentDefn(configurationID, typeID, componentName, routingUUID);
    }

    public ServiceComponentDefn createServiceComponentDefn(ConfigurationID configurationID, ComponentTypeID typeID, String componentName, String routingUUID) {
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }

        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (componentName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ServiceComponentDefn.class.getName()));
        }
        if (routingUUID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0090, componentName));
        }

		BasicServiceComponentDefn defn = (BasicServiceComponentDefn)  BasicUtil.createComponentDefn(ComponentDefn.SERVICE_COMPONENT_CODE, configurationID, typeID, componentName);
 
        //BasicComponentDefn.getInstance(ComponentDefn.SERVICE_COMPONENT_DEFN_CODE, configurationID, typeID, componentName);

        defn.setRoutingUUID(routingUUID );
        createCreationAction(defn.getID(), defn);

        ServiceComponentDefn newDefn = (ServiceComponentDefn) defn.clone();
        return newDefn;
    }

    /**
     * Creates a new ServiceComponentDefn for a given Configuration and
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
     * @return new ServiceComponentDefn
     */
//    public ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ComponentTypeID typeID, String componentName, ProductServiceConfigID pscID) {
//        if ( configuration == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
//        }
//        if ( pscID == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ProductServiceConfigID.class.getName()));
//        }
//        ProductServiceConfig psc = (ProductServiceConfig)configuration.getComponentDefn(pscID);
//
//        Assertion.isNotNull(psc, "PSC " + pscID + " does not exist"); //$NON-NLS-1$ //$NON-NLS-2$
//        ConfigurationID configurationID = (ConfigurationID)configuration.getID();
//
//		BasicServiceComponentDefn newServiceDefn = (BasicServiceComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.SERVICE_COMPONENT_CODE, configurationID, typeID, componentName); 
//        
////        BasicComponentDefn.getInstance(ComponentDefn.SERVICE_COMPONENT_DEFN_CODE,
////							configurationID,
////							typeID,
////							componentName);
//		// add the service to the psc so that this relationship is found in the deployServiceDefn method
//		addServiceComponentDefn(psc, (ServiceComponentDefnID) newServiceDefn.getID());
//
//            ServiceComponentDefn newDefn = (ServiceComponentDefn) newServiceDefn.clone();
//            
//            BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
//            bc.addComponentDefn(newDefn);
//   
//            //automatically deploy the service anywhere that the PSC is already deployed
//        this.deployServiceDefn(bc,newDefn,pscID);            
//        
//        return newDefn;
//    }

    public ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ComponentTypeID typeID, String componentName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (componentName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ServiceComponentDefn.class.getName()));
        }

        String routingUUID;
        if (componentName.equals(RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_TYPE_NAME)) {
        	/**
        	* This is done because the RuntimeMetadataService is a Service
        	* that must be treated as a Connector for only the RuntimeMetadata
        	* database.  Its routing UUID is the same for all RuntimeMetadata
        	* Service instances.  The RuntimeMetadata VDB is created at server
        	* install time and is also set to use 'Connectors' with this
        	* routing UUID.
        	*/
        	routingUUID = RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_ROUTING_ID;
        } else {
            routingUUID = getObjectIDFactory().create().toString();
        }
        return createServiceComponentDefn(configuration, typeID, componentName, routingUUID);
    }

    public ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ComponentTypeID typeID, String componentName, String routingUUID) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
        }
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }
        if (componentName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ServiceComponentDefn.class.getName()));
        }
        if (routingUUID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0090, componentName));
        }

        ConfigurationID configurationID = (ConfigurationID) configuration.getID();
		BasicServiceComponentDefn serviceDefn = (BasicServiceComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.SERVICE_COMPONENT_CODE, configurationID, typeID, componentName); 
//        BasicComponentDefn.getInstance(ComponentDefn.SERVICE_COMPONENT_DEFN_CODE,
//							configurationID,
//							typeID,
//							componentName);

        serviceDefn.setRoutingUUID( routingUUID );
        createCreationAction(serviceDefn.getID(), serviceDefn);

        ServiceComponentDefn newDefn = (ServiceComponentDefn) serviceDefn.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);

        return newDefn;
    }


    /**
     * Creates a new ServiceComponentDefn in the indicated Configuration,
     * copied from the indicated original ServiceComponentDefn, with
     * the indicated new name.
     */
    public ServiceComponentDefn createServiceComponentDefn(Configuration configuration, ServiceComponentDefn originalServiceComponentDefn, String newName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (originalServiceComponentDefn == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ServiceComponentDefn.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ServiceComponentDefn.class.getName()));
        }

        ConfigurationID configurationID = (ConfigurationID)configuration.getID();
		BasicServiceComponentDefn serviceDefn = (BasicServiceComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.SERVICE_COMPONENT_CODE, configurationID, originalServiceComponentDefn.getComponentTypeID(),
                                                                                                        newName);
//        BasicComponentDefn.getInstance(ComponentDefn.SERVICE_COMPONENT_DEFN_CODE,
//							configurationID,
//							originalServiceComponentDefn.getComponentTypeID(),
//							newName);

         if (serviceDefn.getComponentTypeID().getName().equals(RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_TYPE_NAME)) {
        	/**
        	* This is done because the RuntimeMetadataService is a Service
        	* that must be treated as a Connector for only the RuntimeMetadata
        	* database.  Its routing UUID is the same for all RuntimeMetadata
        	* Service instances.  The RuntimeMetadata VDB is created at server
        	* install time and is also set to use 'Connectors' with this
        	* routing UUID.
        	*/
        	serviceDefn.setRoutingUUID(RuntimeMetadataServiceComponentType.RUNTIME_METADATA_SERVICE_ROUTING_ID);
        }else {
            //serviceDefn.setRoutingUUID( getObjectIDFactory().create().toString() );
            //***************************************************************
            //MAJOR KLUDGE - we will COPY the original UUID, instead of making new one
            serviceDefn.setRoutingUUID( originalServiceComponentDefn.getRoutingUUID() );
            //***************************************************************
        }
        createCreationAction(serviceDefn.getID(), serviceDefn);

        this.modifyProperties(serviceDefn, originalServiceComponentDefn.getProperties(), ADD);

        ServiceComponentDefn newServiceDefn = (ServiceComponentDefn) serviceDefn.clone();
        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newServiceDefn);
        return newServiceDefn;
    }

    public ResourceDescriptor createResourceDescriptor(ConfigurationID configurationID, ComponentTypeID typeID, String descriptorName) {
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }
        if (descriptorName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ResourceDescriptor.class.getName()));
        }

        BasicResourceDescriptor defn = (BasicResourceDescriptor) BasicUtil.createComponentDefn(ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE, configurationID, typeID, descriptorName);
//        BasicComponentDefn.getInstance(ComponentDefn.RESOURCE_COMPONENT_DEFN_CODE, configurationID, typeID, descriptorName);
        createCreationAction(defn.getID(), defn);

        ResourceDescriptor newDefn = (ResourceDescriptor) defn.clone();

        return newDefn;
    }

    public ResourceDescriptor createResourceDescriptor(Configuration configuration, ComponentTypeID typeID, String descriptorName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
        }
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }
        if (descriptorName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ResourceDescriptor.class.getName()));
        }

        ConfigurationID configID = (ConfigurationID) configuration.getID();

        BasicResourceDescriptor defn = (BasicResourceDescriptor) BasicUtil.createComponentDefn(ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE, configID, typeID, descriptorName);
        
 //       BasicComponentDefn.getInstance(ComponentDefn.RESOURCE_COMPONENT_DEFN_CODE, configID, typeID, descriptorName);
        createCreationAction(defn.getID(), defn);

        ResourceDescriptor newDefn = (ResourceDescriptor) defn.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);

        return newDefn;
    }

    public ResourceDescriptor createResourceDescriptor(Configuration configuration, ResourceDescriptor original, String newName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (original == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ResourceDescriptor.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ResourceDescriptor.class.getName()));
        }

        ConfigurationID configurationID = (ConfigurationID) configuration.getID();
        BasicResourceDescriptor defn = (BasicResourceDescriptor) BasicUtil.createComponentDefn(ComponentDefn.RESOURCE_DESCRIPTOR_COMPONENT_CODE, configurationID, original.getComponentTypeID(), newName);
 //       BasicComponentDefn.getInstance(ComponentDefn.RESOURCE_COMPONENT_DEFN_CODE, configurationID, original.getComponentTypeID(), newName);

        createCreationAction(defn.getID(), defn);

        this.modifyProperties(defn, original.getProperties(), ADD);

        ResourceDescriptor newDefn = (ResourceDescriptor) defn.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);

        return newDefn;
    }



    public SharedResource createSharedResource(ComponentTypeID typeID, String resourceName) {

        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (resourceName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, SharedResource.class.getName()));
        }

//        SharedResourceID resourceID = new SharedResourceID(resourceName);
        
        BasicSharedResource defn = (BasicSharedResource) BasicUtil.createComponentObject(ComponentDefn.SHARED_RESOURCE_COMPONENT_CODE, typeID, resourceName);
        

//        BasicSharedResource defn = new BasicSharedResource(resourceID, typeID);

        createCreationAction(defn.getID(), defn);

        SharedResource newDefn = (SharedResource) defn.clone();
        return newDefn;
    }



    /*
     * @see createVMComponentDefn(Configuration, ComponentTypeID, String)
     */
    public VMComponentDefn createVMComponentDefn(ConfigurationID configurationID, HostID hostID,  ComponentTypeID typeID, String componentName) {
        if ( configurationID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }

        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }

        if (componentName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, VMComponentDefn.class.getName()));
        }
    	BasicVMComponentDefn defn = (BasicVMComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.VM_COMPONENT_CODE, configurationID, hostID, typeID, componentName); 
        //BasicComponentDefn.getInstance(ComponentDefn.VM_COMPONENT_DEFN_CODE,
//							configurationID,
//							typeID,
//							componentName);
        createCreationAction(defn.getID(), defn);                     

        VMComponentDefn newDefn = (VMComponentDefn) defn.clone();
        return newDefn;
    }

    public VMComponentDefn createVMComponentDefn(Configuration configuration, HostID hostID, ComponentTypeID typeID, String componentName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        
        
        VMComponentDefn newDefn = createVMComponentDefn((ConfigurationID) configuration.getID(), hostID, typeID, componentName);

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);
        
    
        return newDefn;
 
    }

    public VMComponentDefn createVMComponentDefn(Configuration configuration, VMComponentDefn original, String newName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (original == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefn.class.getName()));
        }
        
        
        BasicVMComponentDefn defn = (BasicVMComponentDefn) createVMComponentDefn((ConfigurationID) configuration.getID(), original.getHostID(), original.getComponentTypeID(), newName);

//        ConfigurationID configurationID = (ConfigurationID) configuration.getID();
//    	BasicVMComponentDefn defn = (BasicVMComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.VM_COMPONENT_CODE, configurationID, original.getComponentTypeID(), newName);
//        BasicComponentDefn.getInstance(ComponentDefn.VM_COMPONENT_DEFN_CODE,
//							configurationID,
//							original.getComponentTypeID(),
//							newName);

 

        // take the original properties, and overlay with any new properties
        // that were added when created to create the new set
        Properties props = original.getProperties();
        props.putAll(defn.getEditableProperties());
             

        this.modifyProperties(defn, props, ADD);
        
        this.createDeployedVMComponentx(defn.getName(), configuration, defn);
        
        VMComponentDefn newDefn = (VMComponentDefn) defn.clone();

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);

        return newDefn;
    }

    /**
     * Copy-creation method, creates a new PSC from the given PSC, with the
     * new given name, and inserts it into the given Configuration.  The
     * configuration parameter must match the configuration which the
     * original PSC belongs to.
     * @param targetConfiguration to put the new PSC in; this will be modified
     * @param originConfiguration which the originalPSC exists in - this is
     * needed to retrieve any ServiceComponentDefns from
     * @param originalPSC the PSC to copy from
     * @param newName new String name for the new PSC
     * @return newly-created ProductServiceConfig object
     */
//    public ProductServiceConfig createProductServiceConfig(Configuration configuration, ProductServiceConfig originalPSC, String newName){
//        if ( configuration == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
//        }
//
//        if (originalPSC == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ProductServiceConfig.class.getName()));
//        }
//
//        if (newName == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ProductServiceConfig.class.getName()));
//        }
//
//    	ProductServiceConfig psc = createProductServiceConfig(configuration, (ProductTypeID) originalPSC.getComponentTypeID(),newName);
//
//        this.modifyProperties(psc, originalPSC.getProperties(), ADD);
//
//        Iterator iter = originalPSC.getServiceComponentDefnIDs().iterator();
//        ServiceComponentDefnID originalServiceDefnID = null;
//        while (iter.hasNext()){
//            originalServiceDefnID = (ServiceComponentDefnID)iter.next();
//            this.addServiceComponentDefn(psc, originalServiceDefnID);
//
//        }
//
//        ProductServiceConfig newPSC = (ProductServiceConfig) psc.clone();
//        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
//        bc.addComponentDefn(newPSC);
//        return newPSC;
//    }

    /**
     * Allows the creation of an empty ProductServiceConfig entirely from
     * ID objects.
     */
//    public ProductServiceConfig createProductServiceConfig(ConfigurationID configurationID, ProductTypeID productTypeID, String componentName){
//        if ( configurationID == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
//        }
//
//        if (productTypeID == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
//        }
//
//        if (componentName == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ProductServiceConfig.class.getName()));
//        }
//        
//
//
//   		BasicProductServiceConfig psc = (BasicProductServiceConfig) BasicUtil.createComponentDefn(ComponentDefn.PSC_COMPONENT_CODE, configurationID, productTypeID, componentName); 
////        BasicComponentDefn.getInstance(ComponentDefn.PRODUCT_SERVICE_DEFN_CODE,
////							configurationID,
////							productTypeID,
////							componentName);
//
//        createCreationAction(psc.getID(), psc);
//
//        ProductServiceConfig newPSC = (ProductServiceConfig) psc.clone();
//        return newPSC;
//    }


//    public ProductServiceConfig createProductServiceConfig(Configuration config, ProductTypeID productTypeID,  String name) {
//        ProductServiceConfig psc = createProductServiceConfig((ConfigurationID)config.getID(), productTypeID, name);
//        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(config,BasicConfiguration.class);
//        bc.addComponentDefn(psc);
//        return psc;
//    }

    /**
     * Deploys the ServiceComponentDefns indicated by the ProductServiceConfig,
     * contained by the Configuration, onto the specified Host and VM.
     * @param configuration the Configuration which contains the
     * ServiceComponentDefns, the ProductServiceConfig, and the VMComponentDefn
     * @param psc the ProductServiceConfig which groups the ServiceComponentDefns
     * to be deployed
     * @param hostId ID of the host on which the services will be deployed
     * @param vmId ID of the VMComponentDefn on which the services will be
     * deployed
     * @return Collection of DeployedComponent objects, each representing
     * one of the deployed ServiceComponentDefns
     */
//    public Collection deployProductServiceConfig(Configuration configuration, ProductServiceConfig psc, HostID hostId, VMComponentDefnID vmId){
//        if ( configuration == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
//        }
//        if ( hostId == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, HostID.class.getName()));
//        }
//        if ( vmId == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefnID.class.getName()));
//        }
//        if (psc == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ProductServiceConfig.class.getName()));
//        }
////        ConfigurationID configID = (ConfigurationID) configuration.getID();
//        ProductServiceConfigID pscID = ( ProductServiceConfigID) psc.getID();
//
//        Collection serviceComponentDefnIDs = psc.getServiceComponentDefnIDs();
//        HashSet result = new HashSet(serviceComponentDefnIDs.size());
//        Iterator iter = serviceComponentDefnIDs.iterator();
//        ServiceComponentDefn serviceDefn = null;
//        ServiceComponentDefnID serviceDefnID = null;
//        while (iter.hasNext()){
//
//            serviceDefnID = (ServiceComponentDefnID)iter.next();
//            serviceDefn = (ServiceComponentDefn)configuration.getComponentDefn(serviceDefnID);
//
//            //only deploy the service defn if it is enabled
//            if (psc.isServiceEnabled(serviceDefnID)) {
//                DeployedComponentID id = new DeployedComponentID(serviceDefnID.getName(), (ConfigurationID) configuration.getID(), hostId, vmId, pscID, serviceDefnID);
//
//                if (configuration.getDeployedComponent(id) != null) {
//                    continue;
//                }
//                    
//                DeployedComponent dc = this.createDeployedServiceComponent(serviceDefnID.getName(), configuration, hostId, vmId, serviceDefn, pscID);
//                result.add(dc);
////                DeployedComponentID id = new DeployedComponentID(serviceDefnID.getName(), configID, hostId, vmId, pscID, serviceDefnID);
////                BasicDeployedComponent deployComponent = new BasicDeployedComponent(id,
////                                                                        configID,
////                                                                        hostId,
////                                                                        vmId,
////                                                                        serviceDefnID,
////                                                                        pscID,
////                                                                        serviceDefn.getComponentTypeID());
//
////                createCreationAction(id, deployComponent );
//
////                try {
////                    DeployedComponent newDefn = (DeployedComponent) deployComponent.clone();
////
////                    BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
////                    bc.addDeployedComponent(newDefn);
////
////                    result.add( newDefn);
////                } catch (CloneNotSupportedException e) {
////                    throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0078,
////                                new Object[] {DeployedComponent.class.getName(),  e.getMessage()}));
////                }
//           }
//        }
//        return result;
//    }
    
    /**
     * This method will update a PSC by adding the new service list of ID's and removing
     * the service ID's from the PSC that are not in the service list.
     * @param config
     * @param psc
     * @param newServiceIDList is the new list of ID's that the psc will contain
     * @return updated ProductServiceConfig
     * @throws ConfigurationException
     */
//    public ProductServiceConfig updateProductServiceConfig(Configuration config, ProductServiceConfig psc, Collection newServiceIDList)     throws ConfigurationException {
//        if ( config == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));            
//        }
//        if (psc == null ) {
//            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ProductServiceConfig.class.getName()));            
//        } 
//        
//        if (newServiceIDList == null) {
//            return psc;       
//        }
//        
//        // keep the old-current services for later processing
//        HashSet set = new HashSet();
//        set.addAll(psc.getServiceComponentDefnIDs());
//        
//        ProductServiceConfig c = (ProductServiceConfig) psc.clone();
//        
//        BasicProductServiceConfig basicPSC = (BasicProductServiceConfig) verifyTargetClass(c,BasicProductServiceConfig.class);
//        basicPSC.resetServices();
//        // 1st - deploy the service to the PSC
//        ProductServiceConfigID pscID = (ProductServiceConfigID) psc.getID();
//        
//        
//        // all the services have been removed from this psc
//        // def# 12847 not removing the last service from a psc
//        if (newServiceIDList.isEmpty()) {
//            basicPSC.resetServices();
//        } else {
//       
//            for (Iterator it = newServiceIDList.iterator(); it.hasNext(); ) {
//                // if the service isnt associated with the psc, then add it 
//                // and also deploy it
//                ServiceComponentDefnID sid = (ServiceComponentDefnID)it.next();
//                ServiceComponentDefn sd = (ServiceComponentDefn) config.getComponentDefn(sid);
//    
//                if (psc.containsService(sid)) {
//                    // remove it from the set so that whats left are those
//                    // that have to be removed from the psc
//                    set.remove(sid);
//    
//                } else {
//                        deployServiceDefn(config, sd, pscID);
//                }  
//                
//                basicPSC.addServiceComponentDefnID(sid);
//                
//            }
//        }
//        
//        // 2nd - remove any services no longer selected
//        for (Iterator it = set.iterator(); it.hasNext(); ) {
//            ServiceComponentDefnID sid = (ServiceComponentDefnID) it.next();
//            ServiceComponentDefn sdefn = (ServiceComponentDefn) config.getComponentDefn(sid);
//            // set the service enabled flag to false so that it will be undeployed
//            this.setEnabled(config, sdefn, psc, false, true);
//        }
//        createExchangeAction(pscID,ConfigurationModel.Attribute.UPDATE_PSC, newServiceIDList, Boolean.TRUE);               
//                                  
//        return basicPSC; 
//
//    } 
    
    /**
     * This will update / replace the existing component type with the specified
     * component type. 
     * @param t is the ComponentType to update / replace
     * @return ComponentType that is modified
     * @see #createComponentTypeDefn(ComponentTypeID, PropertyDefinition, boolean)
     */
    public ComponentType updateComponentType(ComponentType origType, ComponentType replaceType)  {
        if ( replaceType == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }
        
        createExchangeAction(replaceType.getID(),ConfigurationModel.Attribute.UPDATE_COMPONENT_TYPE,
                             origType, replaceType );
        

        return replaceType;
    }    


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
    public void setRoutingUUID(ServiceComponentDefn serviceComponentDefn, String newRoutingUUID){
        if ( serviceComponentDefn == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ServiceComponentDefn.class.getName()));
        }
        String oldUUID = serviceComponentDefn.getRoutingUUID();

        BasicServiceComponentDefn basicService = (BasicServiceComponentDefn) verifyTargetClass(serviceComponentDefn,BasicServiceComponentDefn.class);
        basicService.setRoutingUUID(newRoutingUUID);

        createExchangeAction(basicService.getID(),ConfigurationModel.Attribute.ROUTING_UUID, oldUUID, newRoutingUUID);
    }


 public DeployedComponent setEnabled(DeployedComponent deployedcomponent, boolean enabled) {
        if ( deployedcomponent == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, DeployedComponent.class.getName()));
        }

        boolean oldEnabled = deployedcomponent.isEnabled();
        //if a change is not being made to the enabled value, this whole method
        //will be essentially bypassed
        if (enabled != oldEnabled){

        	BasicDeployedComponent basicDC = (BasicDeployedComponent) verifyTargetClass(deployedcomponent,BasicDeployedComponent.class);
			basicDC.setIsEnabled(enabled);

            createExchangeAction(basicDC.getID(),ConfigurationModel.Attribute.IS_ENABLED, deployedcomponent.getDeployedComponentDefnID(), Boolean.valueOf(enabled));

            return basicDC;

        } //end if enabled!= oldEnabled
        
        return deployedcomponent;

    }

    public ComponentObject addProperty( ComponentObject t, String name, String value ) {
        if ( t == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentObject.class.getName()));
        }

        if (name == null || value == null) {
            return t;
        }
        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);

        createAddNamedAction(target.getID(),ConfigurationModel.Attribute.PROPERTY, name, value);

        target.addProperty(name, value);

        return target;
    }

    public ComponentObject setProperty( ComponentObject t, String name, String value ) {
        if ( t == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentObject.class.getName()));
        }

        if (name == null || value == null) {
            return t;
        }

        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
        String oldValue = target.getProperty(name);

        createExchangeNamedAction(target.getID(),ConfigurationModel.Attribute.PROPERTY, name, oldValue ,value);

        target.removeProperty(name);
        target.addProperty(name, value);

        return target;
    }


    public ComponentObject removeProperty( ComponentObject t, String name) {
        if ( t == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentObject.class.getName()));
        }

        if (name == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0092));
        }

        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
        String value = target.getProperty(name);

        createRemoveNamedAction(target.getID(),ConfigurationModel.Attribute.PROPERTY, name, value);

        target.removeProperty(name);

        return target;

    }

    public ComponentTypeDefn modifyComponentTypeDefn(ComponentTypeDefn original, ComponentTypeDefn updated  ) {

        if ( updated == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeDefn.class.getName()));
        }
        BasicComponentTypeDefn target = (BasicComponentTypeDefn) verifyTargetClass(updated, BasicComponentTypeDefn.class);


        createExchangeAction(target.getID(),ConfigurationModel.Attribute.COMPONENT_TYPE_DEFN,
                                                original, updated );

        return target;
    }


    public ComponentObject modifyProperties( ComponentObject t, Properties props, int command ) {
        if ( t == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentObject.class.getName()));
        }
        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
        super.verifyCommand(command);   // throws exception
        Properties newProps = null;

        switch ( command ) {
            case ADD:
                newProps = new Properties();
                newProps.putAll( target.getEditableProperties() );
                newProps.putAll( props );

                createAddAction(target.getID(),ConfigurationModel.Attribute.PROPERTIES,props);
                target.addProperties(newProps);

                break;
            case REMOVE:
                newProps = new Properties();
                newProps.putAll( target.getEditableProperties() );
                Iterator iter = props.keySet().iterator();
                while ( iter.hasNext() ) {
                    newProps.remove( iter.next() );
                }

                createRemoveAction(target.getID(),ConfigurationModel.Attribute.PROPERTIES,props);
                target.setProperties(newProps);
                break;
            case SET:
                createExchangeAction(target.getID(),ConfigurationModel.Attribute.PROPERTIES,
                                                target.getEditableProperties(),props );
                target.setProperties(props);
                break;
        }

        return target;


    }


    public Configuration setIsReleased( Configuration t, boolean newValue ) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0079));
       }

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);

        BasicConfigurationInfo info = (BasicConfigurationInfo) target.getInfo();
        boolean oldValue = info.isReleased();

        if ( oldValue !=  newValue ) {

            createExchangeBoolean(target.getID(), ConfigurationModel.Attribute.IS_RELEASED, oldValue, newValue );

            info.setIsReleased(newValue);
            target.setInfo(info);
        }

        return target;

    }

    public Configuration setIsDeployed( Configuration t, boolean newValue ) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        BasicConfigurationInfo info = (BasicConfigurationInfo) target.getInfo();

        boolean oldValue = info.isDeployed();

        if ( oldValue !=  newValue ) {
            createExchangeBoolean(target.getID(), ConfigurationModel.Attribute.IS_DEPLOYED,oldValue,newValue );

            info.setIsDeployed(newValue);
            target.setInfo(info);
        }

        return target;

    }


// ComponentType methods
    public ComponentType setIsDeployable(ComponentType t, boolean newValue) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        boolean oldValue = target.isDeployable();

        if ( ! oldValue!= newValue ) {
            createExchangeBoolean(target.getID(),ConfigurationModel.Attribute.IS_DEPLOYABLE, oldValue ,newValue);
            target.setIsDeployable(newValue);
        }

        return target;
    }

    public ComponentType setIsDeprecated(ComponentType t, boolean newValue) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        boolean oldValue = target.isDeprecated();

        if ( ! oldValue!= newValue ) {
            createExchangeBoolean(target.getID(),ConfigurationModel.Attribute.IS_DEPRECATED, oldValue ,newValue);
            target.setIsDeprecated(newValue);
        }

        return target;
    }

    public ComponentType setIsMonitored(ComponentType t, boolean newValue) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        boolean oldValue = target.isMonitored();

        if ( ! oldValue!= newValue ) {
            createExchangeBoolean(target.getID(),ConfigurationModel.Attribute.IS_MONITORED, oldValue ,newValue);
            target.setIsMonitored(newValue);
        }

        return target;

    }

    public ComponentObject setComponentType(ComponentObject t, ComponentTypeID componentType) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentObject.class.getName()));
        }
       if ( componentType == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }


        BasicComponentObject target = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
        ComponentTypeID oldValue = target.getComponentTypeID();

        if ( !oldValue.equals(componentType) ) {
            createExchangeAction(target.getID(),ConfigurationModel.Attribute.COMPONENT_TYPEID, oldValue , componentType);
            target.setComponentTypeID(componentType);
        }

        return target;

    }

    public ComponentType setParentComponentTypeID(ComponentType t, ComponentTypeID parentID) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        ComponentTypeID oldValue = target.getParentComponentTypeID();

        if ( (parentID == null && oldValue != null) ||
             ( oldValue==null ||!oldValue.equals(parentID)) ) {
            createExchangeAction(target.getID(),ConfigurationModel.Attribute.PARENT_COMPONENT_TYPEID, oldValue , parentID);
            target.setParentComponentTypeID(parentID);
        }

        return target;

    }

    public ComponentType setSuperComponentTypeID(ComponentType t, ComponentTypeID superID) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        ComponentTypeID oldValue = target.getParentComponentTypeID();

        if ( (superID == null && oldValue != null) ||
             ( !oldValue.equals(superID)) ) {
            createExchangeAction(target.getID(),ConfigurationModel.Attribute.SUPER_COMPONENT_TYPEID, oldValue , superID);
            target.setSuperComponentTypeID(superID);
        }

        return target;

    }

    public ComponentType setLastChangedHistory(ComponentType type, String lastChangedBy, String lastChangedDate) {
		return BasicUtil.setLastChangedHistory(type, lastChangedBy, lastChangedDate);

    }
    public ComponentType setCreationChangedHistory(ComponentType type, String createdBy, String creationDate) {
		return BasicUtil.setCreationChangedHistory(type, createdBy, creationDate);
    }

    public ComponentObject setLastChangedHistory(ComponentObject defn, String lastChangedBy, String lastChangedDate) {
		return BasicUtil.setLastChangedHistory(defn, lastChangedBy, lastChangedDate);

    }
    public ComponentObject setCreationChangedHistory(ComponentObject defn, String createdBy, String creationDate) {
		return BasicUtil.setCreationChangedHistory(defn, createdBy, creationDate);
    }


    // ----------------------------------------------------------------------------------
    //                  D E L E T E    M E T H O D S
    // ----------------------------------------------------------------------------------

    public void delete(Host target) throws ConfigurationException {
        if ( target == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Host.class.getName()));
        }

        this.createDestroyAction(target.getID(),target);
    }


    public void delete(ComponentTypeDefn target, ComponentType type) throws ConfigurationException {
        if ( target == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeDefn.class.getName()));
        }
        if ( type == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType basicType = (BasicComponentType) verifyTargetClass(type,BasicComponentType.class);

        this.createDestroyAction(target.getID(),target);
        basicType.removeComponentTypeDefinition(target);
    }

    public void delete(PropDefnAllowedValue target) throws ConfigurationException {
        if ( target == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, PropDefnAllowedValue.class.getName()));
        }

        this.createDestroyAction(target.getID(), target);
    }

    public void delete(ComponentType target) throws ConfigurationException {
        if ( target == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType basicType = (BasicComponentType) verifyTargetClass(target,BasicComponentType.class);

        // 1st delete all the ComponentTypeDefns dependencies

// [vah] 110302 - a deletion of the object will by default remove its properties
//
        this.createDestroyAction(basicType.getID(), basicType);
    }


	public void delete(ConfigurationID targetID) throws ConfigurationException {
        if (targetID == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConfigurationID.class.getName()));
        }
        this.createDestroyAction(targetID, targetID);

	}

    public void delete(Configuration target) throws ConfigurationException  {
        delete(target, false);
    }

    public void delete(Configuration target, boolean deleteDependencies) throws ConfigurationException {
        if (target == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }
        BasicConfiguration basicConfig = (BasicConfiguration) verifyTargetClass(target,BasicConfiguration.class);

// VAN uncomment this line and comment out the rest
		delete((ConfigurationID) basicConfig.getID());

 	//vah 10/18/02
 	// the change here is that there is no need to send the whole configuration when a
 	// delete is performed because it is all or nothing with a model file.
    }

    public SharedResource delete( SharedResource target ) throws ConfigurationException {
            this.createDestroyAction(target.getID(), target);
            return target;
    }


    public Configuration delete( ComponentObject target, Configuration configuration ) throws ConfigurationException {
        return delete(target, configuration, false);
    }

    public Configuration delete( ComponentObject target, Configuration configuration,  boolean deleteDependencies ) throws ConfigurationException {
        //System.out.println("<!><!><!><!>deleting " + target + ", delete dependencies: " + deleteDependencies);
        if (target instanceof Configuration) {
            // this should never happen except in development, no need to translate
            throw new UnsupportedOperationException("Cannot call method delete(ComponentObject, Configuration) to delete a configuration, call delete(Configuration)."); //$NON-NLS-1$
        }
        
        BasicConfiguration basicConfig = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);

        basicConfig.removeComponentObject( (ComponentObjectID) target.getID());

        this.createDestroyAction(target.getID(), target);

        return basicConfig;
    }

 
    /**
     * Change the name of a previously defined VM in the Next Startup config.
     * @param vm The VM whose name to change.
     * @param name The new name.
     * @return The VM with its name changed.
     */
    public VMComponentDefn renameVM(VMComponentDefn vm, String name) throws ConfigurationException {
        if ( vm == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, VMComponentDefn.class.getName()));
        }
        BasicVMComponentDefn target = (BasicVMComponentDefn) verifyTargetClass(vm, BasicVMComponentDefn.class);
        ConfigurationID configID = target.getConfigurationID();
        // Allow rename only in Next Startup config
//        VMComponentDefnID newID = new VMComponentDefnID(configID, name);
       
       
//        ComponentTypeID typeID = target.getComponentTypeID();
        target = (BasicVMComponentDefn) BasicUtil.createComponentDefn(ComponentDefn.VM_COMPONENT_CODE, configID, target.getComponentTypeID(), name);
//            new BasicVMComponentDefn(configID, newID, typeID);

        createExchangeAction( target.getID(), ConfigurationModel.Attribute.NAME,
                              target.getName(), name );
        
        return target;
    }


    public boolean isDeployed(ServiceComponentDefnID defnID, Configuration config) {
        Collection dcs = config.getDeployedComponents(defnID);
        if (dcs != null && dcs.size() > 0) {
        	return true;

        }
        return false;
    }

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
    public ComponentType setComponentTypeDefinitions(ComponentType t, Collection defns)  {
        if ( t == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentType.class.getName()));
        }

        BasicComponentType target = (BasicComponentType) verifyTargetClass(t,BasicComponentType.class);
        target.setComponentTypeDefinitions(defns);

        return target;
    }

    public ComponentTypeDefn setPropertyDefinition(ComponentTypeDefn t, PropertyDefinition defn) {
       if ( t == null ) {
           throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeDefn.class.getName()));
        }

        BasicComponentTypeDefn target = (BasicComponentTypeDefn) verifyTargetClass(t,BasicComponentTypeDefn.class);
        target.setPropertyDefinition(defn);

        return target;

    }


    // ----------------------------------------------------------------------------------
    //      H E L P E R    M E T H O D S     F O R    T H E   T R A N S L A T O R
    // ----------------------------------------------------------------------------------

    // These methods are not exposed in the Interface

    public Configuration setConfigurationComponentDefns(Configuration t, Collection componentDefns) {

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        ComponentDefn cd;
        Map defns = new HashMap(componentDefns.size());

        for (Iterator it=componentDefns.iterator(); it.hasNext(); ) {
          cd = (ComponentDefn) it.next();
          defns.put(cd.getID(), cd);
        }

        target.setComponentDefns(defns);

        return target;
   }

    public Configuration setConfigurationDeployedComponents(Configuration t, Collection deployedComponents) {

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        DeployedComponent dc;
        Map dcs = new HashMap(deployedComponents.size());

        for (Iterator it=deployedComponents.iterator(); it.hasNext(); ) {
          dc = (DeployedComponent) it.next();
          dcs.put(dc.getID(), dc);
        }

        target.setDeployedComponents(dcs);

        return target;
    }


    public Configuration setConfigurationHostComponents(Configuration t, Collection hostComponents) {

        BasicConfiguration target = (BasicConfiguration) verifyTargetClass(t,BasicConfiguration.class);
        Host host;
        Map hosts = new HashMap(hostComponents.size());

        for (Iterator it=hostComponents.iterator(); it.hasNext(); ) {
          host = (Host) it.next();
          hosts.put(host.getID(), host);
        }

        target.setHosts(hosts);

        return target;
    }


    public Properties getEditableProperties(ComponentObject t) {
           BasicComponentObject bco = (BasicComponentObject) verifyTargetClass(t,BasicComponentObject.class);
           return bco.getEditableProperties();
    }

    /**
    * This method is used by the ConfigurationServiceImpl to enable the
    * console to import actions and assign them to the current configuration.
    */
    public Object assignConfigurationID(Object t, ConfigurationID configurationID) {
          if (t instanceof ComponentDefn) {
             BasicComponentDefn bcd = (BasicComponentDefn) verifyTargetClass(t,BasicComponentDefn.class);
             bcd.setConfigurationID(configurationID);
          } else if (t instanceof DeployedComponent) {
             BasicDeployedComponent bdc = (BasicDeployedComponent) verifyTargetClass(t,BasicDeployedComponent.class);
             bdc.setConfigurationID(configurationID);
          }
          return t;
    }


    // ----------------------------------------------------------------------------------
    //      H E L P E R    M E T H O D S     F O R    T H E  CDK
    // ----------------------------------------------------------------------------------
    
    public ConnectorBinding createConnectorComponent(ConfigurationID configurationID, ComponentTypeID typeID, String descriptorName, String routingUUID) {
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }
        if (descriptorName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ConnectorBinding.class.getName()));
        }


        BasicConnectorBinding defn = (BasicConnectorBinding) 
            BasicUtil.createComponentDefn(ComponentDefn.CONNECTOR_COMPONENT_CODE, configurationID, typeID, descriptorName); 
        createCreationAction(defn.getID(), defn);
        if (routingUUID == null) {
             routingUUID = getObjectIDFactory().create().toString() ;
        }

        defn.setRoutingUUID(routingUUID);

        ConnectorBinding newDefn = (ConnectorBinding) defn.clone();

        return newDefn;
    }

    public ConnectorBinding createConnectorComponent(ConfigurationID configurationID, ConnectorBinding original, String newName, String routingUUID) {

        if (original == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConnectorBinding.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ConnectorBinding.class.getName()));
        }

        BasicConnectorBinding defn = (BasicConnectorBinding) 
            BasicUtil.createComponentDefn(ComponentDefn.CONNECTOR_COMPONENT_CODE, configurationID, 
                                          original.getComponentTypeID(), newName);

        if (routingUUID == null) {
   	     	routingUUID = getObjectIDFactory().create().toString() ;
        }
        defn.setRoutingUUID(routingUUID);

        createCreationAction(defn.getID(), defn);

        this.modifyProperties(defn, original.getProperties(), ADD);

        ConnectorBinding newDefn = (ConnectorBinding) defn.clone();

        return newDefn;
    }

    public ConnectorBinding createConnectorComponent(Configuration configuration, ConnectorBinding originalConnector, String newName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (originalConnector == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ConnectorBinding.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, ConnectorBinding.class.getName()));
        }

        BasicConnectorBinding defn = (BasicConnectorBinding) 
            BasicUtil.createComponentDefn(ComponentDefn.CONNECTOR_COMPONENT_CODE, (ConfigurationID) configuration.getID(), 
                                          originalConnector.getComponentTypeID(), newName);

        defn.setRoutingUUID( originalConnector.getRoutingUUID() );

        createCreationAction(defn.getID(), defn);

        this.modifyProperties(defn, originalConnector.getProperties(), ADD);

        ConnectorBinding newDefn = (ConnectorBinding) defn.clone();
        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);
        return newDefn;
    }


    // ----------------------------------------------------------------------------------
    //      H E L P E R    M E T H O D S     F O R    T H E    A U T H  P R O V I D E R S
    // ----------------------------------------------------------------------------------
    
    public void addAuthenticationProvider(Configuration configuration, AuthenticationProvider provider) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (provider == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, AuthenticationProvider.class.getName()));
        }

        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(provider);
    }

    public AuthenticationProvider createAuthenticationProviderComponent(Configuration configuration, AuthenticationProvider originalProvider, String newName) {
        if ( configuration == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, Configuration.class.getName()));
        }

        if (originalProvider == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, AuthenticationProvider.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, AuthenticationProvider.class.getName()));
        }

        BasicAuthenticationProvider defn = (BasicAuthenticationProvider) 
            BasicUtil.createComponentDefn(ComponentDefn.AUTHPROVIDER_COMPONENT_CODE, (ConfigurationID) configuration.getID(), 
                                          originalProvider.getComponentTypeID(), newName);

        createCreationAction(defn.getID(), defn);

        this.modifyProperties(defn, originalProvider.getProperties(), ADD);

    	AuthenticationProvider newDefn = (AuthenticationProvider) defn.clone();
        BasicConfiguration bc = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);
        bc.addComponentDefn(newDefn);
        return newDefn;
    }

    public AuthenticationProvider createAuthenticationProviderComponent(ConfigurationID configurationID, ComponentTypeID typeID, String descriptorName) {
        if (typeID == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, ComponentTypeID.class.getName()));
        }
        if (descriptorName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, AuthenticationProvider.class.getName()));
        }


        BasicAuthenticationProvider defn = (BasicAuthenticationProvider) 
            BasicUtil.createComponentDefn(ComponentDefn.AUTHPROVIDER_COMPONENT_CODE, configurationID, typeID, descriptorName); 
        createCreationAction(defn.getID(), defn);

        AuthenticationProvider newDefn = (AuthenticationProvider) defn.clone();

        return newDefn;
    }

    public AuthenticationProvider createAuthenticationProviderComponent(ConfigurationID configurationID, AuthenticationProvider original, String newName) {

        if (original == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0089, AuthenticationProvider.class.getName()));
        }

        if (newName == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0087, AuthenticationProvider.class.getName()));
        }

        BasicAuthenticationProvider defn = (BasicAuthenticationProvider) 
            BasicUtil.createComponentDefn(ComponentDefn.AUTHPROVIDER_COMPONENT_CODE, configurationID, 
                                          original.getComponentTypeID(), newName);

        createCreationAction(defn.getID(), defn);

        this.modifyProperties(defn, original.getProperties(), ADD);

    	AuthenticationProvider newDefn = (AuthenticationProvider) defn.clone();

        return newDefn;
    }

    // ----------------------------------------------------------------------------------
    // P R I V A T E
    // ----------------------------------------------------------------------------------

    /**
     * Deploys a ServiceComponentDefn to the specified VM
     *  This method is harmless to call if the
     * ServiceComponentDefn is already deployed anywhere.  
     * @param configuration must be the Configuration containing both
     * the ServiceComponentDefn and PSC ID parameters (but this is not
     * checked for in this method)
     * @param serviceComponentDefn to be deployed
     * @param VMID VM ID indicates the process to deploy the service to
     * @return DeployedComponent of newly-created DeployedComponent object
     */
    public DeployedComponent deployServiceDefn(Configuration configuration, ServiceComponentDefn serviceComponentDefn, VMComponentDefnID vmID) {
         BasicServiceComponentDefn basicService = (BasicServiceComponentDefn) verifyTargetClass(serviceComponentDefn,BasicServiceComponentDefn.class);
        BasicConfiguration targetConfig = (BasicConfiguration) verifyTargetClass(configuration,BasicConfiguration.class);

        //we must automagically create DeployedComponents for the newly-
        //enabled service defn, wherever its PSC has already been deployed
        DeployedComponent aDeployedComponent = null;
        
         ConfigurationID configID = (ConfigurationID)targetConfig.getID();
        ServiceComponentDefnID serviceDefnID = (ServiceComponentDefnID)basicService.getID();
 
        VMComponentDefn vm = targetConfig.getVMComponentDefn(vmID);
        HostID hostID = vm.getHostID();
        DeployedComponent deployComponent = targetConfig.getDeployedServiceForVM( serviceDefnID, vm);
        
        // if its not deployed, deploy it
        if (deployComponent == null) {
            DeployedComponentID id = new DeployedComponentID(serviceDefnID.getName(), configID,  hostID, vmID, serviceDefnID);
            deployComponent = this.createDeployedServiceComponent(serviceDefnID.getName(), configuration, hostID, vmID, basicService);

        }

        return deployComponent;
    }

}



