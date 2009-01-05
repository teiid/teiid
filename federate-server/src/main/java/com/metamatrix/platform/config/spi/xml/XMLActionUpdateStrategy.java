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

package com.metamatrix.platform.config.spi.xml;


import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.AddNamedObject;
import com.metamatrix.common.actions.AddObject;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.actions.ExchangeBoolean;
import com.metamatrix.common.actions.ExchangeNamedObject;
import com.metamatrix.common.actions.ExchangeObject;
import com.metamatrix.common.actions.RemoveNamedObject;
import com.metamatrix.common.actions.RemoveObject;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.AuthenticationProviderID;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.PropDefnAllowedValueID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.ResourceModel;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.SharedResourceID;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.DuplicateComponentException;
import com.metamatrix.common.config.api.exceptions.InvalidArgumentException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidDeployedComponentException;
import com.metamatrix.common.config.api.exceptions.InvalidPropertyValueException;
import com.metamatrix.common.config.model.BasicComponentType;
import com.metamatrix.common.config.model.BasicComponentTypeDefn;
import com.metamatrix.common.config.model.BasicConfiguration;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicHost;
import com.metamatrix.common.config.model.BasicProductType;
import com.metamatrix.common.config.model.BasicSharedResource;
import com.metamatrix.common.config.model.ConfigurationModel;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.model.ConfigurationObjectEditorHelper;
import com.metamatrix.common.config.model.PropertyValidations;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;


public class XMLActionUpdateStrategy  {


    public PropertyValidations validateProperty = new PropertyValidations();


    public XMLActionUpdateStrategy() {
    }


   public Set executeActionsOnTarget(Object target, List actions, ConfigTransaction transaction )
                           throws ConfigTransactionException,  ConfigurationException {
 //     	System.out.println("STRATEGY: Start Execute on Target " + target + " of type " + target.getClass().getName());


		Set affectedIDs = new HashSet();

        if ( actions.isEmpty() ) {

            return affectedIDs;
        }

        ConfigurationModelContainerImpl model = null;

/**
 * Certain object types, when changed, require that both the
 * NextStartup and Operational Configurations be changed.  Those
 * object types are not considered to be bound by a configuration,
 * all the other types are.  The following are the 2 different
 * categories:
 *
 * Unbound by Configuration:
 * -	Host
 * -	ComponentType/ComponentTypeDefn
 * -	ResourcePool (ResourceDescriptor)
 * -	Resource (ResourceDescriptor)
 * -	Connector Bindings (ComponentDefn/DeployedComponent of ConnectorBindingType)
 *
 * Bound by Configuration
 * -	Configuration
 * -	ServiceComponentDefn(ComponentDefn)/DeployedComponent
 * -	VMComponentDefn(ComponentDefn)/DeployedComponent
 * -	PSC
 */

        if ( target instanceof ConfigurationID ) {
            ConfigurationID id = (ConfigurationID) target;


            executeActions(id, actions, transaction);


        } else if (target instanceof HostID) {
            HostID id = (HostID) target;
			// apply changes to all configurations

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
//					System.out.println("STRATEGY: Update Config " + m.getConfigurationID());

				executeActions(m, id, actions, transaction);
			}

        } else if (target instanceof SharedResourceID) {
            SharedResourceID id = (SharedResourceID) target;
			// apply changes to all configurations

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
//					System.out.println("STRATEGY: Update Config " + m.getConfigurationID());

				executeActions(m, id, actions, transaction);
			}



        } else if (target instanceof ResourceDescriptorID) {
            ResourceDescriptorID id = (ResourceDescriptorID) target;

			Collection trans = transaction.getObjects();

			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();

				executeActions(m, id, actions, transaction);

			}
			
        } else if (target instanceof AuthenticationProviderID) {
        	AuthenticationProviderID id = (AuthenticationProviderID) target;

			Collection trans = transaction.getObjects();

			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();

				executeActions(m, id, actions, transaction);

			}			


        } else if (target instanceof ProductTypeID) {
            ProductTypeID id = (ProductTypeID) target;
            // apply changes to all configurations

            Collection trans = transaction.getObjects();
            for (Iterator it=trans.iterator(); it.hasNext(); ) {
                ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
//                  System.out.println("STRATEGY: Update Config " + m.getConfigurationID());

                executeActions(m, id, actions, transaction);
            }            
            
        } else if (target instanceof ComponentTypeID) {
            ComponentTypeID id = (ComponentTypeID) target;
			Collection trans = transaction.getObjects();
			// apply changes to all configurations

			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions(m, id, actions, transaction);
			}

        } else if (target instanceof ComponentTypeDefnID) {
            ComponentTypeDefnID id = (ComponentTypeDefnID) target;
			Collection trans = transaction.getObjects();

			// apply changes to all configurations
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions(m, id, actions, transaction);
			}

        } else if (target instanceof ConnectorBindingID) {
            ConnectorBindingID id = (ConnectorBindingID) target;
			Collection trans = transaction.getObjects();
//		System.out.println("STRATEGY: Update Connector " + target);

			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions(m, id, actions, transaction);
			}



        } else if (target instanceof DeployedComponentID) {
            DeployedComponentID id = (DeployedComponentID) target;

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions(m, id, actions, transaction);
			}


       } else if (target instanceof ProductServiceConfigID ) {
			ProductServiceConfigID id = (ProductServiceConfigID) target;

//			System.out.println("STRATEGY: Update PSC ComponentDefn " + id);

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions(m, id, actions, transaction);
			}


        } else if (target instanceof VMComponentDefnID) {
            VMComponentDefnID id = (VMComponentDefnID) target;

//			System.out.println("STRATEGY: Update ComponentDefn " + id);

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();

//				System.out.println("STRATEGY: Execute Update ComponentDefn " + id);

				executeActions(m, id, actions, transaction);
			}


        } else if (target instanceof ServiceComponentDefnID) {
            ServiceComponentDefnID id = (ServiceComponentDefnID) target;

//			System.out.println("STRATEGY: Update SvcComponentDefn " + id);

			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
				executeActions( m, id, actions, transaction);

			}
            

        } else if (target instanceof PropDefnAllowedValueID) {
            PropDefnAllowedValueID id = (PropDefnAllowedValueID) target;
			Collection trans = transaction.getObjects();
			for (Iterator it=trans.iterator(); it.hasNext(); ) {
				model = (ConfigurationModelContainerImpl) it.next();

				// deployed comps are not necessarily applied to all configs
//				if (doesCompChangeApply(id, model)) {
					executeActions(model, id, actions, transaction);
//				}
			}

        } else {
            throw new ConfigTransactionException(ConfigMessages.CONFIG_0071, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0071, target));
        }


//	System.out.println("STRATEGY: End Update Target " + target);

		return affectedIDs;
	}

    private ConfigurationModelContainerImpl getConfigModel(BaseID id, ConfigTransaction transaction) throws ConfigurationException {

		String name;
		if (id instanceof ConfigurationID) {
			name = id.getFullName();
		} else {
			name = id.getParentFullName();
		}
		ConfigurationModelContainerImpl container = (ConfigurationModelContainerImpl) transaction.getObject(name);

		if (id instanceof ConfigurationID) {
			// a config model may not exist if a delete was performed
		} else {
    		if (container == null) {
    			throw new ConfigurationException(ConfigMessages.CONFIG_0072, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0072,  name));
    		}
		}


    	return container;
    }
    
    private ComponentType getComponentType(ConfigurationModelContainer config, ComponentTypeID id, BaseID targetID)throws ConfigurationException {
        
        ComponentType type = config.getComponentType(id.getFullName());

        if (type == null) {
            type =  config.getProductType(id.getFullName());
            if (type == null) {
                throw new InvalidComponentException(ConfigPlugin.Util.getString("XMLActionUpdateStrategy.Unable_to_add_component_type_not_found", new Object[] {targetID, id})); //$NON-NLS-1$
            }
        }
        
        return type;

    }
    
//    private ProductType getProductType(ConfigurationModelContainer config, ComponentTypeID id, BaseID targetID)throws ConfigurationException {
//        
//        ProductType type = (ProductType) config.getProductType(id.getFullName());
//
//        if (type == null) {
//                throw new InvalidComponentException(ConfigPlugin.Util.getString("XMLActionUpdateStrategy.Unable_to_add_component_type_not_found", new Object[] {targetID, id})); //$NON-NLS-1$
//        }
//        
//        return type;
//
//    }    


    /**
     * Executes the specified component type actions and will update the list of component types
     * with the changes.
     * @param componentTypes is the configuration to be updated
     */

    public Set executeActions(ConfigurationModelContainerImpl config, ResourceDescriptorID targetID, List actions, ConfigTransaction transaction )
                        throws InvalidConfigurationException, ConfigurationException{

    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: ResourceDescriptor Component Target " + targetID);

        int actionIndex = -1;

        affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            ResourceDescriptor rd = null;
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();
				rd = cfg.getResourcePool(targetID.getName());
                if(action instanceof CreateObject ) {

                    if (rd != null) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0073, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0073,  targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
                    rd = (ResourceDescriptor) args[0];
					rd = (ResourceDescriptor) setCreationDate(rd, transaction.getLockAcquiredBy());

                    ComponentType type = getComponentType(config, rd.getComponentTypeID(), targetID); 

                    //ComponentType type = config.getComponentType(rd.getComponentTypeID().getFullName());

                    // process properties for any encryptions
                    processPropertyForNewObject(rd, type, config, transaction.getLockAcquiredBy());

                    ConfigurationObjectEditorHelper.addConfigurationComponentDefn(cfg, rd);


                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject ||
                			action instanceof ExchangeObject)  {


                 	if (rd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0074, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0074,  targetID));
                	}

//                    ComponentType type = config.getComponentType(rd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, rd.getComponentTypeID(), targetID); 

                    rd = (ResourceDescriptor) setLastChangedDate(rd, transaction.getLockAcquiredBy());

        			processPropertyChanges(action,
    									rd,
    									type,
    									config,
    									transaction.getLockAcquiredBy());




                } else if (action instanceof DestroyObject) {

                    if (rd != null) {
						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());
                    }


                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription() ));
                }

            }

        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;
    }
    
    public Set executeActions(ConfigurationModelContainerImpl config,
			AuthenticationProviderID targetID, List actions,
			ConfigTransaction transaction)
			throws InvalidConfigurationException, ConfigurationException {

		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}
		// System.out.println("STRATEGY: ResourceDescriptor Component Target " +
		// targetID);

		int actionIndex = -1;

		affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			AuthenticationProvider rd = null;
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();
				rd = cfg.getAuthenticationProvider(targetID);
				if (action instanceof CreateObject) {

					if (rd != null) {
						DuplicateComponentException e = new DuplicateComponentException(
								ConfigMessages.CONFIG_0189,
								ConfigPlugin.Util.getString(
										ConfigMessages.CONFIG_0189, targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}
					rd = (AuthenticationProvider) args[0];
					rd = (AuthenticationProvider) setCreationDate(rd, transaction.getLockAcquiredBy());

					ComponentType type = getComponentType(config, rd
							.getComponentTypeID(), targetID);

					// ComponentType type =
					// config.getComponentType(rd.getComponentTypeID().getFullName());

					// process properties for any encryptions
					processPropertyForNewObject(rd, type, config, transaction
							.getLockAcquiredBy());

					ConfigurationObjectEditorHelper
							.addConfigurationComponentDefn(cfg, rd);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject
						|| action instanceof ExchangeObject) {

					if (rd == null) {
						throw new InvalidComponentException(
								ConfigMessages.CONFIG_0190,
								ConfigPlugin.Util.getString(
										ConfigMessages.CONFIG_0190, targetID));
					}

					// ComponentType type =
					// config.getComponentType(rd.getComponentTypeID().getFullName());
					ComponentType type = getComponentType(config, rd
							.getComponentTypeID(), targetID);

					rd = (AuthenticationProvider) setLastChangedDate(rd, transaction.getLockAcquiredBy());

					processPropertyChanges(action, rd, type, config,
							transaction.getLockAcquiredBy());

				} else if (action instanceof DestroyObject) {

					if (rd != null) {
						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());
					}

				} else {
					throw new InvalidArgumentException(
							ConfigMessages.CONFIG_0075, ConfigPlugin.Util
									.getString(ConfigMessages.CONFIG_0191,
											action.getActionDescription()));
				}

			}


		} catch (ConfigurationException ce) {
			throw ce;

		} catch (Exception e) {
			ConfigurationException e2 = new ConfigurationException(e);
			e2.setActionIndex(actionIndex);
			throw e2;
		}

		return affectedIDs;
	}
    


    public  Set executeActions(ConfigurationModelContainerImpl config, DeployedComponentID targetID, List actions, ConfigTransaction transaction) throws InvalidDeployedComponentException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: DeployedComp Target " + targetID + " on config " + config.getConfigurationID());

        int actionIndex = -1;
        affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();


        try {

            // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();
				DeployedComponent dc = cfg.getDeployedComponent(targetID);
                if(action instanceof CreateObject) {

                    if ( dc != null ) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076,  targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }

                    dc = (BasicDeployedComponent) args[0];

					dc = (BasicDeployedComponent) setCreationDate(dc, transaction.getLockAcquiredBy());

                    ComponentType type = getComponentType(config, dc.getComponentTypeID(), targetID); 

				//	ComponentType type = config.getComponentType(dc.getComponentTypeID().getFullName());
                    boolean isDeployable = type.isDeployable();
                    if (!isDeployable) {
                        throw new InvalidDeployedComponentException(ConfigMessages.CONFIG_0077, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0077,  targetID.getName()));
                    }

					ConfigurationObjectEditorHelper.addDeployedComponent(cfg, dc);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject ||
                			action instanceof ExchangeObject)  {

                 	if (dc == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0078, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0078, targetID));
                	}

                   //ComponentType type = config.getComponentType(dc.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, dc.getComponentTypeID(), targetID); 

					dc = (BasicDeployedComponent) setLastChangedDate(dc, transaction.getLockAcquiredBy());

        			processPropertyChanges(action,
    									dc,
    									type,
    									config,
    									transaction.getLockAcquiredBy());

                } else if (action instanceof DestroyObject) {


                    if (dc != null) {
						ConfigurationObjectEditorHelper.delete(targetID, config.getConfiguration());
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0079, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0079, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;

    }


    public  Set executeActions(ConfigurationModelContainerImpl config, ProductServiceConfigID targetID, List actions, ConfigTransaction transaction) throws InvalidDeployedComponentException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: PSC Target " + targetID + " on config " + config.getConfigurationID());

        int actionIndex = -1;

        affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();
				ComponentDefn cd = cfg.getPSC(targetID);

                if(action instanceof CreateObject ) {

                    if (cd!= null) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0080, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0080, targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
                    cd = (ComponentDefn) args[0];

					cd = (ComponentDefn) setCreationDate(cd, transaction.getLockAcquiredBy());


//		System.out.println("STRATEGY: Add PSC Target " + targetID);

                    ConfigurationObjectEditorHelper.addConfigurationComponentDefn(cfg, cd);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject) {

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0081, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0081, targetID));
                	}

                //    ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 
					cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());
                                      

        			processPropertyChanges(action,
    									cd,
    									type,
    									config,
    									transaction.getLockAcquiredBy());


            } else if (action instanceof ExchangeObject)  {
               		ExchangeObject anAction = (ExchangeObject) action;

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0081, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0081, targetID));
                	}

                    //ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 


             		if (anAction.hasAttributeCode() &&
                       (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY.getCode() ||
                        anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode() ) )  {
							cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

		        			processPropertyChanges(action,
		    									cd,
		    									type,
		    									config,
		    									transaction.getLockAcquiredBy());

                                                        
                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.UPDATE_PSC.getCode()) {
                         ProductServiceConfig psc = (ProductServiceConfig) cd;
        
                         psc = ConfigurationObjectEditorHelper.resetServices(psc);
   
                         Collection svcIDs = (Collection) anAction.getPreviousValue();
                         Boolean enabled = (Boolean)anAction.getNewValue();
                    
                    
                         for (Iterator it=svcIDs.iterator(); it.hasNext(); ) {
                             ServiceComponentDefnID id = (ServiceComponentDefnID) it.next();
                             ConfigurationObjectEditorHelper.addServiceComponentDefn(psc, id);
                             ConfigurationObjectEditorHelper.setEnabled( id, psc , enabled.booleanValue());                                                        
                         }
                         setLastChangedDate(psc, transaction.getLockAcquiredBy());                    
                   
 

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {

	                	if (cd instanceof ProductServiceConfig) {
	                		ProductServiceConfig psc = (ProductServiceConfig) cd;

	                		ServiceComponentDefnID svcID = (ServiceComponentDefnID) anAction.getPreviousValue();
                        	Boolean enabled = (Boolean)anAction.getNewValue();

	                		ConfigurationObjectEditorHelper.setEnabled( svcID, psc , enabled.booleanValue());

					 		setLastChangedDate(psc, transaction.getLockAcquiredBy());


	                	} else {

	                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0082, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0082, targetID));

	                	}

                	} else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0079, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0079, anAction.getActionDescription()));
                    }


                } else if (action instanceof DestroyObject) {

                    if (cd != null) {
//		System.out.println("STRATEGY: Destroy ComponentDefn Target " + targetID);

						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }


                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;

    }



    public  Set executeActions(ConfigurationModelContainerImpl config,  ServiceComponentDefnID targetID, List actions, ConfigTransaction transaction) throws InvalidDeployedComponentException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: ComponentDefn Target " + targetID);

        int actionIndex = -1;

        affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();

                ComponentDefn cd = cfg.getServiceComponentDefn(targetID);

                actionIndex++;
                Object args[] = action.getArguments();

                if(action instanceof CreateObject ) {
                    if (cd!= null) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0083, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0083, targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
                    cd = (ComponentDefn) args[0];

					cd = (ComponentDefn) setCreationDate(cd, transaction.getLockAcquiredBy());


                  //  ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

                    // process properties for any encryptions
                    processPropertyForNewObject(cd, type, config, transaction.getLockAcquiredBy());

//		System.out.println("STRATEGY: Add ComponentDefn Target " + targetID);

                    ConfigurationObjectEditorHelper.addConfigurationComponentDefn(cfg, cd);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject)  {

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0084, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0084, targetID));
                	}

                 //   ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 
					cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

        			processPropertyChanges(action,
    									cd,
    									type,
    									config,
    									transaction.getLockAcquiredBy());


                } else if (action instanceof ExchangeObject)  {
                   ExchangeObject anAction = (ExchangeObject) action;

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0084, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0084, targetID));
                	}


                   if (anAction.hasAttributeCode() &&
                       (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY.getCode() ||
                        anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode() ) )  {

		                 //   ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                            ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 
							cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

		        			processPropertyChanges(action,
		    									cd,
		    									type,
		    									config,
		    									transaction.getLockAcquiredBy());

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
                    	throw new InvalidArgumentException(ConfigMessages.CONFIG_0085, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0085, action.getActionDescription(), targetID));

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PSC_NAME.getCode()) {
                    	ProductServiceConfigID pscID = (ProductServiceConfigID)anAction.getNewValue();
						ProductServiceConfig psc = config.getConfiguration().getPSC(pscID);

						 setLastChangedDate(psc, transaction.getLockAcquiredBy());


						// add the service the PSC
						  ConfigurationObjectEditorHelper.addServiceComponentDefn(psc, targetID);

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0086, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0086, anAction.getActionDescription()));

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {

	                	if (cd instanceof ServiceComponentDefn) {
	                		ConfigurationObjectEditorHelper.setRoutingUUID((ServiceComponentDefn) cd, (String) anAction.getNewValue());

				 			setLastChangedDate(cd, transaction.getLockAcquiredBy());


	                	} else {

	                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0087, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0087, action.getActionDescription(), targetID));

	                	}

                    } else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0086, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0086, anAction.getActionDescription()));
                    }




                } else if (action instanceof DestroyObject) {

                	if (cd != null) {
//		System.out.println("STRATEGY: Destroy ServiceDefn Target " + targetID);
						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }


                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;

    }

    public  Set executeActions( ConfigurationModelContainerImpl config,  VMComponentDefnID targetID, List actions, ConfigTransaction transaction) throws InvalidDeployedComponentException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: ComponentDefn Target " + targetID);

        int actionIndex = -1;

        affectedIDs.add(targetID);

		Configuration cfg = config.getConfiguration();

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

				ComponentDefn cd = cfg.getVMComponentDefn(targetID);

                if(action instanceof CreateObject ) {
                    if (cd!= null) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0088, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0088, action.getActionDescription(), targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
                    cd = (ComponentDefn) args[0];
//		System.out.println("STRATEGY: Add ComponentDefn Target " + targetID);

					cd = (ComponentDefn) setCreationDate(cd, transaction.getLockAcquiredBy());

                    //ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

                    // process properties for any encryptions
                    processPropertyForNewObject(cd, type, config, transaction.getLockAcquiredBy());

                    ConfigurationObjectEditorHelper.addConfigurationComponentDefn(cfg, cd);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject)  {

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0089, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0089, targetID));
                	}
//		System.out.println("STRATEGY: Add or Remove object for target " + targetID);

                   // ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 
					cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());


 //		System.out.println("STRATEGY: Got type for " + targetID);
       			processPropertyChanges(action,
    									cd,
    									type,
    									config,
    									transaction.getLockAcquiredBy());

//   		System.out.println("STRATEGY: End of Property changes " + targetID);

                } else if (action instanceof ExchangeObject)  {
                   ExchangeObject anAction = (ExchangeObject) action;

                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0089, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0089, targetID));
                	}
//		System.out.println("STRATEGY: Exchange for Target " + targetID);


                   if (anAction.hasAttributeCode() &&
                       (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY.getCode() ||
                        anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode() ) )  {

		                 //   ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                            ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

 //		System.out.println("STRATEGY: Exchange Properties  " + targetID);
							cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

		        			processPropertyChanges(action,
		    									cd,
		    									type,
		    									config,
		    									transaction.getLockAcquiredBy());

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
                    	throw new InvalidArgumentException(ConfigMessages.CONFIG_0090, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0090, action.getActionDescription(), targetID));

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PSC_NAME.getCode()) {
                   		throw new InvalidArgumentException(ConfigMessages.CONFIG_0090, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0090, action.getActionDescription(), targetID));



                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0091, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));

//		System.out.println("STRATEGY: Exchange enables " + targetID);

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {
//		System.out.println("STRATEGY: Exchange routing  " + targetID);
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0091, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));

                    } else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0091, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));
                    }
                } else if (action instanceof DestroyObject) {

                    if (cd != null) {
//		System.out.println("STRATEGY: Destroy ComponentDefn Target " + targetID);

						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }


                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0091, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;

    }


    public  Set executeActions( ConfigurationModelContainerImpl config,  ConnectorBindingID targetID, List actions, ConfigTransaction transaction) throws InvalidDeployedComponentException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: ConnectorBinding Target " + targetID + " on config " + config.getConfigurationID());

        int actionIndex = -1;

//        BindingAdjustments data = new BindingAdjustments();

        affectedIDs.add(targetID);

 //       ComponentDefn dvt;
		Configuration cfg = config.getConfiguration();

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

				ComponentDefn cd = cfg.getConnectorBinding(targetID);

                if(action instanceof CreateObject ) {

                    if (cd!= null) {
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0092, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0092, targetID));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
                    cd = (ComponentDefn) args[0];

    				cd = (ComponentDefn) setCreationDate(cd, transaction.getLockAcquiredBy());

                //    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

                    // process properties for any encryptions
                    processPropertyForNewObject(cd, type, config, transaction.getLockAcquiredBy());

//		System.out.println("STRATEGY: Add ConnectorBinding Target " + dvt.getID() + " to config " + cfg.getID());

                	ConfigurationObjectEditorHelper.addConfigurationComponentDefn(cfg, cd);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject)  {


                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0093, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0093, targetID));
                	}
//	System.out.println("STRATEGY: Add or Remove ConnectorBinding Property " + dvt.getID() + " to config " + cfg.getID());

               //     ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

                    cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

        			processPropertyChanges(action,
    									cd,
    									type,
    									config,
    									transaction.getLockAcquiredBy());


                } else if (action instanceof ExchangeObject)  {
                   ExchangeObject anAction = (ExchangeObject) action;


                 	if (cd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0093, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0093, targetID));
                	}


                   if (anAction.hasAttributeCode() &&
                       (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY.getCode() ||
                        anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode() ) )  {
// System.out.println("STRATEGY: Exchange ConnectorBinding Properties for Target " + dvt.getID() + " to config " + cfg.getID());

		          //          ComponentType type = config.getComponentType(cd.getComponentTypeID().getFullName());
                            ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID); 

                            cd = (ComponentDefn) setLastChangedDate(cd, transaction.getLockAcquiredBy());

		        			processPropertyChanges(action,
		    									cd,
		    									type,
		    									config,
		    									transaction.getLockAcquiredBy());

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
                    	throw new InvalidArgumentException(ConfigMessages.CONFIG_0094, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0094, action.getActionDescription(),targetID));


                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PSC_NAME.getCode()) {

//	System.out.println("STRATEGY: Exchange ConnectorBinding PSC for Target " + dvt.getID() + " for PSC exchange");

                        ProductServiceConfigID pscID = (ProductServiceConfigID)anAction.getNewValue();
						ProductServiceConfig psc = config.getConfiguration().getPSC(pscID);

						if (psc == null) {
	                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0095, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0095, pscID, config.getConfigurationID()));
						}

				 		 setLastChangedDate(psc, transaction.getLockAcquiredBy());

						// add the service the PSC
						  ConfigurationObjectEditorHelper.addServiceComponentDefn(psc, targetID);


                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0096, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096, anAction.getActionDescription() ));

//System.out.println("STRATEGY: Exchange ConnectorBinding isEnabled for Target " + dvt.getID() + " to config " + cfg.getID());

                    } else if (anAction.hasAttributeCode() && anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {
// System.out.println("STRATEGY: Exchange ConnectorBinding RoutingUUID for Target " + dvt.getID() + " to config " + cfg.getID());

				 		    cd = (ConnectorBinding) setLastChangedDate(cd, transaction.getLockAcquiredBy());

	                		ConfigurationObjectEditorHelper.setRoutingUUID((ConnectorBinding) cd, (String) anAction.getNewValue());

                    } else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0096, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096, anAction.getActionDescription() ));
                    }


                } else if (action instanceof DestroyObject) {
                    if (cd != null) {
//		System.out.println("STRATEGY: Destroy ConnectorBinding Target " + targetID);

						ConfigurationObjectEditorHelper.delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0096, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096, action.getActionDescription() ));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;

    }




    public  Set executeActions( ConfigurationID targetID, List actions, ConfigTransaction transaction) throws InvalidConfigurationException, ConfigurationException{
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: Configuaraton Target " + targetID);

        int actionIndex = -1;

        ConfigurationModelContainerImpl config = getConfigModel(targetID, transaction);


        BasicConfiguration dvt = null;

        affectedIDs.add(targetID);

        try {


            // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

                if (action.hasAttributeCode() &&
                           (action.getAttributeCode().intValue() == ConfigurationModel.Attribute.CURRENT_CONFIGURATION.getCode() ||
                            action.getAttributeCode().intValue() == ConfigurationModel.Attribute.NEXT_STARTUP_CONFIGURATION.getCode() ||
                            action.getAttributeCode().intValue() == ConfigurationModel.Attribute.STARTUP_CONFIGURATION.getCode())        ) {
                  throw new InvalidArgumentException(ConfigMessages.CONFIG_0097, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0097, action.getActionDescription() ));
                }

		// NOTE: These changes are always made within a transaction and
		//		 a configurtation is never deleted without one taking its place.
		//		 And when a delete occurs, a new empty model will take its place.
		//		 Therefore, a model will ALWAYS exists
				if(action instanceof CreateObject ) {
//		System.out.println("STRATEGY: CREATE Configuaraton " + targetID);
                    Object obj = args[0];
                    if (obj instanceof Collection) {
                    	Collection objs = (Collection) obj;
//                    	System.out.println("TRANS  SIZE 1: " + transaction.getObjects().size());

                		ConfigurationModelContainerImpl newConfig = new ConfigurationModelContainerImpl();
						transaction.addObjects(targetID.getFullName(), newConfig);

//          				System.out.println("STRATEGY: # OF CONFIG OBJS: " + objs.size());
               			newConfig.setConfigurationObjects(objs);

                    	for (Iterator it=objs.iterator(); it.hasNext(); ) {
                    		Object o = it.next();
                    	    if (o instanceof ComponentObject) {
                    	    	ComponentObject co = (ComponentObject) o;
                    		//	ComponentType type = newConfig.getComponentType(co.getComponentTypeID().getFullName());
                                ComponentType type = getComponentType(newConfig, co.getComponentTypeID(), targetID); 

                    // process properties for any encryptions
                    			processPropertyForNewObject(co, type, newConfig, transaction.getLockAcquiredBy());
                    	    }

                    	}

//                    	System.out.println("TRANS  SIZE 2: " + transaction.getObjects().size());

                    } else {
                        dvt = (BasicConfiguration) obj;

                    	config.addObject(dvt);

                    }

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject ||
                			action instanceof ExchangeObject)  {

	                	Configuration configuration = config.getConfiguration();

	                 	if (configuration == null) {
	                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0098, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0098, targetID));
	                	}

	                    //ComponentType type = config.getComponentType(configuration.getComponentTypeID().getFullName());
                   //     ComponentType type = getComponentType(config, configuration.getComponentTypeID(), targetID); 
                        ComponentType type = getComponentType(config, configuration.getComponentTypeID(), targetID); 

	        			processPropertyChanges(action,
	    									configuration,
	    									type,
	    									config,
	    									transaction.getLockAcquiredBy());

                } else if (action instanceof DestroyObject) {
//		System.out.println("STRATEGY: DESTROY Configuaraton " + targetID);

                	// create an empty model
                	// the assumption here is there should other actions as part of this
                	// transaction that will load the container before it is committed
                	ConfigurationModelContainerImpl emptyConfig = new ConfigurationModelContainerImpl();
                	transaction.addObjects(targetID.getFullName(), emptyConfig);

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0099, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0099, action.getActionDescription()));
                }

            }  // end of while loop


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {

            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }
        return affectedIDs;



    }


    public  Set executeActions(ConfigurationModelContainerImpl config,ComponentTypeID  targetID, List actions, ConfigTransaction transaction) throws InvalidConfigurationException, ConfigurationException {
        Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: ComponentType Target " + targetID);

        int actionIndex = -1;

        affectedIDs.add(targetID);

        try {

            // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();

                actionIndex++;
                Object args[] = action.getArguments();

                if(action instanceof CreateObject) {

                	ComponentType type = config.getComponentType(targetID.getFullName());
 
                    if ( type != null ) {
//                    	 System.out.println(ConfigPlugin.Util.getString(LogMessageKeys.CONFIG_0005, targetID));

                    	// because types are not configuration bound, the create
                    	// for the type will only be added where it does not exist
                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0100, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0100,targetID ));
                        e.setActionIndex(actionIndex);
                        throw e;
                    }
 //					System.out.println("STRATEGY: Add ComponentType Target " + targetID);

                    BasicComponentType dvt = (BasicComponentType) args[0];

					setCreationDate(dvt, transaction.getLockAcquiredBy());

                    config.addComponentType(dvt);

					if (dvt.isOfTypeConnector()) {
						// the new type must be added to the connector product type.
	                    ProductType pt = config.getProductType(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME);
                        ConfigurationObjectEditorHelper.addServiceComponentType(pt, dvt);
					}

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject) {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));


                } else if (action instanceof ExchangeObject) {
                    ExchangeObject exchangeAction = (ExchangeObject) action;


                	ComponentType type = config.getComponentType(targetID.getFullName());

					if (type == null) {
                        throw new InvalidDeployedComponentException(ConfigMessages.CONFIG_0102,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0102,targetID));
                    }

                	BasicComponentType bct = (BasicComponentType) type;

                    if (action.hasAttributeCode() && action.getAttributeCode().intValue() == ConfigurationModel.Attribute.UPDATE_COMPONENT_TYPE.getCode()) {

                        ComponentType newCt = (ComponentType)exchangeAction.getNewValue();
                        setLastChangedDate(newCt, transaction.getLockAcquiredBy());
                        
                        config.addComponentType((ComponentType)exchangeAction.getNewValue());
                    

                    } else if (action.hasAttributeCode() && action.getAttributeCode().intValue() == ConfigurationModel.Attribute.PARENT_COMPONENT_TYPEID.getCode()) {

						bct.setParentComponentTypeID((ComponentTypeID)exchangeAction.getNewValue());

				 		setLastChangedDate(bct, transaction.getLockAcquiredBy());


                    } else if (action.hasAttributeCode() && action.getAttributeCode().intValue() == ConfigurationModel.Attribute.SUPER_COMPONENT_TYPEID.getCode()){

						bct.setSuperComponentTypeID((ComponentTypeID)exchangeAction.getNewValue());

				 		setLastChangedDate(bct, transaction.getLockAcquiredBy());

                    } else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));
                    }
                } else if (action instanceof ExchangeBoolean) {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));

                } else if (action instanceof DestroyObject) {

                	ComponentType type = config.getComponentType(targetID.getFullName());

					if (type != null) {
                        
                        Collection services = config.getConfiguration().getComponentDefnIDs((ComponentTypeID) type.getID());
                        if (services != null && services.size() > 0) {
                            ComponentDefnID d = (ComponentDefnID) services.iterator().next();
                            throw new InvalidDeployedComponentException(ConfigPlugin.Util.getString("XMLActionUpdateStrategy.Unable_to_delete_component_type_related_components_found", new Object[] {type.getID(), d.getFullName()})); //$NON-NLS-1$
                            
                        }
                        
						config.remove(targetID);

						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());


//						if (type instanceof ProductType) {
//
//						} else {
							// if its not a product type then the regular component
							// type needs to be removed the its related product type
							Iterator it = config.getProductTypes().iterator();
							while(it.hasNext()) {
								ProductType pt = (ProductType) it.next();
								if (pt.contains((ComponentTypeID) type.getID())) {
                                    ConfigurationObjectEditorHelper.removeServiceComponentType(pt, type);
								}
							}

//						}



                    }

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));
                }
            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {

            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }

        return affectedIDs;

    }


    public  Set executeActions(ConfigurationModelContainerImpl config, ComponentTypeDefnID  targetID, List actions, ConfigTransaction transaction) throws InvalidConfigurationException, ConfigurationException {

	/**
	 * This method is used for the mass importing of a configuration, instead of
	 * recreating every object using the editor.
	 *
	 * NOTE: DO NOT add calls for setting history in this method because the importing
	 * should retain what the original file contained.
	 */
        Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }

        int actionIndex = -1;
        affectedIDs.add(targetID);

        try {


            // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

                BasicComponentTypeDefn dvt = (BasicComponentTypeDefn) args[0];
//                ComponentTypeID typeID = dvt.getComponentTypeID();

        		//ComponentType type = config.getComponentType(typeID.getFullName());

                ComponentType type = getComponentType(config, dvt.getComponentTypeID(), targetID); 
               
//				if (type == null) {
//                    throw new InvalidDeployedComponentException(ConfigMessages.CONFIG_0102,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0102,typeID));
//                }


                if(action instanceof CreateObject) {
                	if (type.getComponentTypeDefinition(dvt.getFullName()) != null) {
                		// this check is added because when the editor is used by the importer,
                		// it creates actions for this defn where the component type
                		// that it added already contains the definitions.
                		// Therefore, only throw an exception if it is different.
                		ComponentTypeDefn ctd = type.getComponentTypeDefinition(dvt.getFullName());
                		if (dvt.equals(ctd)) {
                		} else {
	                        DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0104,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0104,targetID));
    	                    e.setActionIndex(actionIndex);
        	                throw e;
                		}
                    }

	                BasicComponentType btype = (BasicComponentType) type;
	                btype.addComponentTypeDefinition(dvt);


              } else if (action instanceof ExchangeObject) {
              		BasicComponentType btype = (BasicComponentType) type;

                	if (type.getComponentTypeDefinition(dvt.getFullName()) == null) {
                  		btype.removeComponentTypeDefinition(dvt);
                    }

	                btype.addComponentTypeDefinition(dvt);
                } else if (action instanceof DestroyObject) {
                	if (type.getComponentTypeDefinition(dvt.getFullName()) == null) {

                    } else {

	                  	BasicComponentType btype = (BasicComponentType) type;
    	              	btype.removeComponentTypeDefinition(dvt);
                	}


                } else {
                        throw new InvalidArgumentException(ConfigMessages.CONFIG_0103,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0103,action.getActionDescription() ));
                }
            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {

            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }

        return affectedIDs;


     }


    public  Set executeActions(ConfigurationModelContainerImpl config,PropDefnAllowedValueID targetID, List actions, ConfigTransaction transaction ) throws InvalidConfigurationException, ConfigurationException {
    Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }

        affectedIDs.add(targetID);



        return affectedIDs;
    }




    public  Set executeActions(ConfigurationModelContainerImpl config, HostID targetID, List actions, ConfigTransaction transaction) throws InvalidComponentException, ConfigurationException{
        Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }
//		System.out.println("STRATEGY: Update Host " + targetID);

        int actionIndex = -1;
        BasicHost dvt = null;

        affectedIDs.add(targetID);

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

                if(action instanceof CreateObject ) {
					Host host = config.getHost(targetID.getFullName());
                    if (host != null) {
                    	// because hosts are not configuration bound, the create
                    	// for the hosts will only be added where it does not exist
                    	continue;

//                        DuplicateComponentException e = new DuplicateComponentException("The host (\"" + targetID + "\") already exists and may not be recreated");
//                        e.setActionIndex(actionIndex);
//                        throw e;
                    }
                    dvt = (BasicHost) args[0];

                    // call to validate the type exists
                    getComponentType(config, dvt.getComponentTypeID(), targetID); 


				 	dvt = (BasicHost) setCreationDate(dvt, transaction.getLockAcquiredBy());

                    ConfigurationObjectEditorHelper.addHostComponent(config.getConfiguration(), dvt);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject ||
                			action instanceof ExchangeObject)  {


					Host host = config.getHost(targetID.getFullName());
                 	if (host == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0105,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0105,targetID));
                	}

                 //   ComponentType type = config.getComponentType(host.getComponentTypeID().getFullName());
                    ComponentType type = getComponentType(config, host.getComponentTypeID(), targetID); 

                    host = (Host) setLastChangedDate(host, transaction.getLockAcquiredBy());

        			processPropertyChanges(action,
    									host,
    									type,
    									config,
    									transaction.getLockAcquiredBy());


                } else if (action instanceof DestroyObject) {
					Host host = config.getHost(targetID.getFullName());

                    if (host != null) {
                    	// if the host is deleted, so must the dependent objects be deleted (if they exist)
						ConfigurationObjectEditorHelper.delete(targetID, config.getConfiguration());
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());
                    }


                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }
        return affectedIDs;

    }


   /**
     * Executes the specified component type actions and will update the list of component types
     * with the changes.
     * @param componentTypes is the configuration to be updated
     */

    public Set executeActions(ConfigurationModelContainerImpl config, SharedResourceID targetID, List actions, ConfigTransaction transaction )
                        throws InvalidConfigurationException, ConfigurationException{

    	Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }

        int actionIndex = -1;

        affectedIDs.add(targetID);
        BasicSharedResource dvt = null;

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

                if(action instanceof CreateObject ) {
                	SharedResource rd = config.getResource(targetID.getFullName());

                    if (rd != null) {
                    	// because rd are not configuration bound, the create
                    	// for the rd will only be added where it does not exist
                    	continue;

//                        DuplicateComponentException e = new DuplicateComponentException("The host (\"" + targetID + "\") already exists and may not be recreated");
//                        e.setActionIndex(actionIndex);
//                        throw e;
                    }
                    dvt = (BasicSharedResource) args[0];

					dvt = (BasicSharedResource) setCreationDate(dvt, transaction.getLockAcquiredBy());

                    ComponentType type = ResourceModel.getComponentType(dvt.getName());

                    // process properties for any encryptions
                    processPropertyForNewObject(dvt, type, config, transaction.getLockAcquiredBy());

                    config.addResource(dvt);

                } else if (action instanceof AddObject ||
                			action instanceof RemoveObject ||
                			action instanceof ExchangeObject)  {

                	SharedResource rd = config.getResource(targetID.getFullName());

                 	if (rd == null) {
                    	throw new InvalidComponentException(ConfigMessages.CONFIG_0106,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0106, targetID));
                	}

                    // get the type based on the SharedResource Name, not its type
                    ComponentType type = ResourceModel.getComponentType(rd.getName());

              //  	ComponentType type = config.getComponentType(rd.getComponentTypeID().getFullName());

               //     ComponentType type = getComponentType(config, rd.getComponentTypeID(), targetID); 
                    
                    rd = (SharedResource)setLastChangedDate(rd, transaction.getLockAcquiredBy());

    				processPropertyChanges(action,
									rd,
									type,
									config,
									transaction.getLockAcquiredBy());



                } else if (action instanceof DestroyObject) {
                	SharedResource rd = config.getResource(targetID.getFullName());

                    if (rd != null) {
						config.remove(targetID);
						setLastChangedDate(config.getConfiguration(), transaction.getLockAcquiredBy());

                    }

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;
    }

    public void updateSharedResource(SharedResource resource, ConfigTransaction transaction) throws  ConfigurationException{

		Collection trans = transaction.getObjects();
		for (Iterator it=trans.iterator(); it.hasNext(); ) {
			ConfigurationModelContainerImpl m = (ConfigurationModelContainerImpl) it.next();
//				System.out.println("STRATEGY: Update SharedResource " + m.getConfigurationID());

            
            ComponentType type = ResourceModel.getComponentType(resource.getName());

            Properties props = resource.getProperties();
            
            
			SharedResource sr = m.getResource(resource.getFullName());
            if (sr == null) {
                    throw new InvalidComponentException(ConfigMessages.CONFIG_0106,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0106, resource.getFullName()));
            }

 //           if (ResourceModel.doesResourceRequireEncryption(sr.getName())) {
                updateProperties(sr, props, ConfigurationObjectEditor.ADD, type, m, transaction.getLockAcquiredBy());
//             } else {
//                 
//                ConfigurationObjectEditorHelper.modifyProperties(sr, resource.getProperties(), ConfigurationObjectEditorHelper.ADD);
//
//             }

			sr = (SharedResource) setLastChangedDate(sr, transaction.getLockAcquiredBy());



		}

    }
    
    /**
     * Executes the specified component type actions and will update the list of component types
     * with the changes.
     * @param componentTypes is the configuration to be updated
     */

    public Set executeActions(ConfigurationModelContainerImpl config, ProductTypeID targetID, List actions, ConfigTransaction transaction )
                        throws InvalidConfigurationException, ConfigurationException{

        Set affectedIDs = new HashSet();
        if ( actions.isEmpty() ) {
            return affectedIDs;
        }

        int actionIndex = -1;

        affectedIDs.add(targetID);
        BasicProductType dvt = null;

        try {

           // Iterate through the actions ...
            Iterator iter = actions.iterator();
            while ( iter.hasNext() ) {
                ActionDefinition action = (ActionDefinition) iter.next();
                actionIndex++;
                Object args[] = action.getArguments();

                if(action instanceof CreateObject ) {
                    ProductType rd = config.getProductType(targetID.getFullName());

                    if (rd != null) {
                        // because rd are not configuration bound, the create
                        // for the rd will only be added where it does not exist
                        continue;
                    }
                    dvt = (BasicProductType) args[0];

                    dvt = (BasicProductType) setCreationDate(dvt, transaction.getLockAcquiredBy());


                    config.addProductType(dvt);

                } else if (action instanceof AddObject ||
                            action instanceof RemoveObject ||
                            action instanceof ExchangeObject)  {
                    
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                    
//
//                    ProductType rd = config.getProductType(targetID.getFullName());
//
//                    if (rd == null) {
//                        throw new InvalidComponentException(ConfigMessages.CONFIG_0106,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0106, targetID));
//                    }
//
//
//                    ComponentType type = config.getComponentType(rd.getComponentTypeID().getFullName());
//                    rd = (SharedResource)setLastChangedDate(editor, rd, transaction.getLockAcquiredBy());
//
//                    processPropertyChanges(action,
//                                    rd,
//                                    type,
//                                    config,
//                                    transaction.getLockAcquiredBy());
//
//

                } else if (action instanceof DestroyObject) {
                    // CANNOT REMOVE PRODUCT TYPES, they are predetermined for each release
                    
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                    
//                    ProductType rd = config.getProductType(targetID.getFullName());
//
//                    if (rd != null) {
//                        config.remove(targetID);
//                        setLastChangedDate(editor, config.getConfiguration(), transaction.getLockAcquiredBy());
//
//                    }

                } else {
                    throw new InvalidArgumentException(ConfigMessages.CONFIG_0075,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075, action.getActionDescription()));
                }

            }


        } catch (ConfigurationException ce) {
            throw ce;

        } catch ( Exception e ) {
            ConfigurationException e2 = new ConfigurationException(e);
            e2.setActionIndex(actionIndex);
            throw e2;
        }


        return affectedIDs;
    }
    

    // ----------------------------------------------------------------------------------------
    //                P R O P E R T Y    M E T H O D S
    // ----------------------------------------------------------------------------------------

    private  void processPropertyForNewObject(ComponentObject object,
    									ComponentType type,
    									ConfigurationModelContainerImpl config,
    									String principal)
                                   throws InvalidPropertyValueException, ConfigurationException {

// because new objects may no longer have seperate actions for their properties, the
// properties that they have must be checked for passwords so that
// they can be encrypted.
        Properties props = ConfigurationObjectEditorHelper.getEditableProperties(object);

        updateProperties(object, props, ConfigurationObjectEditor.SET, type, config, principal);

   }

    private  void processPropertyChanges(ActionDefinition action,
    									ComponentObject object,
    									ComponentType type,
    									ConfigurationModelContainerImpl config,
    									String principal)
                                   throws InvalidPropertyValueException, ConfigurationException {

        int operation = -1;
        String propName;
        String propValue;
 //		System.out.println("STRATEGY: Process Property Changes" + object.getFullName());

        if(action instanceof AddNamedObject ||
            action instanceof AddObject) {
            operation = ConfigurationObjectEditor.ADD;
        } else if (action instanceof ExchangeNamedObject ||
                    action instanceof ExchangeObject) {
            operation = ConfigurationObjectEditor.SET;
        } else if (action instanceof RemoveNamedObject ||
                    action instanceof RemoveObject) {
            operation = ConfigurationObjectEditor.REMOVE;
        } else {
            throw new RuntimeException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0107, action.getClass().getName()));
        }

        Object args[] = action.getArguments();

        /*
        System.out.println("<!><!>Properties processing action " + action + " with args:");
        for (int i= 0; i<args.length; i++){
            System.out.println(i + ": " + args[i]);
        }
        */
// 		System.out.println("STRATEGY: Process Property Changes 1" );

        if (args[0] instanceof Properties) {

            Properties props = null;
            if (operation == ConfigurationObjectEditor.ADD) {
                props = (Properties) args[0];
            } else if (operation == ConfigurationObjectEditor.SET) {
                // 0 arg is the old value
                // 1 arg is the new value
                props = (Properties) args[1];
            } else if (operation == ConfigurationObjectEditor.REMOVE) {
                props = (Properties) args[0];
            }

// 		System.out.println("STRATEGY: Process Property Changes 2" );

            updateProperties(object, props, operation, type, config, principal);

        } else {
            propName = (String) args[0];

            propValue = ""; //$NON-NLS-1$

            if (operation == ConfigurationObjectEditor.ADD) {
                propValue = (String) args[1];
            } else if (operation == ConfigurationObjectEditor.SET) {
                propValue = (String) args[2];
            } else if (operation == ConfigurationObjectEditor.REMOVE) {
                propValue = ""; //$NON-NLS-1$
            }
//		System.out.println("STRATEGY: Process Property Changes 3" );

            updateProperty(object, propName, propValue, operation, type, config, principal);
            // Must encrypt connection passwords here
            // "targetObj instanceof ServiceComponentDefnID" indicates it's a connector binding
        }

    }

    private void updateProperties(ComponentObject object,
    									Properties props,
    									int operation,
    									ComponentType type,
    									ConfigurationModelContainerImpl config,
    									String principal) throws InvalidPropertyValueException, ConfigurationException {

        String propName;
        String propValue;

            Enumeration propertyNames = props.propertyNames();
            Properties passwordProps = new Properties();
            
            while ( propertyNames.hasMoreElements() ) {

                propName = (String) propertyNames.nextElement();
                propValue = props.getProperty(propName);
                
                validateProperty.isPropertyValid(propName, propValue);
                // Must encrypt connection passwords here

                 if (propValue != null && propValue.trim().length() > 0 &&
                    isPasswordProp(propName, type, config) &&
                    ! CryptoUtil.isValueEncrypted(propValue) ) {
                    try {

                    	propValue = CryptoUtil.stringEncrypt(propValue);
                    } catch ( CryptoException e ) {
                    	e.printStackTrace();
                        throw new InvalidPropertyValueException(e,ConfigMessages.CONFIG_0108, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0108));
                    }
                }
                passwordProps.setProperty(propName, propValue);

            }

			ConfigurationObjectEditorHelper.modifyProperties(object, passwordProps, operation);

    }

    private void updateProperty(ComponentObject object,
    									String propName,
    									String propValue,
    									int operation,
    									ComponentType type,
    									ConfigurationModelContainerImpl config,
    									String principal) throws InvalidPropertyValueException, ConfigurationException {
// 		System.out.println("STRATEGY: Process Property Changes 5 " + propName + " value: " + propValue );

           validateProperty.isPropertyValid(propName, propValue);

            if (propValue != null && propValue.trim().length() > 0 &&
                isPasswordProp(propName, type, config) &&
                ! CryptoUtil.isValueEncrypted(propValue)) {
	            char[] pwd = null;
	            try {
//		System.out.println("STRATEGY: Process Property Changes 5a" );

	            	propValue = CryptoUtil.getCryptor().encrypt(propValue);
	            } catch ( CryptoException e ) {
	                throw new InvalidPropertyValueException(e, ConfigMessages.CONFIG_0108, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0108));
	            }
            }

//		System.out.println("STRATEGY: Process Property Changes 5b " + propName + " value: " + propValue );

            if (operation == ConfigurationObjectEditor.ADD) {
                ConfigurationObjectEditorHelper.addProperty(object, propName, propValue);

            } else if (operation == ConfigurationObjectEditor.SET) {
                ConfigurationObjectEditorHelper.setProperty(object, propName, propValue);

            } else if (operation == ConfigurationObjectEditor.REMOVE) {
            	ConfigurationObjectEditorHelper.removeProperty(object, propName);

            }
// 		System.out.println("STRATEGY: Process Property Changes 6" );

    }

    /**
     *  Determines if the given propDefn is a password property (ISMASKED == 1).
     */
    private static boolean isPasswordProp(String  propName, ComponentType type, ConfigurationModelContainerImpl config)
            throws ConfigurationException {

		if(type == null){
            Assertion.isNotNull(type, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0109, propName));
        }

		ComponentType checkType = null;

		if (type.getComponentTypeDefinition(propName) != null) {
			checkType = type;
		} else {
			checkType = findComponentTypeBasedOnHierarchy(propName, type, INITIAL_TREE_PATH, config);
		}

	 	if (checkType == null) {
	 		return false;
	 	}

//		Assertion.isNotNull(checkType.getComponentTypeDefinition(propName), "No component type definition for " + propName + " in component type " + type.getFullName());
        ComponentTypeDefn td = checkType.getComponentTypeDefinition(propName);

        if (td.getPropertyDefinition().isMasked()) {
        	return true;
        }

        return false;
    }

    /**
     * This methods traverses the component type parent or super types heirarchy.
     * This is a recursive method used to traverse the tree.
     * NOTE: the treePath is used to ensure only one path is used and not
     *     allow it to cross over and use the other path.  An example
     *     would be the "Service" component type that has a parent type
     *     but is also the super type of all the services.
     *
     */
    private static final int INITIAL_TREE_PATH = -1;
    private static final int PARENT_TREE_PATH = 1;
    private static final int SUPER_TREE_PATH = 2;

    private static ComponentType findComponentTypeBasedOnHierarchy(String propName, ComponentType type, int treePath, ConfigurationModelContainerImpl config) throws ConfigurationException {
        ComponentType foundType = null;

// first try down the parent path to find the definition for the propName
       if ( (treePath == PARENT_TREE_PATH || treePath == INITIAL_TREE_PATH) &&
	           type.getParentComponentTypeID() != null) {

            foundType = config.getComponentType(type.getParentComponentTypeID().getFullName());

            // not all parents or supers may be component types, but product types - that have no definitions
            if (foundType != null) {
            // if this parent doesn't contain the definition then goto its parent
				if (foundType.getComponentTypeDefinition(propName) != null) {

					return foundType;
				}

			// dont want to return here because at the intial call of this method
			// and the parent type doesnt have the defn, the super has to be checked
				foundType = findComponentTypeBasedOnHierarchy(propName, foundType, PARENT_TREE_PATH, config);

				if (foundType != null) {
					return foundType;
				}

            }
        }


        if ( (treePath == SUPER_TREE_PATH || treePath == INITIAL_TREE_PATH) &&
              type.getSuperComponentTypeID() != null) {

            foundType = config.getComponentType(type.getSuperComponentTypeID().getFullName());

            if (foundType != null) {
            // if this parent doesn't contain the definition then goto its parent
				if (foundType.getComponentTypeDefinition(propName) != null) {
					return foundType;
				}

				foundType = findComponentTypeBasedOnHierarchy(propName, foundType, SUPER_TREE_PATH, config);

				if (foundType != null) {
					return foundType;
				}
            }

        }

        return foundType;

    }

   private ComponentObject setLastChangedDate(ComponentObject defn, String principal) {

   		String lastChangedDate = DateUtil.getCurrentDateAsString();

   		return ConfigurationObjectEditorHelper.setLastChangedHistory(defn, principal, lastChangedDate);
   }

   private ComponentType setLastChangedDate(ComponentType defn, String principal) {

   		String lastChangedDate = DateUtil.getCurrentDateAsString();

   		return ConfigurationObjectEditorHelper.setLastChangedHistory(defn, principal, lastChangedDate);
   }


   private ComponentObject setCreationDate(ComponentObject defn, String principal) {

   		String creationDate = DateUtil.getCurrentDateAsString();

   		defn = ConfigurationObjectEditorHelper.setLastChangedHistory(defn, principal, creationDate);

   		return ConfigurationObjectEditorHelper.setCreationChangedHistory(defn, principal, creationDate);
   }

   private ComponentType setCreationDate(ComponentType defn, String principal) {

   		String creationDate = DateUtil.getCurrentDateAsString();

   		defn = ConfigurationObjectEditorHelper.setLastChangedHistory(defn, principal, creationDate);

   		return ConfigurationObjectEditorHelper.setCreationChangedHistory(defn, principal, creationDate);
   }

}





