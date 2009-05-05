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
import com.metamatrix.common.config.api.ComponentObjectID;
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
import com.metamatrix.common.config.api.PropDefnAllowedValueID;
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
import com.metamatrix.common.config.model.BasicComponentObject;
import com.metamatrix.common.config.model.BasicComponentType;
import com.metamatrix.common.config.model.BasicComponentTypeDefn;
import com.metamatrix.common.config.model.BasicConfiguration;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicHost;
import com.metamatrix.common.config.model.BasicServiceComponentDefn;
import com.metamatrix.common.config.model.BasicSharedResource;
import com.metamatrix.common.config.model.BasicUtil;
import com.metamatrix.common.config.model.ConfigurationModel;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.model.PropertyValidations;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.security.audit.AuditManager;

public class XMLActionUpdateStrategy {

	public PropertyValidations validateProperty = new PropertyValidations();

	public XMLActionUpdateStrategy() {
	}

	Set executeActionsOnTarget(Object target, List actions, ConfigurationModelContainerImpl config, String principal, XMLConfigurationConnector transaction) throws ConfigurationException {

		Set affectedIDs = new HashSet();

		if (actions.isEmpty()) {
			return affectedIDs;
		}

		/**
		 * Certain object types, when changed, require that both the NextStartup
		 * and Operational Configurations be changed. Those object types are not
		 * considered to be bound by a configuration, all the other types are.
		 * The following are the 2 different categories:
		 * 
		 * Unbound by Configuration: - Host - ComponentType/ComponentTypeDefn -
		 * ResourcePool (ResourceDescriptor) - Resource (ResourceDescriptor) -
		 * Connector Bindings (ComponentDefn/DeployedComponent of
		 * ConnectorBindingType)
		 * 
		 * Bound by Configuration - Configuration -
		 * ServiceComponentDefn(ComponentDefn)/DeployedComponent -
		 * VMComponentDefn(ComponentDefn)/DeployedComponent - PSC
		 */

		if (target instanceof ConfigurationID) {
			ConfigurationID id = (ConfigurationID) target;
			executeActions(id, actions, config, principal, transaction);
		} else if (target instanceof HostID) {
			HostID id = (HostID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof SharedResourceID) {
			SharedResourceID id = (SharedResourceID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof AuthenticationProviderID) {
			AuthenticationProviderID id = (AuthenticationProviderID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof ComponentTypeID) {
			ComponentTypeID id = (ComponentTypeID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof ComponentTypeDefnID) {
			ComponentTypeDefnID id = (ComponentTypeDefnID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof ConnectorBindingID) {
			ConnectorBindingID id = (ConnectorBindingID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof DeployedComponentID) {
			DeployedComponentID id = (DeployedComponentID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof VMComponentDefnID) {
			VMComponentDefnID id = (VMComponentDefnID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof ServiceComponentDefnID) {
			ServiceComponentDefnID id = (ServiceComponentDefnID) target;
			executeActions(id, actions, config, principal);
		} else if (target instanceof PropDefnAllowedValueID) {
			PropDefnAllowedValueID id = (PropDefnAllowedValueID) target;
			executeActions(id, actions);
		} else {
			throw new ConfigurationException(ConfigMessages.CONFIG_0071,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0071,target));
		}
		return affectedIDs;
	}

	private ComponentType getComponentType(ConfigurationModelContainer config,
			ComponentTypeID id, BaseID targetID) throws ConfigurationException {
		ComponentType type = config.getComponentType(id.getFullName());

		if (type == null) {
			throw new InvalidComponentException(ConfigPlugin.Util.getString("XMLActionUpdateStrategy.Unable_to_add_component_type_not_found", new Object[] { targetID, id })); //$NON-NLS-1$
		}
		return type;
	}

	private Set executeActions(AuthenticationProviderID targetID, List actions,
			ConfigurationModelContainerImpl config, String principal)
			throws InvalidConfigurationException, ConfigurationException {

		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;

		affectedIDs.add(targetID);

		BasicConfiguration cfg = (BasicConfiguration) config.getConfiguration();

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
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0189, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0189,targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}
					rd = (AuthenticationProvider) args[0];
					rd = (AuthenticationProvider) setCreationDate(rd, principal);

					ComponentType type = getComponentType(config, rd.getComponentTypeID(), targetID);

					// process properties for any encryptions
					processPropertyForNewObject(rd, type, config, principal);

					cfg.addComponentDefn(rd);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject
						|| action instanceof ExchangeObject) {

					if (rd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0190, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0190,targetID));
					}

					ComponentType type = getComponentType(config, rd
							.getComponentTypeID(), targetID);

					rd = (AuthenticationProvider) setLastChangedDate(rd,
							principal);

					processPropertyChanges(action, rd, type, config, principal);

				} else if (action instanceof DestroyObject) {

					if (rd != null) {
						delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), principal);
					}
				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0191,action.getActionDescription()));
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

	private Set executeActions(DeployedComponentID targetID, List actions, ConfigurationModelContainerImpl config, String principal)
			throws InvalidDeployedComponentException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;
		affectedIDs.add(targetID);

		BasicConfiguration cfg = (BasicConfiguration) config.getConfiguration();

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();
				DeployedComponent dc = cfg.getDeployedComponent(targetID);
				if (action instanceof CreateObject) {

					if (dc != null) {
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076,targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}

					dc = (BasicDeployedComponent) args[0];

					dc = (BasicDeployedComponent) setCreationDate(dc, principal);

					ComponentType type = getComponentType(config, dc.getComponentTypeID(), targetID);

					boolean isDeployable = type.isDeployable();
					if (!isDeployable) {
						throw new InvalidDeployedComponentException(ConfigMessages.CONFIG_0077, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0077,targetID.getName()));
					}

					cfg.addDeployedComponent(dc);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject
						|| action instanceof ExchangeObject) {

					if (dc == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0078, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0078,targetID));
					}

					ComponentType type = getComponentType(config, dc.getComponentTypeID(), targetID);

					dc = (BasicDeployedComponent) setLastChangedDate(dc,principal);

					processPropertyChanges(action, dc, type, config, principal);

				} else if (action instanceof DestroyObject) {

					if (dc != null) {
						delete(targetID, config.getConfiguration());
						setLastChangedDate(config.getConfiguration(), principal);
					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0079, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0079,action.getActionDescription()));
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

	private Set executeActions(ServiceComponentDefnID targetID, List actions,
			ConfigurationModelContainerImpl config, String principal)
			throws InvalidDeployedComponentException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}
		// System.out.println("STRATEGY: ComponentDefn Target " + targetID);

		int actionIndex = -1;

		affectedIDs.add(targetID);

		BasicConfiguration cfg = (BasicConfiguration) config.getConfiguration();

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();

				ComponentDefn cd = cfg.getServiceComponentDefn(targetID);

				actionIndex++;
				Object args[] = action.getArguments();

				if (action instanceof CreateObject) {
					if (cd != null) {
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0083, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0083,targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}
					cd = (ComponentDefn) args[0];

					cd = (ComponentDefn) setCreationDate(cd, principal);

					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);

					// process properties for any encryptions
					processPropertyForNewObject(cd, type, config, principal);

					cfg.addComponentDefn(cd);

				} else if (action instanceof AddObject || action instanceof RemoveObject) {

					if (cd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0084, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0084,targetID));
					}

					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);
					cd = (ComponentDefn) setLastChangedDate(cd, principal);

					processPropertyChanges(action, cd, type, config, principal);

				} else if (action instanceof ExchangeObject) {
					ExchangeObject anAction = (ExchangeObject) action;

					if (cd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0084, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0084,targetID));
					}

					if (anAction.hasAttributeCode()
							&& (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY
									.getCode() || anAction.getAttributeCode()
									.intValue() == ConfigurationModel.Attribute.PROPERTIES
									.getCode())) {

						ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);
						cd = (ComponentDefn) setLastChangedDate(cd, principal);

						processPropertyChanges(action, cd, type, config,principal);

					} else if (anAction.hasAttributeCode()
							&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME
									.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0085, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0085,action.getActionDescription(),targetID));
					
					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0086,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0086, anAction.getActionDescription()));

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {

						if (cd instanceof ServiceComponentDefn) {
							setRoutingUUID((ServiceComponentDefn) cd,(String) anAction.getNewValue());
							setLastChangedDate(cd, principal);
						} else {

							throw new InvalidComponentException(ConfigMessages.CONFIG_0087,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0087, action.getActionDescription(),targetID));
						}

					} else {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0086,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0086, anAction.getActionDescription()));
					}

				} else if (action instanceof DestroyObject) {

					if (cd != null) {
						delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), principal);
					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075,action.getActionDescription()));
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

	private Set executeActions(VMComponentDefnID targetID, List actions, ConfigurationModelContainerImpl config, String principal)
			throws InvalidDeployedComponentException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;

		affectedIDs.add(targetID);

		BasicConfiguration cfg = (BasicConfiguration) config.getConfiguration();

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				ComponentDefn cd = cfg.getVMComponentDefn(targetID);

				if (action instanceof CreateObject) {
					if (cd != null) {
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0088, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0088,action.getActionDescription(),targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}
					cd = (ComponentDefn) args[0];

					cd = (ComponentDefn) setCreationDate(cd, principal);

					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);

					// process properties for any encryptions
					processPropertyForNewObject(cd, type, config, principal);

					cfg.addComponentDefn(cd);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject) {

					if (cd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0089, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0089,targetID));
					}
					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);
					cd = (ComponentDefn) setLastChangedDate(cd, principal);

					processPropertyChanges(action, cd, type, config, principal);

				} else if (action instanceof ExchangeObject) {
					ExchangeObject anAction = (ExchangeObject) action;

					if (cd == null) {
						throw new InvalidComponentException(
								ConfigMessages.CONFIG_0089, ConfigPlugin.Util
										.getString(ConfigMessages.CONFIG_0089,
												targetID));
					}

					if (anAction.hasAttributeCode()
							&& (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY
									.getCode() || anAction.getAttributeCode()
									.intValue() == ConfigurationModel.Attribute.PROPERTIES
									.getCode())) {

						ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);

						cd = (ComponentDefn) setLastChangedDate(cd, principal);
						processPropertyChanges(action, cd, type, config,principal);

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
						
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0090, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0090,action.getActionDescription(),targetID));

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PSC_NAME.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0090, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0090,action.getActionDescription(),targetID));

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0091,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0091,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));

					} else {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0091,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091, anAction.getActionDescription()));
					}
				} else if (action instanceof DestroyObject) {

					if (cd != null) {
						delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), principal);
					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0091, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0091,action.getActionDescription()));
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

	private Set executeActions(ConnectorBindingID targetID, List actions, ConfigurationModelContainerImpl config, String principal)
			throws InvalidDeployedComponentException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;
		affectedIDs.add(targetID);
		BasicConfiguration cfg = (BasicConfiguration) config.getConfiguration();

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				ComponentDefn cd = cfg.getConnectorBinding(targetID);

				if (action instanceof CreateObject) {

					if (cd != null) {
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0092, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0092,targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}
					cd = (ComponentDefn) args[0];
					cd = (ComponentDefn) setCreationDate(cd, principal);
					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);

					// process properties for any encryptions
					processPropertyForNewObject(cd, type, config, principal);
					cfg.addComponentDefn(cd);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject) {

					if (cd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0093, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0093,targetID));
					}

					ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);
					cd = (ComponentDefn) setLastChangedDate(cd, principal);
					processPropertyChanges(action, cd, type, config, principal);

				} else if (action instanceof ExchangeObject) {
					ExchangeObject anAction = (ExchangeObject) action;

					if (cd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0093, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0093,targetID));
					}

					if (anAction.hasAttributeCode() && (anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTY.getCode() || anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.PROPERTIES.getCode())) {
						ComponentType type = getComponentType(config, cd.getComponentTypeID(), targetID);
						cd = (ComponentDefn) setLastChangedDate(cd, principal);
						processPropertyChanges(action, cd, type, config,principal);

					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.NAME.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0094, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0094,action.getActionDescription(),targetID));
					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.IS_ENABLED.getCode()) {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0096,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096, anAction.getActionDescription()));
					} else if (anAction.hasAttributeCode()&& anAction.getAttributeCode().intValue() == ConfigurationModel.Attribute.ROUTING_UUID.getCode()) {
						cd = (ConnectorBinding) setLastChangedDate(cd,principal);
						setRoutingUUID((ConnectorBinding) cd, (String) anAction.getNewValue());
					} else {
						throw new InvalidArgumentException(ConfigMessages.CONFIG_0096,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096, anAction.getActionDescription()));
					}

				} else if (action instanceof DestroyObject) {
					if (cd != null) {
						delete(targetID, cfg);
						setLastChangedDate(config.getConfiguration(), principal);
					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0096, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0096,action.getActionDescription()));
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

	private Set executeActions(ConfigurationID targetID, List actions, ConfigurationModelContainerImpl config, String principal, XMLConfigurationConnector transaction) throws InvalidConfigurationException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;

		BasicConfiguration dvt = null;

		affectedIDs.add(targetID);

		try {
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				if (action.hasAttributeCode() && (action.getAttributeCode().intValue() == ConfigurationModel.Attribute.CURRENT_CONFIGURATION .getCode() || action.getAttributeCode().intValue() == ConfigurationModel.Attribute.NEXT_STARTUP_CONFIGURATION.getCode())) {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0097, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0097,action.getActionDescription()));
				}

				// NOTE: These changes are always made within a transaction and
				// a configurtation is never deleted without one taking its
				// place.
				// And when a delete occurs, a new empty model will take its
				// place.
				// Therefore, a model will ALWAYS exists
				if (action instanceof CreateObject) {
					Object obj = args[0];
					if (obj instanceof Collection) {
						Collection objs = (Collection) obj;

						// Not sure of this pattern here - rareddy
						 ConfigurationModelContainerImpl newConfig = new ConfigurationModelContainerImpl();
						 transaction.setConfigurationModel(newConfig);
						 newConfig.setConfigurationObjects(objs);

						for (Iterator it = objs.iterator(); it.hasNext();) {
							Object o = it.next();
							if (o instanceof ComponentObject) {
								ComponentObject co = (ComponentObject) o;
								ComponentType type = getComponentType(config,co.getComponentTypeID(), targetID);
								processPropertyForNewObject(co, type, config,principal);
							}
						}
					} else {
						dvt = (BasicConfiguration) obj;
						config.addObject(dvt);
					}

				} else if (action instanceof AddObject || action instanceof RemoveObject || action instanceof ExchangeObject) {
					Configuration configuration = config.getConfiguration();
					if (configuration == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0098, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0098,targetID));
					}

					ComponentType type = getComponentType(config, configuration.getComponentTypeID(), targetID);
					processPropertyChanges(action, configuration, type, config,principal);

				} else if (action instanceof DestroyObject) {
					// create an empty model
					// the assumption here is there should other actions as part
					// of this
					// transaction that will load the container before it is
					// committed

					// Not sure of this pattern here - rareddy
					 ConfigurationModelContainerImpl emptyConfig = new ConfigurationModelContainerImpl();
					 transaction.setConfigurationModel(emptyConfig);

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0099, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0099,action.getActionDescription()));
				}

			} // end of while loop

		} catch (ConfigurationException ce) {
			throw ce;

		} catch (Exception e) {

			ConfigurationException e2 = new ConfigurationException(e);
			e2.setActionIndex(actionIndex);
			throw e2;
		}
		return affectedIDs;

	}

	private Set executeActions(ComponentTypeID targetID, List actions,ConfigurationModelContainerImpl config, String principal) 
		throws InvalidConfigurationException, ConfigurationException {
		
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;

		affectedIDs.add(targetID);

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();

				actionIndex++;
				Object args[] = action.getArguments();

				if (action instanceof CreateObject) {

					ComponentType type = config.getComponentType(targetID
							.getFullName());

					if (type != null) {
						// because types are not configuration bound, the create
						// for the type will only be added where it does not
						// exist
						DuplicateComponentException e = new DuplicateComponentException(ConfigMessages.CONFIG_0100, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0100,targetID));
						e.setActionIndex(actionIndex);
						throw e;
					}

					BasicComponentType dvt = (BasicComponentType) args[0];
					setCreationDate(dvt, principal);
					config.addComponentType(dvt);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject) {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));

				} else if (action instanceof ExchangeObject) {
					ExchangeObject exchangeAction = (ExchangeObject) action;

					ComponentType type = config.getComponentType(targetID.getFullName());

					if (type == null) {
						throw new InvalidDeployedComponentException(ConfigMessages.CONFIG_0102, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0102,targetID));
					}

					BasicComponentType bct = (BasicComponentType) type;

					if (action.hasAttributeCode()&& action.getAttributeCode().intValue() == ConfigurationModel.Attribute.UPDATE_COMPONENT_TYPE.getCode()) {

						ComponentType newCt = (ComponentType) exchangeAction.getNewValue();
						setLastChangedDate(newCt, principal);

						config.addComponentType((ComponentType) exchangeAction.getNewValue());

					} else if (action.hasAttributeCode()&& action.getAttributeCode().intValue() == ConfigurationModel.Attribute.PARENT_COMPONENT_TYPEID.getCode()) {

						bct.setParentComponentTypeID((ComponentTypeID) exchangeAction.getNewValue());

						setLastChangedDate(bct, principal);

					} else if (action.hasAttributeCode()&& action.getAttributeCode().intValue() == ConfigurationModel.Attribute.SUPER_COMPONENT_TYPEID.getCode()) {

						bct.setSuperComponentTypeID((ComponentTypeID) exchangeAction.getNewValue());

						setLastChangedDate(bct, principal);

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
							throw new InvalidDeployedComponentException(ConfigPlugin.Util.getString("XMLActionUpdateStrategy.Unable_to_delete_component_type_related_components_found", new Object[] { type.getID(), d.getFullName() })); //$NON-NLS-1$
						}

						config.remove(targetID);

						setLastChangedDate(config.getConfiguration(), principal);

					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0101, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0101,action.getActionDescription()));
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

	private Set executeActions(ComponentTypeDefnID targetID, List actions, ConfigurationModelContainerImpl config, String principal)
			throws InvalidConfigurationException, ConfigurationException {

		/**
		 * This method is used for the mass importing of a configuration,
		 * instead of recreating every object using the editor.
		 * 
		 * NOTE: DO NOT add calls for setting history in this method because the
		 * importing should retain what the original file contained.
		 */
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;
		affectedIDs.add(targetID);

		try {
			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				BasicComponentTypeDefn dvt = (BasicComponentTypeDefn) args[0];
				ComponentType type = getComponentType(config, dvt.getComponentTypeID(), targetID);

				if (action instanceof CreateObject) {
					if (type.getComponentTypeDefinition(dvt.getFullName()) != null) {
						// this check is added because when the editor is used
						// by the importer,
						// it creates actions for this defn where the component
						// type
						// that it added already contains the definitions.
						// Therefore, only throw an exception if it is
						// different.
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
					if (type.getComponentTypeDefinition(dvt.getFullName()) != null) {
						BasicComponentType btype = (BasicComponentType) type;
						btype.removeComponentTypeDefinition(dvt);
					}
				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0103, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0103,action.getActionDescription()));
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

	private Set executeActions(PropDefnAllowedValueID targetID, List actions) {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		affectedIDs.add(targetID);
		return affectedIDs;
	}

	private Set executeActions(HostID targetID, List actions,ConfigurationModelContainerImpl config, String principal)
			throws InvalidComponentException, ConfigurationException {
		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;
		BasicHost dvt = null;

		affectedIDs.add(targetID);

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				if (action instanceof CreateObject) {
					Host host = config.getHost(targetID.getFullName());
					if (host != null) {
						// because hosts are not configuration bound, the create
						// for the hosts will only be added where it does not
						// exist
						continue;
					}
					dvt = (BasicHost) args[0];

					// call to validate the type exists
					getComponentType(config, dvt.getComponentTypeID(), targetID);

					dvt = (BasicHost) setCreationDate(dvt, principal);

					BasicConfiguration bc = (BasicConfiguration) config.getConfiguration();
					bc.addHost(dvt);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject
						|| action instanceof ExchangeObject) {

					Host host = config.getHost(targetID.getFullName());
					if (host == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0105, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0105,targetID));
					}

					ComponentType type = getComponentType(config, host.getComponentTypeID(), targetID);
					host = (Host) setLastChangedDate(host, principal);
					processPropertyChanges(action, host, type, config,principal);

				} else if (action instanceof DestroyObject) {
					Host host = config.getHost(targetID.getFullName());

					if (host != null) {
						// if the host is deleted, so must the dependent objects
						// be deleted (if they exist)
						delete(targetID, config.getConfiguration());
						setLastChangedDate(config.getConfiguration(), principal);
					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075,action.getActionDescription()));
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

	/**
	 * Executes the specified component type actions and will update the list of
	 * component types with the changes.
	 * 
	 * @param componentTypes
	 *            is the configuration to be updated
	 */

	private Set executeActions(SharedResourceID targetID, List actions,
			ConfigurationModelContainerImpl config, String principal)
			throws InvalidConfigurationException, ConfigurationException {

		Set affectedIDs = new HashSet();
		if (actions.isEmpty()) {
			return affectedIDs;
		}

		int actionIndex = -1;

		affectedIDs.add(targetID);
		BasicSharedResource dvt = null;

		try {

			// Iterate through the actions ...
			Iterator iter = actions.iterator();
			while (iter.hasNext()) {
				ActionDefinition action = (ActionDefinition) iter.next();
				actionIndex++;
				Object args[] = action.getArguments();

				if (action instanceof CreateObject) {
					SharedResource rd = config.getResource(targetID.getFullName());

					if (rd != null) {
						// because rd are not configuration bound, the create
						// for the rd will only be added where it does not exist
						continue;
					}
					dvt = (BasicSharedResource) args[0];

					dvt = (BasicSharedResource) setCreationDate(dvt, principal);

					ComponentType type = ResourceModel.getComponentType(dvt.getName());

					// process properties for any encryptions
					processPropertyForNewObject(dvt, type, config, principal);

					config.addResource(dvt);

				} else if (action instanceof AddObject
						|| action instanceof RemoveObject
						|| action instanceof ExchangeObject) {

					SharedResource rd = config.getResource(targetID
							.getFullName());

					if (rd == null) {
						throw new InvalidComponentException(ConfigMessages.CONFIG_0106, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0106,targetID));
					}

					// get the type based on the SharedResource Name, not its
					// type
					ComponentType type = ResourceModel.getComponentType(rd.getName());
					rd = (SharedResource) setLastChangedDate(rd, principal);
					processPropertyChanges(action, rd, type, config, principal);

				} else if (action instanceof DestroyObject) {
					SharedResource rd = config.getResource(targetID.getFullName());

					if (rd != null) {
						config.remove(targetID);
						setLastChangedDate(config.getConfiguration(), principal);

					}

				} else {
					throw new InvalidArgumentException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075,action.getActionDescription()));
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

	void updateSharedResource(SharedResource resource,ConfigurationModelContainerImpl config, String principal)	throws ConfigurationException {

		ComponentType type = ResourceModel.getComponentType(resource.getName());

		Properties props = resource.getProperties();
		SharedResource sr = config.getResource(resource.getFullName());
		if (sr == null) {
			throw new InvalidComponentException(ConfigMessages.CONFIG_0106,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0106,resource.getFullName()));
		}

		updateProperties(sr, props, null, ConfigurationObjectEditor.ADD, type,config, principal);

		sr = (SharedResource) setLastChangedDate(sr, principal);
	}

	//--------------------------------------------------------------------------
	// --------------
	// P R O P E R T Y M E T H O D S
	//--------------------------------------------------------------------------
	// --------------

	private void processPropertyForNewObject(ComponentObject object, ComponentType type, ConfigurationModelContainerImpl config, String principal) 
		throws InvalidPropertyValueException, ConfigurationException {

		// because new objects may no longer have seperate actions for their
		// properties, the
		// properties that they have must be checked for passwords so that
		// they can be encrypted.

		BasicComponentObject bco = (BasicComponentObject) object;

		Properties props = bco.getEditableProperties();

		updateProperties(object, props, null, ConfigurationObjectEditor.SET,
				type, config, principal);
	}

	private void processPropertyChanges(ActionDefinition action,ComponentObject object, ComponentType type,ConfigurationModelContainerImpl config, String principal) 
		throws InvalidPropertyValueException, ConfigurationException {

		int operation = -1;
		String propName;
		String propValue;

		if (action instanceof AddNamedObject || action instanceof AddObject) {
			operation = ConfigurationObjectEditor.ADD;
		} else if (action instanceof ExchangeNamedObject
				|| action instanceof ExchangeObject) {
			operation = ConfigurationObjectEditor.SET;
		} else if (action instanceof RemoveNamedObject
				|| action instanceof RemoveObject) {
			operation = ConfigurationObjectEditor.REMOVE;
		} else {
			throw new RuntimeException(ConfigPlugin.Util.getString(
					ConfigMessages.CONFIG_0107, action.getClass().getName()));
		}

		Object args[] = action.getArguments();

		/*
		 * System.out.println("<!><!>Properties processing action " + action +
		 * " with args:"); for (int i= 0; i<args.length; i++){
		 * System.out.println(i + ": " + args[i]); }
		 */
		// System.out.println("STRATEGY: Process Property Changes 1" );
		if (args[0] instanceof Properties) {

			Properties props = null;
			Properties oldProps = null;
			if (operation == ConfigurationObjectEditor.ADD) {
				props = (Properties) args[0];
			} else if (operation == ConfigurationObjectEditor.SET) {
				// 0 arg is the old value
				// 1 arg is the new value
				oldProps = (Properties) args[0];
				props = (Properties) args[1];
			} else if (operation == ConfigurationObjectEditor.REMOVE) {
				props = (Properties) args[0];
			}
			updateProperties(object, props, oldProps, operation, type, config,
					principal);

		} else {
			propName = (String) args[0];

			String oldValue = ""; //$NON-NLS-1$
			propValue = ""; //$NON-NLS-1$

			if (operation == ConfigurationObjectEditor.ADD) {
				propValue = (String) args[1];
			} else if (operation == ConfigurationObjectEditor.SET) {
				oldValue = (String) args[1];
				propValue = (String) args[2];
			} else if (operation == ConfigurationObjectEditor.REMOVE) {
				propValue = ""; //$NON-NLS-1$
			}

			updateProperty(object, propName, propValue, oldValue, operation,
					type, config, principal);
		}

	}

	private void updateProperties(ComponentObject object, Properties props, Properties oldValues, int operation, ComponentType type, ConfigurationModelContainerImpl config, String principal)
		throws InvalidPropertyValueException, ConfigurationException {

		String propName;
		String propValue;

		Enumeration propertyNames = props.propertyNames();
		Properties passwordProps = new Properties();

		while (propertyNames.hasMoreElements()) {

			propName = (String) propertyNames.nextElement();
			propValue = props.getProperty(propName);

			validateProperty.isPropertyValid(propName, propValue);
			// Must encrypt connection passwords here

			if (propValue != null && propValue.trim().length() > 0 && isPasswordProp(propName, type, config) && !CryptoUtil.isValueEncrypted(propValue)) {
				try {
					propValue = CryptoUtil.stringEncrypt(propValue);
				} catch (CryptoException e) {
					throw new InvalidPropertyValueException(e,ConfigMessages.CONFIG_0108, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0108));
				}
			}
			passwordProps.setProperty(propName, propValue);
		}
		switch (operation) {
			case ConfigurationObjectEditor.ADD:
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"adding properties " + passwordProps, principal, object.getName()); //$NON-NLS-1$
			break;
		case ConfigurationObjectEditor.SET:
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"setting properties " + passwordProps + " previous values " + oldValues, principal, object.getName()); //$NON-NLS-1$ //$NON-NLS-2$
			break;
		case ConfigurationObjectEditor.REMOVE:
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"removing properties " + passwordProps, principal, object.getName()); //$NON-NLS-1$
			break;
		}
		modifyProperties(object, passwordProps, operation);
	}

	private void updateProperty(ComponentObject object, String propName,String propValue, String oldValue, int operation,ComponentType type, ConfigurationModelContainerImpl config,String principal) 
		throws InvalidPropertyValueException,ConfigurationException {
		
		validateProperty.isPropertyValid(propName, propValue);

		if (propValue != null && propValue.trim().length() > 0 && isPasswordProp(propName, type, config) && !CryptoUtil.isValueEncrypted(propValue)) {
			try {
				propValue = CryptoUtil.getCryptor().encrypt(propValue);
			} catch (CryptoException e) {
				throw new InvalidPropertyValueException(e,ConfigMessages.CONFIG_0108, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0108));
			}
		}

		if (operation == ConfigurationObjectEditor.ADD) {
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"adding " + propName + " with value " + propValue, principal, object.getName()); //$NON-NLS-1$ //$NON-NLS-2$
			BasicComponentObject target = (BasicComponentObject) object;
			target.addProperty(propName, propValue);
		} else if (operation == ConfigurationObjectEditor.SET) {
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"setting " + propName + " to value " + propValue + " previous value was " + oldValue, principal, object.getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

			BasicComponentObject target = (BasicComponentObject) object;
			target.removeProperty(propName);
			target.addProperty(propName, propValue);

		} else if (operation == ConfigurationObjectEditor.REMOVE) {
			AuditManager.getInstance().record(ConfigurationServiceInterface.NAME,"removing " + propName, principal, object.getName()); //$NON-NLS-1$
			BasicComponentObject target = (BasicComponentObject) object;
			target.removeProperty(propName);
		}
	}

	/**
	 * Determines if the given propDefn is a password property (ISMASKED == 1).
	 */
	private static boolean isPasswordProp(String propName, ComponentType type, ConfigurationModelContainerImpl config)
			throws ConfigurationException {

		if (type == null) {
			Assertion.isNotNull(type, ConfigPlugin.Util.getString(
					ConfigMessages.CONFIG_0109, propName));
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

		// Assertion.isNotNull(checkType.getComponentTypeDefinition(propName),
		// "No component type definition for " + propName +
		// " in component type " + type.getFullName());
		ComponentTypeDefn td = checkType.getComponentTypeDefinition(propName);

		if (td.getPropertyDefinition().isMasked()) {
			return true;
		}

		return false;
	}

	/**
	 * This methods traverses the component type parent or super types
	 * heirarchy. This is a recursive method used to traverse the tree. NOTE:
	 * the treePath is used to ensure only one path is used and not allow it to
	 * cross over and use the other path. An example would be the "Service"
	 * component type that has a parent type but is also the super type of all
	 * the services.
	 * 
	 */
	private static final int INITIAL_TREE_PATH = -1;
	private static final int PARENT_TREE_PATH = 1;
	private static final int SUPER_TREE_PATH = 2;

	private static ComponentType findComponentTypeBasedOnHierarchy(
			String propName, ComponentType type, int treePath,
			ConfigurationModelContainerImpl config)
			throws ConfigurationException {
		ComponentType foundType = null;

		// first try down the parent path to find the definition for the
		// propName
		if ((treePath == PARENT_TREE_PATH || treePath == INITIAL_TREE_PATH)
				&& type.getParentComponentTypeID() != null) {

			foundType = config.getComponentType(type.getParentComponentTypeID()
					.getFullName());

			// not all parents or supers may be component types, but product
			// types - that have no definitions
			if (foundType != null) {
				// if this parent doesn't contain the definition then goto its
				// parent
				if (foundType.getComponentTypeDefinition(propName) != null) {

					return foundType;
				}

				// dont want to return here because at the intial call of this
				// method
				// and the parent type doesnt have the defn, the super has to be
				// checked
				foundType = findComponentTypeBasedOnHierarchy(propName, foundType, PARENT_TREE_PATH, config);

				if (foundType != null) {
					return foundType;
				}

			}
		}

		if ((treePath == SUPER_TREE_PATH || treePath == INITIAL_TREE_PATH)
				&& type.getSuperComponentTypeID() != null) {

			foundType = config.getComponentType(type.getSuperComponentTypeID().getFullName());

			if (foundType != null) {
				// if this parent doesn't contain the definition then goto its
				// parent
				if (foundType.getComponentTypeDefinition(propName) != null) {
					return foundType;
				}

				foundType = findComponentTypeBasedOnHierarchy(propName,foundType, SUPER_TREE_PATH, config);

				if (foundType != null) {
					return foundType;
				}
			}

		}

		return foundType;

	}

	private ComponentObject setLastChangedDate(ComponentObject defn,String principal) {
		String lastChangedDate = DateUtil.getCurrentDateAsString();
		return BasicUtil.setLastChangedHistory(defn, principal, lastChangedDate);
	}

	private ComponentType setLastChangedDate(ComponentType defn,String principal) {
		String lastChangedDate = DateUtil.getCurrentDateAsString();
		return BasicUtil.setLastChangedHistory(defn, principal, lastChangedDate);
	}

	private ComponentObject setCreationDate(ComponentObject defn,String principal) {

		String creationDate = DateUtil.getCurrentDateAsString();
		defn = BasicUtil.setLastChangedHistory(defn, principal, creationDate);
		return BasicUtil.setCreationChangedHistory(defn, principal,creationDate);
	}

	private ComponentType setCreationDate(ComponentType defn, String principal) {
		String creationDate = DateUtil.getCurrentDateAsString();
		defn = BasicUtil.setLastChangedHistory(defn, principal, creationDate);
		return BasicUtil.setCreationChangedHistory(defn, principal,creationDate);
	}

	private Configuration delete(ComponentObjectID targetID,Configuration configuration) {
		BasicConfiguration basicConfig = (BasicConfiguration) configuration;
		basicConfig.removeComponentObject(targetID);
		return basicConfig;
	}

	/**
	 * The command to signify setting of an attribute.
	 */
	public static final int SET = 0;

	/**
	 * The command to signify addition of an attribute.
	 */
	public static final int ADD = 1;

	/**
	 * The command to signify removal of an attribute.
	 */
	public static final int REMOVE = 2;

	private ComponentObject modifyProperties(ComponentObject t, Properties props, int command) {
		Assertion.isNotNull(t);

		if (props == null) {
			return t;
		}

		BasicComponentObject target = (BasicComponentObject) t;
		Properties newProps = null;

		switch (command) {
		case ADD:
			newProps = new Properties();
			newProps.putAll(target.getEditableProperties());
			newProps.putAll(props);

			target.addProperties(newProps);

			break;
		case REMOVE:
			newProps = new Properties();
			newProps.putAll(target.getEditableProperties());
			Iterator iter = props.keySet().iterator();
			while (iter.hasNext()) {
				newProps.remove(iter.next());
			}

			target.setProperties(newProps);
			break;
		case SET:
			target.setProperties(props);
			break;
		}

		return target;

	}

	private void setRoutingUUID(ServiceComponentDefn serviceComponentDefn,
			String newRoutingUUID) {
		Assertion.isNotNull(serviceComponentDefn);

		BasicServiceComponentDefn basicService = (BasicServiceComponentDefn) serviceComponentDefn;
		basicService.setRoutingUUID(newRoutingUUID);
	}
}
