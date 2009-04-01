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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

/**
 * Acts as a scoped unit of work configuration service layer.
 */

public class XMLConfigurationConnector {

    private String principal;
    private XMLConfigurationMgr configurationMgr;

    private Map configurationObjects = new HashMap(3);
    private XMLActionUpdateStrategy updateStrategy = new XMLActionUpdateStrategy();
    
    XMLConfigurationConnector(XMLConfigurationMgr configurationMgr, String principal) {
    	this.configurationMgr = configurationMgr;
    	this.principal = principal;
    }
    
    /**
     * Returns the name that holds the lock.
     * @return String name who holds the lock
     */
    public String getLockAcquiredBy() {
    	return principal;
    }

    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    public void commit() throws ConfigTransactionException{
    	Collection<ConfigurationModelContainer> models = getObjects();
    	this.configurationObjects.clear();
  		configurationMgr.applyTransaction(models, this.principal);
    }

     /**
     * Returns the objects that changed during this transaction
     */
    public Collection getObjects() {
    	return new ArrayList(configurationObjects.values());
    }

    /**
     * Call to add an object to the set of objects that changed during this
     * transaction.
     * For the configuration process, this object will be
     * @see {ConfigurationModelContainer Configuration}
     * @param key is the id of the configuration
     * @param value is the configuration container
     */
    public void addObjects(Object key, Object value) {
        configurationObjects.put(key, value);

    }

     /**
     * Returns the objects that changed during this transaction.  For
     * the configuration process, these objects will be
     * @see {ConfigurationModelContainer Configurations}.
     * @return Collection of objects that changed during the transaction.
     */
    public Object getObject(Object key) {
        return configurationObjects.get(key);
    }


    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

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
    public Configuration getDesignatedConfiguration(String designation) throws ConfigurationException{
        return getConfigurationModel(XMLConfigurationMgr.getDesignatedConfigurationID(designation)).getConfiguration();
    }

    public ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException {
        return getConfigurationModel(XMLConfigurationMgr.getDesignatedConfigurationID(configName));
    }

    public ConfigurationID getDesignatedConfigurationID(String designation) throws ConfigurationException {
        return XMLConfigurationMgr.getDesignatedConfigurationID(designation);
    }

    /**
     * Obtain the list of component definition instances that makeup the configuration.
     * @return the list of Component instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Map getComponentDefinitions(ConfigurationID configurationID) throws ConfigurationException {
    	ConfigurationModelContainer config = getConfigurationModel(configurationID);

        return config.getConfiguration().getComponentDefns();
    }
    
    public ConfigurationModelContainer getConfigurationModel(ConfigurationID configurationID) throws ConfigurationException {
    	return this.configurationMgr.getConfigurationModel(configurationID);
    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Collection getResources() throws ConfigurationException {
    	ConfigurationModelContainer cmc = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        return cmc.getResources();
    }

    /**
     * Obtain the component definition
     * @return the ComponentDefn
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ComponentDefn getComponentDefinition(ComponentDefnID targetID, ConfigurationID configurationID) throws ConfigurationException{
    	if (targetID == null) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0045,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045,"ComponentDefnID")); //$NON-NLS-1$
        }
        ComponentDefn defn = null;


        if (configurationID == null) {
            configurationID = getDesignatedConfigurationID(targetID.getParentFullName());
        }

        ConfigurationModelContainer config = getConfigurationModel(configurationID);
        defn = config.getConfiguration().getComponentDefn(targetID);

        return defn;
    }


    public Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException{
    	ConfigurationModelContainer config =  getConfigurationModel(Configuration.NEXT_STARTUP_ID);
        ComponentType t = config.getComponentType(componentTypeID.getFullName());
        if (t!= null) {
            return t.getComponentTypeDefinitions();
        }
        return Collections.EMPTY_LIST;
    }


    /**
     * Obtain the list of deployed components that represent the configuration
     * when deployed.
     * @return the list of DeployedComponents
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public List getDeployedComponents(ConfigurationID configurationID) throws ConfigurationException{
        ConfigurationModelContainer config = getConfigurationModel(configurationID);


        Collection dcs = config.getConfiguration().getDeployedComponents();
        List result = new LinkedList();
        result.addAll(dcs);
        return result;
    }

    /**
     * Obtain the list of registered host
     * @return Collection of Hosts
     * @throws ConfigurationException if an error occurred within or during communication with the Metadata Service.
     */
    public Collection getHosts() throws ConfigurationException{
        return this.configurationMgr.getConfigurationModel(Configuration.NEXT_STARTUP_ID).getConfiguration().getHosts();
    }

    public ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException {
    	if ( id == null ) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0127, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0127));
        }
        return this.configurationMgr.getConfigurationModel(Configuration.NEXT_STARTUP_ID).getComponentType(id.getFullName());
    }


    public Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException {
    	ConfigurationModelContainer config = getConfigurationModel(Configuration.NEXT_STARTUP_ID);

        Map typeMap = config.getComponentTypes();
        Collection types = new ArrayList(typeMap.size());
        types.addAll(typeMap.values());
        return types;

    }
    
    public Collection getAllObjectsForConfigurationModel(ConfigurationID configID) throws ConfigurationException {
    	return this.configurationMgr.getConfigurationModel(configID).getAllObjects();
    }

    // ----------------------------------------------------------------------------------------
    //                 C O N F I G U R A T I O N    U P D A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

    public Set executeActions( List actions ) throws InvalidComponentException, ConfigurationException {
        ArgCheck.isNotNull(actions, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045, "actions")); //$NON-NLS-1$

        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, "Executing", actions.size(), "action(s) for principal", principal); //$NON-NLS-1$ //$NON-NLS-2$

        Set result = new HashSet();

        if ( actions.isEmpty() ) {
            return result;
        }

        List actionsWithSameTarget = new ArrayList(13);   // guessing at an initial size, probably high
        Object currentTarget = null;
        ActionDefinition currentAction = null;
        ActionDefinition nextAction = null;

        int actionCounter = -1;

        // Iterate through the actions, and apply all as a single transaction
        try {
            boolean createObject = false;

            // Get the first action and its target, and add it to the list ...
            Iterator iter = actions.iterator();
            if ( iter.hasNext() ) {
                currentAction = (ActionDefinition) iter.next();
                currentTarget = currentAction.getTarget();
                actionsWithSameTarget.add(currentAction);
            }

//System.out.println("WRITER: Iterater Actions " + actions.size());
            while ( iter.hasNext() ) {
                nextAction = (ActionDefinition) iter.next();
                if ( currentAction instanceof CreateObject ) {
                    createObject = true;
                }

                // If the current action is a 'DestroyObject' action, then process only
                // the destroy (other actions not processed up to this point do not
                // need to be processed, since the target will be destroyed anyway).
                if ( currentAction instanceof DestroyObject ) {
                    // If creating and destroying an object in the same action list,
                    // then don't need to do anything ...
                    if ( !createObject ) {
                        result.addAll( executeActionsOnTarget( currentTarget,actionsWithSameTarget)  );
                    }
                    actionCounter += actionsWithSameTarget.size();
                    actionsWithSameTarget.clear();
                    createObject = false;
                    currentTarget = nextAction.getTarget();
                }

                // Otherwise, if the next action has another target, process up to the current action ...
                else if ( currentTarget != nextAction.getTarget() ) {

                    result.addAll( executeActionsOnTarget( currentTarget,actionsWithSameTarget)  );
                    actionCounter += actionsWithSameTarget.size();
                    actionsWithSameTarget.clear();
                    createObject = false;
                    currentTarget = nextAction.getTarget();
                }

                // Add this next action ...
                currentAction = nextAction;
                actionsWithSameTarget.add(currentAction);
            }

            // Process the last set of actions ...
            if ( actionsWithSameTarget.size() != 0 ) {

                result.addAll( executeActionsOnTarget(currentTarget,actionsWithSameTarget)  );
                createObject = false;
            }

        } catch ( Exception e ) {
            throw new ConfigurationException(e);
        }
        LogManager.logInfo(LogCommonConstants.CTX_CONFIG,ConfigPlugin.Util.getString(ConfigMessages.MSG_0007));
        return result;
    }
    
    /**
     * Executes the specified transactions, which must all be applied to the same target, using
     * the specified transaction.
     */
    private Set executeActionsOnTarget( Object target, List actions)
                        throws ConfigTransactionException, ConfigurationException {

        Set results =  updateStrategy.executeActionsOnTarget(target, actions, this);

        return results;

    }

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          *
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public void saveResources(Collection resourceDescriptors, String principalName) throws ConfigurationException {

        for (Iterator it=resourceDescriptors.iterator(); it.hasNext(); ) {
            SharedResource rd = (SharedResource) it.next();

            ArgCheck.isNotNull(rd, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0130));

            updateStrategy.updateSharedResource(rd, this);
        }
    }

}
