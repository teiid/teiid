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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.log.LogManager;
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

    private ConfigurationModelContainerImpl clonedConfiguration;
    
    private XMLActionUpdateStrategy updateStrategy = new XMLActionUpdateStrategy();
    
    XMLConfigurationConnector(XMLConfigurationMgr configurationMgr, ConfigurationModelContainerImpl config, String principal) {
    	this.configurationMgr = configurationMgr;
    	this.principal = principal;
    	this.clonedConfiguration = config;
    }
    
    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    public void commit() throws ConfigurationException {
    	checkState();
  		configurationMgr.applyTransaction(clonedConfiguration, this.principal);
  		this.clonedConfiguration = null;
    }

    
    public ConfigurationModelContainerImpl getConfigurationModel() throws ConfigurationException {
    	checkState();
        return clonedConfiguration;
    }

	private void checkState() throws ConfigurationException {
		if (clonedConfiguration == null) {
    		throw new ConfigurationException("Tranaction already commited; Configuration in Invalid State"); //$NON-NLS-1$
    	}
	}


    // ----------------------------------------------------------------------------------------
    //                 C O N F I G U R A T I O N    U P D A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

    public Set executeActions( List actions ) throws ConfigurationException {
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
        boolean createObject = false;

        // Get the first action and its target, and add it to the list ...
        Iterator iter = actions.iterator();
        if ( iter.hasNext() ) {
            currentAction = (ActionDefinition) iter.next();
            currentTarget = currentAction.getTarget();
            actionsWithSameTarget.add(currentAction);
        }

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
        LogManager.logInfo(LogCommonConstants.CTX_CONFIG,ConfigPlugin.Util.getString(ConfigMessages.MSG_0007));
        return result;
    }
    
    /**
     * Executes the specified transactions, which must all be applied to the same target, using
     * the specified transaction.
     */
    private Set executeActionsOnTarget( Object target, List actions) throws ConfigurationException {
        return  updateStrategy.executeActionsOnTarget(target, actions, clonedConfiguration, this.principal, this);
    }

    /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          *
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public void saveResources(Collection resourceDescriptors, String principalName) throws ConfigurationException {
    	checkState();
        for (Iterator it=resourceDescriptors.iterator(); it.hasNext(); ) {
            SharedResource rd = (SharedResource) it.next();

            ArgCheck.isNotNull(rd, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0130));

            updateStrategy.updateSharedResource(rd, clonedConfiguration, principalName);
        }
    }

    void setConfigurationModel(ConfigurationModelContainerImpl config) {
    	this.clonedConfiguration = config;
    }
}
