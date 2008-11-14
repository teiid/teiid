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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.actions.ActionDefinition;
import com.metamatrix.common.actions.CreateObject;
import com.metamatrix.common.actions.DestroyObject;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.transaction.ConfigTransaction;
import com.metamatrix.platform.config.transaction.ConfigTransactionException;
import com.metamatrix.platform.config.transaction.ConfigTransactionHelper;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockException;
import com.metamatrix.platform.config.transaction.ConfigUserTransaction;
import com.metamatrix.platform.config.transaction.ConfigUserTransactionFactory;

public class XMLConfigurationWriter  {


    private static XMLConfigurationMgr configMgr = XMLConfigurationMgr.getInstance();

    private XMLConfigurationReader reader = null;

    private ConfigUserTransactionFactory factory;
    private ManagedConnection mgdConnection;
    private XMLActionUpdateStrategy updateStrategy = new XMLActionUpdateStrategy();


	public XMLConfigurationWriter(ManagedConnection mgdConnection) {
		this.mgdConnection = mgdConnection;


	    factory = new ConfigUserTransactionFactory(configMgr.getConfigTransactionFactory());

    }

/*
    public  void insertResource(String resourceName, ConfigTransaction transaction) throws  ConfigurationException, ConfigTransactionException{

		ArgCheck.isNotNull(resourceName, "Unable to insert resource, the resource name is null");
		validateLock(transaction);

		// both models are added because changes are applied across the board
		addConfigurationToTransaction(Configuration.NEXT_STARTUP, transaction);
		addConfigurationToTransaction(Configuration.OPERATIONAL, transaction);

        updateStrategy.insertResource(resourceName, transaction);
    }

    public  void insertResourceProperties(String resourceName,  Properties properties, ConfigTransaction transaction) throws  ConfigurationException, ConfigTransactionException{
		ArgCheck.isNotNull(resourceName, "Unable to insert resource properties, the resource name is null");
		ArgCheck.isNotNull(properties, "Unable to insert resource, the resource properties are null");
		validateLock(transaction);

		// both models are added because changes are applied across the board

		addConfigurationToTransaction(Configuration.NEXT_STARTUP, transaction);
		addConfigurationToTransaction(Configuration.OPERATIONAL, transaction);

        updateStrategy.insertResourceProperties(resourceName, properties, transaction);
    }
*/
    public  void updateSharedResource(SharedResource resource, ConfigTransaction transaction) throws  ConfigurationException, ConfigTransactionException{
		validateLock(transaction);

		if(resource == null){
            ArgCheck.isNotNull(resource, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0130));
        }

        updateStrategy.updateSharedResource(resource, transaction);

    }

    // ----------------------------------------------------------------------------------------
    //      S Y S T E M    S T A R T U P    S T A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * Called by {@link XMLCurrentConfigurationReader#performSystemInitialization}
     *
     * NOTE - This is a self contained transaction, the calling class cannot
     * control the transaction
     *
     * @see XMLCurrentConfigurationReader#performSystemInitialization
     */
    public  void performSystemInitialization(ConfigTransaction transaction) throws StartupStateException,  ConfigurationException{
		validateLock(transaction);

        configMgr.performSystemInitialization(transaction);

    }

    public void beginSystemInitialization(boolean forceInitialization, ConfigTransaction transaction) throws StartupStateException,  ConfigurationException{
		validateLock(transaction);


    	if (forceInitialization) {

        	transaction.setAction(ConfigTransaction.SERVER_FORCE_INITIALIZATION);
    	} else {
        	transaction.setAction(ConfigTransaction.SERVER_INITIALIZATION);
    	}

		configMgr.setServerStateToStarting(forceInitialization);

    }

    /**
     * Called by {@link JDBCCurrentConfigurationReader#finishSystemInitialization}
     * @see JDBCCurrentConfigurationReader#finishSystemInitialization
     */

    public  void finishSystemInitialization(ConfigTransaction transaction) throws StartupStateException, ConfigurationException{
		validateLock(transaction);

		configMgr.setServerStateToStarted();

    }

    /**
     * Called by {@link XMLCurrentConfigurationReader#indicateSystemShutdown}
     * @see XMLCurrentConfigurationReader#indicateSystemShutdown
     */
    public void indicateSystemShutdown(ConfigTransaction transaction) throws StartupStateException, ConfigurationException{
		validateLock(transaction);

		transaction.setAction(ConfigTransaction.SERVER_SHUTDOWN);
		configMgr.setServerStateToStopped();
		//NOTE: when the transaction is committed, the change will be applied to the persistent layer
//

    }

    /**
     * Called by {@link JDBCCurrentConfigurationReader#beginSystemInitialization}
     * @see JDBCCurrentConfigurationReader#beginSystemInitialization
     */
/*
    public  void initializeConfigurations(ConfigTransaction transaction) throws StartupStateException, ConfigurationException{
   }
*/

    // ----------------------------------------------------------------------------------------
    //                 C O N F I G U R A T I O N    U P D A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

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
     * @return the new ID of the newly-copied Configuration
     * @throws InvalidConfigurationException if either ConfigurationID is invalid.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationID overwriteConfiguration(ConfigurationID configIDToCopy, ConfigurationID configIDToOverwrite, ConfigTransaction transaction) throws InvalidConfigurationException, ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0131));
/*
		validateLock(transaction);

		configMgr.overwriteConfiguration(configIDToCopy, configIDToOverwrite, transaction);
        return configIDToOverwrite;
*/
    }


    /**
     * Executes the list of actions, returns the Set of affected objects.
     * This is assumed one logical transaction.
     * @return Set of affected configuration objects
     */
    public Set executeActions(List actions, ConfigTransaction transaction) throws ConfigTransactionException, ConfigurationLockException, ConfigurationException {
		validateLock(transaction);

        if(actions == null){
            ArgCheck.isNotNull(actions, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045, "actions")); //$NON-NLS-1$
        }
        if(transaction == null){
            ArgCheck.isNotNull(transaction, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045, "transaction")); //$NON-NLS-1$
        }


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
                        result.addAll( executeActionsOnTarget( currentTarget,actionsWithSameTarget,transaction)  );
                    }
                    actionCounter += actionsWithSameTarget.size();
                    actionsWithSameTarget.clear();
                    createObject = false;
                    currentTarget = nextAction.getTarget();
                }

                // Otherwise, if the next action has another target, process up to the current action ...
                else if ( currentTarget != nextAction.getTarget() ) {

                    result.addAll( executeActionsOnTarget( currentTarget,actionsWithSameTarget,transaction)  );
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

                result.addAll( executeActionsOnTarget(currentTarget,actionsWithSameTarget,transaction)  );
                createObject = false;
            }

        } catch ( Exception e ) {
            throw new ConfigurationException(e);
        }
        return result;

    }


    /**
     * Executes the specified transactions, which must all be applied to the same target, using
     * the specified transaction.
     */
    private Set executeActionsOnTarget( Object target, List actions, ConfigTransaction transaction )
                        throws ConfigTransactionException,  ConfigurationLockException, ConfigurationException {
 //               	System.out.println("WRITER: Execute on target " + target);

        Set results =  updateStrategy.executeActionsOnTarget(target, actions, transaction);

        return results;

    }


    protected ConfigUserTransaction getTransaction(String principal) throws ConfigTransactionLockException, ConfigTransactionException, ConfigurationException {
        ConfigUserTransaction trans = ConfigTransactionHelper.getWriteTransactionWithRetry(principal, factory);
        ConfigTransaction transaction = trans.getTransaction();
		ConfigurationID configID = getConfigurationReader().getDesignatedConfigurationID(Configuration.NEXT_STARTUP);
        ConfigurationModelContainer transconfig = configMgr.getConfigurationModelForTransaction(configID);
		transaction.addObjects(configID.getFullName(), transconfig);



        return trans;
    }



    protected XMLConfigurationReader getConfigurationReader() {
        if (reader == null) {
            reader = new XMLConfigurationReader(mgdConnection);
        }
        return reader;
    }


    private void validateLock(ConfigTransaction transaction) throws ConfigurationLockException {

    	if (transaction == null) {
     		throw new ConfigurationLockException(ConfigMessages.CONFIG_0123, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0123));
     	}


     	if (transaction.getTransactionLock() == null) {
     		throw new ConfigurationLockException(ConfigMessages.CONFIG_0124, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0124));
     	}

    }



}





