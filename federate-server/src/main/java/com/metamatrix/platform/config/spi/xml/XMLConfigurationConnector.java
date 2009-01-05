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
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentObjectID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationInfo;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidComponentException;
import com.metamatrix.common.config.api.exceptions.InvalidConfigurationException;
import com.metamatrix.common.config.api.exceptions.InvalidNameException;
import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.spi.ConfigurationTransaction;
import com.metamatrix.platform.config.spi.SystemConfigurationNames;



/*******************
 JDBCConfigurationTransaction is responsible for executing and managing
 transactions.

 History:
   12/20/00  vhalbert - when adding/updating property values, the values
                       will be trimmed.  This is because the use
                       of the value later is incorrect if the user
                       does not trim.  Thereforre, it is easier to do
                       this trimming one time and in one location.
                       @see addProperty() and updateProperty()

*/

public class XMLConfigurationConnector extends BaseTransaction implements ConfigurationTransaction {

    private ConfigUserTransaction configUserTransaction=null;

//    private static final int INITIAL_TREE_PATH = -1;
//    private static final int PARENT_TREE_PATH = 1;
//    private static final int SUPER_TREE_PATH = 2;

    private XMLConfigurationReader reader = null;
    private XMLConfigurationWriter writer = null;

    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    XMLConfigurationConnector( ManagedConnection connection, boolean readonly ) throws ManagedConnectionException {
        super(connection,readonly);
/*
        try {


            JDBCMgdResourceConnection jdbcManagedConnection = (JDBCMgdResourceConnection) connection;
            this.jdbcConnection = jdbcManagedConnection.getConnection();

        } catch ( Exception e ) {
            throw new ManagedConnectionException("The connection is not the appropriate type (\"" + JDBCMgdResourceConnection.class.getName() + "\")");
        }
*/
     }

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
    public Configuration getCurrentConfiguration() throws ConfigurationException {
        return getConfigurationReader().getDesignatedConfiguration(Configuration.NEXT_STARTUP_ID);
    }

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
        return getConfigurationReader().getDesignatedConfiguration(designation);
    }

    /**
     * Obtain a configuration that contains all its components and
     * the deployed components.
     * @param configurationName
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Configuration getConfiguration(String configurationName) throws ConfigurationException{
        return getConfigurationReader().getDesignatedConfiguration(configurationName);
    }

    public ConfigurationModelContainer getConfigurationModel(String configName) throws ConfigurationException {
        if (configName.equalsIgnoreCase(Configuration.STARTUP)) {
            return getConfigurationReader().getConfigurationModel(Configuration.STARTUP_ID);
        }
        return getConfigurationReader().getConfigurationModel(Configuration.NEXT_STARTUP_ID);
    }

    /**
     * Obtain the configuration info for the specified configuration and version.
     * @return the configuration info instance
     * @param configurationName
     * @throws InvalidNameException if the configuration does not exist
     * @throws ConfigurationException when an error occurred within or during communication with the Configuration Service.
     */
    public ConfigurationInfo getConfigurationInfo(String configurationName) throws InvalidNameException, ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "getConfigurationInfo" )); //$NON-NLS-1$
    }

    /**
     * @deprecated as of v 2.0 beta 1 use {@link #getDesignatedConfigurationID}
     */
    public ConfigurationID getCurrentConfigurationID() throws ConfigurationException {
        return getConfigurationReader().getDesignatedConfigurationID(Configuration.NEXT_STARTUP);
    }

    public ConfigurationID getDesignatedConfigurationID(String designation) throws ConfigurationException {
        return getConfigurationReader().getDesignatedConfigurationID(designation);
    }

    /**
     * Obtain the list of component definition instances that makeup the configuration.
     * @return the list of Component instances
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Map getComponentDefinitions(ConfigurationID configurationID) throws ConfigurationException {
        return getConfigurationReader().getComponentDefinitions(configurationID);
    }

/**
 * @deprecated 5.5.4
 */
    public Collection getConnectionPools(ConfigurationID configurationID) throws ConfigurationException {
        return getConfigurationReader().getConnectionPools(configurationID);

    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * for all internal resources defined to the system.  The internal resources are not managed with
     * the other configuration related information.  They are not dictated based on which configuration
     * they will operate (i.e., next startup or operational);
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Collection getResources() throws ConfigurationException {
        return getConfigurationReader().getResources();

    }

    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * that are of the specified resource type.
     * @param componentType that identifies the type of resources to be returned
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public Collection getResources(ComponentTypeID componentTypeID) throws ConfigurationException{

        Collection resources = getResources();

        Collection resourcesForType = new ArrayList(resources.size());

        for (Iterator it = resources.iterator(); it.hasNext(); ) {
            ResourceDescriptor rd = (ResourceDescriptor) it.next();
            if (rd.getComponentTypeID().equals(componentTypeID)) {
                resourcesForType.add(rd);
            }
        }

        return resourcesForType;

    }


    public String getComponentPropertyValue(ComponentObjectID componentObjectID, ComponentTypeID typeID, String propertyName) throws ConfigurationException {
        String value = getConfigurationReader().getComponentPropertyValue(componentObjectID, typeID, propertyName);
        return value;
    }


    /**
     * Obtain the component definition
     * @return the ComponentDefn
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ComponentDefn getComponentDefinition(ComponentDefnID componentID, ConfigurationID configurationID) throws ConfigurationException{
        return getConfigurationReader().getComponentDefinition(componentID, configurationID);
    }


    public Collection getComponentTypeDefinitions(ComponentTypeID componentTypeID) throws ConfigurationException{
        return getConfigurationReader().getComponenTypeDefinitions(componentTypeID);
    }


    /**
     * Obtain the list of deployed components that represent the configuration
     * when deployed.
     * @return the list of DeployedComponents
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public List getDeployedComponents(ConfigurationID configurationID) throws ConfigurationException{
        return getConfigurationReader().getDeployedComponents(configurationID);
    }

    /**
     * Obtain the deployed component
     * @return the DeployedComponent
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public DeployedComponent getDeployedComponent(DeployedComponentID deployedComponentID) throws ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "getDeployedComponent" )); //$NON-NLS-1$
    }


    /**
     * Obtain the list of registered host
     * @return Collection of Hosts
     * @throws ConfigurationException if an error occurred within or during communication with the Metadata Service.
     */
    public Collection getHosts() throws ConfigurationException{
        return getConfigurationReader().getHosts();
    }

    public ComponentType getComponentType(ComponentTypeID id) throws ConfigurationException {
        return getConfigurationReader().getComponentType(id);
    }


    public Collection getAllComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        return getConfigurationReader().getComponentTypes(includeDeprecated);

    }
    
    public Collection getProductTypes(boolean includedeprecated) throws ConfigurationException {
        return getConfigurationReader().getProductTypes(includedeprecated);
    }
    

    public Collection getAllObjectsForConfigurationModel(ConfigurationID configID) throws ConfigurationException {
    	return getConfigurationReader().getConfigurationModel(configID).getAllObjects();
    }



    public Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "getMonitoredComponentTypes" )); //$NON-NLS-1$

//        return  getConfigurationReader().getMonitoredComponentTypes(includeDeprecated);
    }

    /**
     * Return the time the server was started. If the state of the server is not "Started"
     * then a null is returned.
     *
     * @return Date Time server was started.
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service.
     */
    public Date getServerStartupTime() throws ConfigurationException {
        return getConfigurationReader().getServerStartupTime();
    }


    public int getServerStartupState() throws ConfigurationException {
        return getConfigurationReader().getServerStartupState();
    }



    //**************************************************
    //
    //  L O C K I N G    M E T H O D S
    //
    //**************************************************

    /**
     * Attempt to reserve a lock for the specified version of the specified configuration.
     * If the configuration is already locked by the same principal, this method simply returns true.
     * Otherwise, this method attempts to reserve a lock, and returns whether
     * the lock could be successfully reserved.
     * @param configurationName the identifier of the configuration.
     * @param principalName the name of the principal that is requesting the lock
     * @return true if the requested lock was able to be reserved by this
     * editor, or false if the lock could not be reserved because there
     * already exists a lock reserved by another editor.
     * @throws InvalidConfigurationException if the configuration name and version are invalid.
     * @throws ConfigurationLockException if there was an error locking the configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
/*
    public boolean lockConfiguration(ConfigurationID configurationID, String principalName)
    throws InvalidConfigurationException, ConfigurationLockException, ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "lockConfiguration" ));
    }
*/
    /**
     * Attempt to release the lock currently held by the principal for the
     * specified configuration and version.  If this principal does not have a lock,
     * this method simply returns false.
     * Otherwise, this method attempts to release the lock, and returns whether
     * the lock could be successfully released.
     * @param configurationName the identifier of the configuration.
     * @param principalName the name of the principal that holds the lock
     * to be released.
     * @return true if the lock held by this editor for the specified
     * configuration was able to be returned, or false if the lock could not
     * returned because a lock for the specified configuration was not
     * held.
     * @throws InvalidConfigurationException if the configuration name and version are invalid.
     * @throws ConfigurationLockException if there was an error unlocking the configuration
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */

//    public boolean unlockConfiguration(LockedConfigurationID configurationID, String principalName )
/*
    public boolean unlockConfiguration(ConfigurationID configurationID, String principalName )
    throws InvalidConfigurationException, ConfigurationLockException, ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "unlockConfiguration" ));
    }
*/
    /**
     * Retrieve the lock information (if a lock exists) for the configuration
     * with the specified configuration name and version.
     * @param configurationName the identifier of the configuration.
     * @return the lock information if a lock exists for the configuration, or null
     * if no lock is held.
     * @throws InvalidConfigurationException if the configurationName and version are invalid.
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    /*
    public LockedConfigurationID getConfigurationLockInformation( ConfigurationID configurationID )
    throws InvalidConfigurationException, ConfigurationException{
        throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0110, "getConfigurationLockInformation" ));
    }
*/

    /**
     * Returns a boolean indicating if the configuration already exist or not.
     * @param configurationName the identifier of the configuration
     * @return boolean of false if the configuration is found
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public boolean doesConfigurationExist( String configurationName )
        throws ConfigurationException{
        ConfigurationID id = getConfigurationReader().getDesignatedConfigurationID(configurationName);
        if (id != null) {
          return true;
        }
        return false;
    }



    public void commit() throws ManagedConnectionException {

         if (configUserTransaction != null) {
             try {
                configUserTransaction.commit();
             } catch (Exception ce) {
                throw new ManagedConnectionException(ce, ConfigMessages.CONFIG_0111,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0111, configUserTransaction.getTransaction().getLockAcquiredBy()));
             }

         }

        super.commit();

        configUserTransaction = null;


    }

    public void rollback() throws ManagedConnectionException {
        if (configUserTransaction != null) {
            try {
                configUserTransaction.rollback();

            } catch (Exception re) {
            }
        }

        super.rollback();

    }



    // ----------------------------------------------------------------------------------------
    //                 C O N F I G U R A T I O N    U P D A T E    M E T H O D S
    // ----------------------------------------------------------------------------------------

    public Set executeActions( List actions, String principalName ) throws InvalidComponentException, ConfigurationException {
        Set affectedIDs = null;
        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, new Object[] {"Executing " + actions.size() +  " action(s) for principal ", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$

        try {

            configUserTransaction = getConfigurationWriter().getTransaction(principalName);
            affectedIDs = getConfigurationWriter().executeActions(actions, configUserTransaction.getTransaction());

        } catch (TransactionException e) {
            LogManager.logTrace(LogCommonConstants.CTX_CONFIG, e, new Object[]{"Failed actions: ", actions}); //$NON-NLS-1$

			try {
				configUserTransaction.rollback();
			} catch(Exception re) {
			}

            throw new ConfigurationException(e, ConfigPlugin.Util.getString(ConfigMessages.MSG_0006));
        } catch (ConfigurationException e) {
            LogManager.logTrace(LogCommonConstants.CTX_CONFIG, e, new Object[]{"Failed actions: ", actions}); //$NON-NLS-1$

			try {
				configUserTransaction.rollback();
			} catch(Exception re) {
			}

            throw e;
        } finally {
          // release the lock
//            unlockconfiguration(lockID, principalName);
        }
        LogManager.logInfo(LogCommonConstants.CTX_CONFIG,ConfigPlugin.Util.getString(ConfigMessages.MSG_0007));

        return affectedIDs;
    }

    public Set executeActions( boolean doAdjust, List actions, String principalName ) throws InvalidComponentException, ConfigurationException{
		throw new UnsupportedOperationException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0112));

    }



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
    public ConfigurationID overwriteConfiguration(ConfigurationID configToCopy, ConfigurationID configToOverwrite, String principalName) throws InvalidConfigurationException, ConfigurationException{
        ConfigurationID resultID = null;
        LogManager.logDetail(LogCommonConstants.CTX_CONFIG, new Object[] {"Overwriting configuration ", configToOverwrite, "with configuration ", configToCopy, " for principal", principalName} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$


        try {

            configUserTransaction = getConfigurationWriter().getTransaction(principalName);

            resultID = getConfigurationWriter().overwriteConfiguration(configToCopy, configToOverwrite, configUserTransaction.getTransaction());

        } catch (ConfigTransactionException te) {
//            LogManager.logCritical(LogCommonConstants.CTX_CONFIG, e, "Unable to execute actions; use trace logging for actions");
//            LogManager.logTrace(LogCommonConstants.CTX_CONFIG, e, new Object[]{"Failed actions: ", actions});
            throw new ConfigurationException(te);
        }
        LogManager.logInfo(LogCommonConstants.CTX_CONFIG, ConfigPlugin.Util.getString(ConfigMessages.MSG_0008));

        return resultID;
    }

   /**
     * Save the resource changes based on each {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}
     * in the collection.
     * @param resourceDescriptors for the resources to be changed          *
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public void saveResources(Collection resourceDescriptors, String principalName) throws ConfigurationException {


        try {

            configUserTransaction = getConfigurationWriter().getTransaction(principalName);

	        for (Iterator it=resourceDescriptors.iterator(); it.hasNext(); ) {
	            SharedResource rd = (SharedResource) it.next();

	            getConfigurationWriter().updateSharedResource(rd, configUserTransaction.getTransaction());

	        }

        } catch (ConfigTransactionException te) {
            throw new ConfigurationException(te);
        }


    }


    protected XMLConfigurationReader getConfigurationReader() throws ConfigurationException {
        if (reader == null) {
        	try {
	            reader = new XMLConfigurationReader(getConnection());
        	} catch (ManagedConnectionException mce) {
        		throw new ConfigurationException(mce);
        	}
        }
        return reader;
    }

    protected XMLConfigurationWriter getConfigurationWriter() throws ConfigurationException {
        if (writer == null) {
        	try {
	            writer = new XMLConfigurationWriter(getConnection());
        	} catch (ManagedConnectionException mce) {
        		throw new ConfigurationException(mce);
        	}


        }
        return writer;
    }

}





