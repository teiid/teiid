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
import java.util.Properties;

import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.reader.CurrentConfigurationReader;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class XMLCurrentConfigurationReader implements CurrentConfigurationReader {
    /**
     * The date of installation of the MetaMatrix suite
     */
    public static final String INSTALL_DATE = "metamatrix.config.installationDate"; //$NON-NLS-1$

    private static final String PRINCIPAL = "CurrentConfiguration"; //$NON-NLS-1$

    private static XMLConfigurationMgr configMgr = XMLConfigurationMgr.getInstance();

    private XMLConfigurationReader reader = null;
    private XMLConfigurationWriter writer = null;



    public XMLCurrentConfigurationReader() {
    }


   /**
     * This method should connect to the repository that holds the current
     * configuration, using the specified properties.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @param env the environment properties that define the information
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */
    public synchronized void connect( Properties env ) throws ConfigurationConnectionException{
        try {
            // this needs to be done first before readers and writers use it.
			configMgr.init(env);


        } catch(Exception e) {
            e.printStackTrace();
            throw new ConfigurationConnectionException(e, ConfigMessages.CONFIG_0133, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0133));

        }


        try {
         	XMLConfigurationConnectorFactory factory = new XMLConfigurationConnectorFactory();
         	ManagedConnection mc = factory.createConnection(env, PRINCIPAL);
            
            if(mc == null){
         	  Assertion.isNotNull(mc, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0135));
            }

         	this.reader = new XMLConfigurationReader(mc);
			this.writer = new XMLConfigurationWriter(mc);
            
            if(reader == null){
        	   Assertion.isNotNull(reader, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0136));
            }
            if(writer == null){
        	   Assertion.isNotNull(writer, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0137));
            }

        } catch(Exception e) {
            e.printStackTrace();
            throw new ConfigurationConnectionException(e, ConfigMessages.CONFIG_0138, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0138));

        }


    }


    /**
     * This method should close the connection to the repository that holds the current
     * configuration.  The implementation may <i>not</i> use logging but
     * instead should rely upon returning an exception in the case of any errors.
     * @throws Exception if there is an error establishing the connection.
     */

    public void close() throws Exception{
  /*
        if ( this.connection != null ) {
            try{
                this.connection.close();
            } catch (SQLException e) {
                throw new ConfigurationConnectionException(e, "JDBCCurrentConfigurationReader - could not close connection");
            }
            this.connection = null;
        }
   */
    }

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Obtain the properties for the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the properties
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public Properties getConfigurationProperties() throws ConfigurationException{
        Properties result = getConfigurationReader().getDesignatedConfigurationProperties(Configuration.NEXT_STARTUP);

        if ( result == null ) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0139, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0139));
        }

        return result;
    }

   /**
     * Obtain the next startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */

    public Configuration getNextStartupConfiguration() throws ConfigurationException {
        return getConfigurationReader().getDesignatedConfiguration(Configuration.NEXT_STARTUP_ID);
    }

   /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
        return getConfigurationReader().getConfigurationModel(Configuration.NEXT_STARTUP_ID);
    }
    
	public synchronized void performSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException {

    /*


    the basic process is this:
-  The performInitialization process will obtain a lock using the user of "SystemInitialization"
-  The state of server should be STOPPED
-  Set the startup state to STARTING
-  The lock will be registered with userName, Start Date/Time of Lock, Expiration of Lock
-   Initialize the models
-   Set the startup state to STARTED
-   Release the lock

if the process ends abrubtly and the state doesn't get changed, here's how it can be handled:
-  The performInitialization process is started again
-  Get the current state of the server
	-  if state = STOPPED
		- if can obtain lock then perform system initialization
		otherwise
		- if another user already has a lock then see performLockCheck ROUTINE

	- if state = STARTING
		- if no lock is held then proceed to perform system initialization (This assumes the system never previously fully started)
		or
		- if another user already has a lock then see performLockCheckK ROUTINE

	- if state = STARTED
		- if no lock is held then perform system initialization (this assumes the user wants to restart the server - i.e., bounce or when a server stops abrubtly and the state never gets set back to stopped)
		This is also making the assumption that only one server in a clustered environment should have called the startserver.
		However,, it doesn't hurt if they do, it just means the NextStartup model will be copied to Startup.
		- if another user already has a lock then see performLockCheck ROUTINE

    */

    boolean force = !configMgr.isServerStopped();
    
    ConfigUserTransaction inittrans = null;
        try {
            inittrans = getWriteTransaction();
        	
			ConfigTransaction cfgt = inittrans.getTransaction();

	  		getConfigurationWriter().beginSystemInitialization(force, cfgt);

	  		getConfigurationWriter().performSystemInitialization(cfgt);

	  		getConfigurationWriter().finishSystemInitialization(cfgt);

            inittrans.commit();

         } catch(TransactionException te) {
         	if (inittrans != null) {
         		rollbackTransaction(inittrans);
         	}

			throw new ConfigurationException(te, ConfigMessages.CONFIG_0143, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0143));

         }

	}

    /**
     * @see com.metamatrix.common.config.reader.CurrentConfigurationInitializer#indicateSystemShutdown
     */
    public synchronized void indicateSystemShutdown() throws ConfigurationException{

    	ConfigUserTransaction trans = null;
        try {
        	trans = getWriteTransaction();
	  		getConfigurationWriter().indicateSystemShutdown(trans.getTransaction());
        	trans.commit();
         } catch(StartupStateException sse) {
         	if (trans != null) {
				try {
					trans.rollback();

				} catch(Exception e) {
				}
         	}

			throw new ConfigurationException(sse, ConfigMessages.CONFIG_0143, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0143));


         } catch(TransactionException te) {
         	if (trans != null) {
				try {
					trans.rollback();

				} catch(Exception e) {
				}
         	}

			throw new ConfigurationException(te, ConfigMessages.CONFIG_0143, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0143));

         }

    }

    private void rollbackTransaction(ConfigUserTransaction trans) {
			try {
				trans.rollback();

			} catch(Exception e) {
			}
    }

    protected ConfigUserTransaction getWriteTransaction() throws ConfigTransactionException, ConfigurationException {

		 return getConfigurationWriter().getTransaction(PRINCIPAL);
    }

    protected XMLConfigurationReader getConfigurationReader() {
        return this.reader;
    }

    protected XMLConfigurationWriter getConfigurationWriter() {
        return this.writer;
    }


}

