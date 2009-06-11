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

import java.util.EventObject;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;

/**
 * Created on Aug 27, 2002
 *
 * The Mgr operates under the singleton pattern.  It uses other components
 * to manage the configuration.  This would include transactions and persistence.
 * Those components are implemented such they can be changed so that the
 * behavior of the mgr is changed.
 */
public class XMLConfigurationMgr {

    private static XMLConfigurationMgr mgr;

    private PersistentConnectionFactory connFactory;
    private MessageBus messageBus;

    private ConfigurationModelContainerImpl readOnlyConfig;
    private ConfigurationModelContainerAdapter adapter =  new ConfigurationModelContainerAdapter();
    
    private XMLConfigurationMgr(Properties properties) throws ConfigurationException  {
        Assertion.isNotNull(properties, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0113));

        connFactory = new PersistentConnectionFactory(properties);

        getConfigurationModel(Configuration.NEXT_STARTUP_ID);
    }
    
    protected XMLConfigurationMgr() {
    	
    }
    
    public void setMessageBus(MessageBus messageBus) {
		this.messageBus = messageBus;
    	if (messageBus != null) {
	    	try {
				messageBus.addListener(ConfigurationChangeEvent.class, new EventObjectListener() {
		            public void processEvent(EventObject obj){
		                if(obj instanceof ConfigurationChangeEvent){
		                	mgr.clearCache();
		                }
		            }
		    	});
			} catch (MessagingException e) {
				throw new MetaMatrixRuntimeException(e);
			}
    	}
	}

    /**
     * Always call getInstance to get the reference to the XMLConfigurationMgr to
     * use to make other method calls on.
     */
    public static final synchronized XMLConfigurationMgr getInstance() {
        if (mgr == null) {
        	try {
				mgr = new XMLConfigurationMgr(CurrentConfiguration.getInstance().getBootStrapProperties());
			} catch (ConfigurationException e) {
				throw new MetaMatrixRuntimeException(e);
			}
        }
        return mgr;
    }
    
    public XMLConfigurationConnector getTransaction(String principal) throws ConfigurationException {
    	return new XMLConfigurationConnector(this, getConfigurationModelForTransaction(Configuration.NEXT_STARTUP_ID), principal);
    }

    /**
     * Returns the configuration for the specified configID.
     * {@see Configuration}.
     */
    public synchronized ConfigurationModelContainerImpl getConfigurationModel(ConfigurationID configID) throws ConfigurationException {

        if (this.readOnlyConfig == null) {
        	PersistentConnection pc = getConnection(true);

        	try {
        		this.readOnlyConfig = (ConfigurationModelContainerImpl)pc.read(configID);
        	} finally {
        		pc.close();
        	}
        	
            if (this.readOnlyConfig == null) {
                throw new ConfigurationException(ConfigMessages.CONFIG_0114,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0114, configID));
            }

        }
        return this.readOnlyConfig;

    } 

	/**
	 * This method is only used by the XMLConfigurationWriter so that is may obtain
	 * a model specifically for transaction purposes.
	 */
    private ConfigurationModelContainerImpl getConfigurationModelForTransaction(ConfigurationID configID) throws ConfigurationException {
        ConfigurationModelContainer cmc = getConfigurationModel(configID);
        return (ConfigurationModelContainerImpl)cmc.clone();
    }

    /**
     * Apply transaction is called when the Transaction is committed.
     * @param transaction is the transaction that contains the object model that changed
     * @throws ConfigurationException if a problem occurs setting the configuration.
     */
    synchronized void applyTransaction(ConfigurationModelContainerImpl cmc, String principal) throws ConfigurationException {

		PersistentConnection pc = null;
		boolean success = false;
		try {
			// validate the model before saving
			adapter.validateModel(cmc);
			
			cmc = (ConfigurationModelContainerImpl)cmc.clone();
			pc = getConnection(false);
			pc.write(cmc, principal);
						
			pc.commit();
			success = true;
			this.readOnlyConfig = cmc;
		} catch (ConfigObjectsNotResolvableException e) {
			throw new ConfigurationException(e, ConfigMessages.CONFIG_0120,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0120, cmc.getConfigurationID()));
		} finally {
			if (pc != null) {
				try {
					if (!success && pc != null) {
						pc.rollback();
					}
				} finally {
					pc.close();
				}
			}
		}

		if (messageBus != null) {
			try {
				messageBus.processEvent(new ConfigurationChangeEvent(XMLConfigurationMgr.class.getName(),ConfigurationChangeEvent.CONFIG_REFRESH));
			} catch (MessagingException e) {
				LogManager.logWarning(LogCommonConstants.CTX_CONFIG, e,"Exception sending refresh event"); //$NON-NLS-1$
			}
		}
	}

    /**
     * This method should connect to the persitent storage.
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */
    private PersistentConnection getConnection(boolean readOnly) throws ConfigurationException{
    	return connFactory.createPersistentConnection(readOnly);
    }

    private synchronized void clearCache() {
    	this.readOnlyConfig = null;
    }
    
    // this method is need for testing so that the Mgr can be recreated with a different set of properties.
    public void reset() {
    	this.mgr = null;
    }

	/**
	 * Returns ID of one of the well-known configuration.  Will
	 * return null if the designation parameter is invalid.
	 * @param designation String indicating which of the system configurations
	 * is desired; use one of the {@link SystemConfigurationNames} constants
	 * @param jdbcConnection connection to the proper config database
	 * @return the desired ConfigurationID
	 * @throws ConfigurationException if an error occurred within or during
	 * communication with the Configuration Service.
	 */
	public static ConfigurationID getDesignatedConfigurationID(String designation ) throws ConfigurationException{
	    // This was changed to public so installation could use the method
	
		if (designation.startsWith(Configuration.NEXT_STARTUP) ) {
	        return Configuration.NEXT_STARTUP_ID;
	    } 
		throw new ConfigurationException(ConfigMessages.CONFIG_0128, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0128, designation ));
	}

}
