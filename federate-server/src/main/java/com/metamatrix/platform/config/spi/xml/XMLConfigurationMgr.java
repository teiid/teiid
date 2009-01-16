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
import java.util.EventObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;

/**
 * Created on Aug 27, 2002
 *
 * The Mgr operates under the single pattern.  It uses other components
 * to manage the configuration.  This would include transactions and persistence.
 * Those components are implemented such they can be changed so that the
 * behavior of the mgr is changed.
 *
 * NOTE:
 * -   STARTUP - before any changes can be made to any configuration, the
 * 				 the performSystemInitialization must be performed.
 */
public class XMLConfigurationMgr {

    private static XMLConfigurationMgr mgr = null;

    private PersistentConnection connection = null;
    private PersistentConnectionFactory connFactory = null;
    
    private Properties props = null;

    private MessageBus messageBus = null;


    private Map configs = new HashMap();

    // this map of config models are only used by the writer for transaction processing
    //
    // NOTE: this is done to keep the problem where changes are applied
    //		 to the same config model reference twice.
    // HOW DOES THIS HAPPEN:  When processing is done within the same VM,
    //			the same reference used by the ConfigurationObjectEditor is also
    //			used in the update transaction {@see XMLConfigurationWriter.getWriteTransaction}
    //			therefore, a change applied by the editor to the configuration object
    //			now exist in the model when the same reference is used to apply the
    //			actions created by the editor.
    private Map transConfigs = new HashMap();

    private ConfigurationModelContainerAdapter adapter =  new ConfigurationModelContainerAdapter();
    private String hostName = null;
    
    private XMLConfigurationMgr(MessageBus bus)  {
    	this.messageBus = bus;
    	this.hostName = VMNaming.getConfigName();
    	
    	try {
			messageBus.addListener(ConfigurationChangeEvent.class, createChangeListener());
		} catch (MessagingException e) {
			System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0126));		
		}
    }

    /**
     * Always call getInstance to get the reference to the XMLConfigurationMgr to
     * use to make other method calls on.
     */
    public static final synchronized XMLConfigurationMgr getInstance() {

        if (mgr == null) {

             XMLConfigurationMgr xmlMgr = new XMLConfigurationMgr(ResourceFinder.getMessageBus());
             mgr = xmlMgr;

        }
        return mgr;

    }

    public synchronized void init(Properties properties) throws ConfigurationException {

       
        // if already initialized before, don't reinit everything
		if (this.connection != null && !connection.isClosed()) {
			return;
		}
        
        if(properties == null){
            Assertion.isNotNull(properties, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0113));
        }


        this.props = PropertiesUtils.clone(properties, false);

        Properties factoryProps = new Properties();
        factoryProps.putAll(this.props);

        connFactory = PersistentConnectionFactory.createPersistentConnectionFactory(props);
//        createPersistentConnectionFactory(props);

         connection = connFactory.createPersistentConnection();

        // preload next startup
         ConfigurationModelContainer cmc = readModel(Configuration.NEXT_STARTUP_ID);

         if (cmc == null) {
	         throw new ConfigurationException(ConfigMessages.CONFIG_0114, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0114, Configuration.NEXT_STARTUP_ID ));
         }

         configs.put(cmc.getConfigurationID().getFullName(), cmc);

          try {
         	  ConfigurationModelContainer cmct = (ConfigurationModelContainer) cmc.clone();

     	  	  transConfigs.put(cmct.getConfigurationID().getFullName(), cmct);
          } catch (Exception ce) {
          	throw new ConfigurationException(ce,ConfigMessages.CONFIG_0115, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0115,cmc.getConfigurationID()));
          }
    }

	protected Properties getProperties() {
		return this.props;
	}

    public synchronized java.util.Date getServerStartupTime() throws ConfigurationException {
		return getConnection().getStartupTime();
    }


    public synchronized int getServerStartupState() throws ConfigurationException {
    	return getConnection().getServerState();

    }

    public boolean isServerStarting() throws ConfigurationException {
    	int startupState = getServerStartupState();

 		if (startupState == StartupStateController.STATE_STARTING) {
 			return true;
 		}
 		return false;
    }


    public boolean isServerStopped() throws ConfigurationException {
    	int startupState = getServerStartupState();

 		if (startupState == StartupStateController.STATE_STOPPED) {
 			return true;
 		}
 		return false;
    }

    public boolean isServerAvailable() throws ConfigurationException {
		// the assumption here is that onle one user can have a lock at a time,
		// therefore, if a change has been committed therefore they have the lock
		// and the server state must be updated on their behalf
    	int startupState = getServerStartupState();


 		if (startupState == StartupStateController.STATE_STARTED) {
 			return true;
 		}
 		return false;
    }


    /**
     * The setting of the server state is not part
     */

    protected synchronized void setServerStateToStarting(boolean force) throws StartupStateException, ConfigurationException {
        PersistentConnection conn = getConnection();
		if (force) {
            conn.setServerStarting(force);
		} else {

            conn.setServerStarting();
		}

    }

    protected synchronized void setServerStateToStopped() throws StartupStateException,ConfigurationException {

        getConnection().setServerStopped();

    }

    protected synchronized void setServerStateToStarted() throws StartupStateException,ConfigurationException {

        getConnection().setServerStarted();
    }

    /**
     * Returns the configuration for the specified configID.
     * {@see Configuration}.
     */
    public synchronized ConfigurationModelContainer getConfigurationModel(ConfigurationID configID) throws ConfigurationException {
        ConfigurationModelContainer cmc = null;

        if (configs.containsKey(configID.getFullName())) {
            cmc = (ConfigurationModelContainer)configs.get(configID.getFullName());
        }

        if (cmc == null) {
            cmc = readModel(configID);

            if (cmc == null) {
                throw new ConfigurationException(ConfigMessages.CONFIG_0114,
                                                 ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0114, configID));
            }

            configs.put(cmc.getConfigurationID().getFullName(), cmc);

            try {
                ConfigurationModelContainer cmct = (ConfigurationModelContainer)cmc.clone();

                transConfigs.put(cmct.getConfigurationID().getFullName(), cmct);
            } catch (Exception ce) {
                throw new ConfigurationException(ce, ConfigMessages.CONFIG_0116,
                                                 ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0116, configID));
            }
        }
        return cmc;

    } 

    private synchronized ConfigurationModelContainer readModel(ConfigurationID configID) throws ConfigurationException {
    	return getConnection().read(configID);
    }

	/**
	 * This method is only used by the XMLConfigurationWriter so that is may obtain
	 * a model specifically for transaction purposes.
	 */
    synchronized ConfigurationModelContainer getConfigurationModelForTransaction(ConfigurationID configID) throws ConfigurationException {
          if (transConfigs.containsKey(configID.getFullName())) {
              return (ConfigurationModelContainer) transConfigs.get(configID.getFullName());
          }
          
          // call to refresh caches
          getConfigurationModel(configID);
          
        if (transConfigs.containsKey(configID.getFullName())) {
            return (ConfigurationModelContainer) transConfigs.get(configID.getFullName());
        }
          
          
        throw new ConfigurationException(ConfigMessages.CONFIG_0114, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0114, configID));                    

    }

//******************************
//
// Transaction related methods
//
//******************************


    public synchronized void rollbackTransaction() {
        this.connection.close();
        this.connection = null;
    }

    /**
     * Apply transaction is called when the Transaction is committed.
     * @param transaction is the transaction that contains the object model that changed
     * @throws ConfigurationException if a problem occurs setting the configuration.
     */
    public synchronized void applyTransaction(ConfigTransaction transaction) throws ConfigTransactionException {
        ArgCheck.isNotNull(transaction, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0118));

//		System.out.println("MGR - APPLY TRANSACTION " + transaction.getAction());
		// only for non-initialization or server startup actions will the configuration be written
		// the no server initialize actions are created due to specific changes to the configuration
		// the initialize actions are results of starting and bouncing the server
		if (transaction.getAction() == ConfigTransaction.NO_SERVER_INITIALIZATION_ACTION ||
			transaction.getAction() == ConfigTransaction.SERVER_FORCE_INITIALIZATION ||
			transaction.getAction() == ConfigTransaction.SERVER_INITIALIZATION) {

			if (transaction.getObjects() == null || transaction.getObjects().isEmpty()) {
	              throw new ConfigTransactionException(ConfigMessages.CONFIG_0119, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0119));
			}

			Collection models = transaction.getObjects();
			PersistentConnection pc = null;
			boolean success = false;
			try {
				for (Iterator it=models.iterator(); it.hasNext(); ) {
	
					Object obj = it.next();
					if (!(obj instanceof ConfigurationModelContainer)) {
		              	throw new ConfigTransactionException(ConfigMessages.CONFIG_0121, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0121, obj.getClass().getName()));
					}
					ConfigurationModelContainer config = (ConfigurationModelContainer) obj;
	
					try {
						
						//validate the model before saving
			             adapter.validateModel(config);
	
						if (pc == null) {
							pc = getConnection();
							pc.beginTransaction();
						}

				        pc.write(config, transaction.getLockAcquiredBy());
				        //                            transConfigs.clear();  
	
	//							clearCache();
	//					System.out.println("<CONFIGMGR> write configuration " + config.getConfigurationID().getFullName());
	
	/*
								ConfigurationModelContainer checkM = readModel(config.getConfigurationID());
								if (checkM == null) {
									throw new ConfigTransactionException("Error persisting configuration " + config.getConfigurationID() + ", it was not saved properly");
								}
	*/                            
	//                            
						configs.put(config.getConfigurationID().getFullName(), config);
	
                        ConfigurationModelContainer cmct = (ConfigurationModelContainer) config.clone();

                        transConfigs.put(cmct.getConfigurationID().getFullName(), cmct);
					} catch (ConfigurationException ce) {
						throw new ConfigTransactionException(ce, ConfigMessages.CONFIG_0120, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0120, config.getConfigurationID()));
					} catch (CloneNotSupportedException e) {
						throw new ConfigTransactionException(e,ConfigMessages.CONFIG_0115, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0115,config.getConfigurationID()));
					} catch (ConfigObjectsNotResolvableException e) {
						throw new ConfigTransactionException(e, ConfigMessages.CONFIG_0120, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0120, config.getConfigurationID()));
					} 
				}
				try {
					pc.commit();
				} catch (ConfigurationException e) {
					throw new ConfigTransactionException(e, e.getMessage());
				}
				success = true;
			} finally {
				if (!success && pc != null) {
					try {
						pc.rollback();
					} catch (ConfigurationException e) {
						throw new ConfigTransactionException(e, e.getMessage());
					}
				}
			}
		}

		if (transaction.getAction() != ConfigTransaction.SERVER_SHUTDOWN) {
	    	 if(messageBus != null){
	    	 	try{
//	    	 		System.out.println("<CONFIG_MGR>Send Change Event " + v);
	            	messageBus.processEvent(new ConfigurationChangeEvent(hostName, ConfigurationChangeEvent.CONFIG_REFRESH));
	    	 	}catch(Exception e){
	    	 		System.err.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0122, e.getMessage()));

	    	 	}
	    	 }
		}


    }




    /**
     * Call to initialize persistence when the NextStartUp configuration
     * needs to copied to the Operation configuation.
     *
     * This method is used for the following reasons:
     * <li> When the ServerMgr starts it calls performSystemInitialization
     * <li> Bouncing server
     */
    void performSystemInitialization(ConfigTransaction transaction) throws ConfigurationException, StartupStateException, ConfigurationLockException {

		if (!isServerStarting()) {
	 		throw new StartupStateException(StartupStateController.STATE_STARTING, getServerStartupState());
		}

         
          ConfigurationModelContainer ns =  readModel(Configuration.NEXT_STARTUP_ID);

          ConfigurationModelContainer st = ns.copyAs(Configuration.STARTUP_ID);

	      transaction.addObjects(ns.getConfigurationID().getFullName(), ns);
	      transaction.addObjects(st.getConfigurationID().getFullName(), st);

    }


    /**
     * This method should connect to the persitent storage.
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */

    synchronized PersistentConnection getConnection(  ) throws ConfigurationConnectionException{
        if (connection==null) {
            try {
                connection = connFactory.createPersistentConnection();
            } catch (ConfigurationException e) {
                throw new ConfigurationConnectionException(e);
            }
        } else {
            if (connection.isClosed()) {
                try {
                    connection = connFactory.createPersistentConnection();
                } catch (ConfigurationException e) {
                    throw new ConfigurationConnectionException(e);
                }
                
            }
        }

        return this.connection;

    }



    protected synchronized void clearCache() {
    	configs.clear();
        transConfigs.clear();  
    }

    protected  ConfigurationChangeListener createChangeListener() {
    	return new ConfigurationChangeListener(this);
    }


    protected class ConfigurationChangeListener implements EventObjectListener{
    	private XMLConfigurationMgr mgr = null;

    	public ConfigurationChangeListener(XMLConfigurationMgr theMgr) {
    		mgr = theMgr;
    	}

        public void processEvent(EventObject obj){
            if(obj instanceof ConfigurationChangeEvent){
//                ConfigurationChangeEvent eventObj = (ConfigurationChangeEvent)obj;
                // [vah 5/23/02] per Steve W., the events passed along the
                // JMS Message Bus were not originally notifing all JVMs
                // The change was made to the JMSMessageBus to notify all JVMs
                // so the change here is to not execute the change
                // if the event came from the same VM

                // Null source object means the event came from another VM

//                if (eventObj.getSource() == null) {
                	mgr.clearCache();
//                }
            }
        }
    }
}
