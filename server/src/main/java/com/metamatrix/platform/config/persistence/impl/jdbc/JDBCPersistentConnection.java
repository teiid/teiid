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

package com.metamatrix.platform.config.persistence.impl.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionModuleReader;
import com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionModuleWriter;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCPlatformFactory;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class JDBCPersistentConnection implements PersistentConnection {


    /**
     * env properties are exprected to be that of JDBCConnectionResource
     */

    private Connection connection;


    private ConfigurationModelContainerAdapter adapter;
    private JDBCPlatform platform;

    public JDBCPersistentConnection(Connection conn, ConfigurationModelContainerAdapter adapter, Properties props)  {
	   Assertion.isNotNull(conn, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0146));
	   Assertion.isNotNull(adapter, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0147));

 		this.connection = conn;
        this.adapter = adapter;

        try {
            this.platform = JDBCPlatformFactory.getPlatform(conn);
        } catch (MetaMatrixException e) {
            this.platform = null;
        }
    }


    protected void finalize() {
        try {
            close();
//            System.out.println(new java.util.Date() + " <JDBCPersistenctConnection> Finalize");
        } catch ( Exception e ) {
            // Should never happen, but ...
            e.printStackTrace(System.err);
        }
    }

	public void close() {
//        System.out.println(new java.util.Date() + " <JDBCPersistenctConnection> Close");
      
        if (!isClosed()) {
            try {
 	                this.connection.close();
            } catch (SQLException e) {
//                throw new ConfigurationConnectionException(e, "JDBCPersistentConnection - could not close connection");
            }
        }
        this.connection = null;

	}

   public boolean isClosed() {
       if ( this.connection != null ) {
           try{
               if (this.platform != null) {
                   return this.platform.isClosed(connection);
               }
               
               return this.connection.isClosed();
           } catch (SQLException e) {
           }
       }
       return true;
       
       
   }
	/**
	 * Call to set the startup state to @see {StartupStateController.STARTING Starting}.
	 * The server must be in the STOPPED state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state is not currently
	 * set to STOPPED.
	 */
    public synchronized void setServerStarting() throws StartupStateException, ConfigurationException {
		int state = getServerState();
		if (state != StartupStateController.STATE_STOPPED) {
			throw new StartupStateException(StartupStateController.STATE_STOPPED, state);
		}

		updateStartupState(StartupStateController.STATE_STOPPED, StartupStateController.STATE_STARTING);

    }

	/**
	 * Call to forcibly set the startup state to @see {StartupStateController.STARTING Starting},
	 * regardless of the current state of the server.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStarting( boolean force) throws StartupStateException, ConfigurationException {
		forceUpdateStartupState(StartupStateController.STATE_STARTING);

    }

	/**
	 * Call to set the startup state to @see {StartupStateController.STARTED Started}.
	 * The server must be in the STARTING state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStarted( ) throws StartupStateException, ConfigurationException {
		int state = getServerState();

		if (state != StartupStateController.STATE_STARTING) {
			throw new StartupStateException(StartupStateController.STATE_STARTING, state);
		}

		updateStartupState(StartupStateController.STATE_STARTING, StartupStateController.STATE_STARTED);

    }


	/**
	 * Call to set the startup state to @see {StartupStateController.STOPPED Stopped}.
	 * This is normally called when the system is shutdown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStopped() throws StartupStateException, ConfigurationException {
		forceUpdateStartupState(StartupStateController.STATE_STOPPED);

    }

    /**
     * Returns the int startup state, use constants in
     * {@link com.metamatrix.common.config.StartupStateController} to
     * interpret the meaning
     */
	public synchronized int getServerState() throws ConfigurationException {
        int state = -1;

        String sql = null;
        PreparedStatement statement = null;
        try{
            sql = SQL_Translator.SELECT_STARTUP_STATE;
            statement = connection.prepareStatement(sql);

            if ( ! statement.execute() ) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0148, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0148, sql));
            }
            ResultSet results = statement.getResultSet();

            if (results.next()) {
                state = SQL_Translator.getStartupState(results);
            } else {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0149, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0149));
            }
            results.close();
        } catch ( SQLException e ) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0150, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0150, sql));
        } catch ( Exception e ) {
            if (e instanceof ConfigurationException){
                throw (ConfigurationException)e;
            }
            throw new ConfigurationException(e);
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                }
            }
        }

        return state;


	}


	/**
	 * Call to get the current state
	 * @return int state @see {StartupStateController Controller}
	 */
	public synchronized java.util.Date getStartupTime() throws ConfigurationException {
		java.util.Date timestamp = null;
        String sql = null;
        PreparedStatement statement = null;
        try{
            sql = SQL_Translator.SELECT_STARTUP_STATE;
            statement = connection.prepareStatement(sql);

            if ( ! statement.execute() ) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0148, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0148, sql));
            }
            ResultSet results = statement.getResultSet();

            timestamp = SQL_Translator.getServerStartupTime(results);

        } catch ( SQLException e ) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0150, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0150, sql));
        } catch ( Exception e ) {
            if (e instanceof ConfigurationException){
                throw (ConfigurationException)e;
            }
            throw new ConfigurationException(e);
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                }
            }
        }

		return timestamp;

	}


    /**
     * Returns an ConfigurationModelContainer based on how the implementation read configuation information
     * @param configID indicates which configuration to read
     * @return ConfigurationModel
     * @throws ConfigurationException if an error occurs
     */
    public synchronized ConfigurationModelContainer read(ConfigurationID configID) throws ConfigurationException {
    	return readExtensionConfiguration(configID, true);

    }

    /**
     * Writes the model to its persistent store based on the implementation of
     * the persistent connection.
     * @param model to be writen to output
     * @param principal is the user executing the write
     * @throws ConfigurationException if an error occurs
     */
    public synchronized void write(ConfigurationModelContainer config, String principalName) throws ConfigurationException {
        try {
        	ByteArrayOutputStream out = new ByteArrayOutputStream();
        	BufferedOutputStream bos = new BufferedOutputStream(out);

            adapter.writeConfigurationModel(bos, config, principalName);

			bos.close();
			out.close();


            byte[] data = out.toByteArray();

            if (data == null || data.length == 0) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0156, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0156));
            }

            boolean inUse = JDBCExtensionModuleReader.isNameInUse(config.getConfigurationID().getFullName(), connection);

            if (inUse) {
            	// update the source
//                System.out.println("<EXPORT JDBC CONFIG> Update Configuration " + config.getConfigurationID());

                JDBCExtensionModuleWriter.setSource(principalName, config.getConfigurationID().getFullName(), data, data.length, connection);
            } else {
//             System.out.println("<EXPORT JDBC CONFIG> Insert Configuration "+ config.getConfigurationID());
   				// insert the source
                JDBCExtensionModuleWriter.addSource(principalName, ExtensionModuleTypes.CONFIGURATION_MODEL_TYPE,
                                    config.getConfigurationID().getFullName(),
                                    data,
                                    data.length,
                                    config.getConfigurationID().getFullName() + " Configuration Model", //$NON-NLS-1$
                                    true,
                                    connection);
//                 System.out.println("EXT DESC " + (ed == null ? " NULL " : ed.getName()) );

            }
             
            // load the system properties table only when the Startup configuration is being saveed 
            if (config.getConfigurationID().getFullName().equals(Configuration.STARTUP)) {
                cleanSystemPropertiesTable();
                
                ComponentTypeID id = (ComponentTypeID) config.getComponentType(Configuration.COMPONENT_TYPE_NAME).getID();
                               
                Properties allprops = new Properties();
                allprops.putAll(config.getDefaultPropertyValues(id));
                allprops.putAll(config.getConfiguration().getProperties());
                
                insertSystemProperties(allprops);
                
            }


        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0157, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0157, config.getConfigurationID()));

        }
    }

    /**
     * Writes the collection of models to its persistent store based on the implementation of
     * the persistent connection.
     * @param models to be writen to output
     * @param principal is the user executing the write
     * @throws ConfigurationException if an error occurs
     */

    public synchronized void delete(ConfigurationID configID, String principal) throws ConfigurationException {
        try {


            boolean inUse = JDBCExtensionModuleReader.isNameInUse(configID.getFullName(), connection);

            if (inUse) {
                JDBCExtensionModuleWriter.removeSource(principal, configID.getFullName(), connection);
            }


        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0153, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0153, configID));

       }
    }



    /**
     * Uses the ExtensionManager to obtain the configuration xml file
     *
     * @param errorOnNotFound is used in 2 ways
     * 1.  To support the transition from table based to xml based configurations, the
     *      first time the transition occurs the extension will not exist, therefore do
     *      not throw an error
     * 2.  After the initial transition, the error should be thrown because it should
     *      have been inserted
     */
    private ConfigurationModelContainer readExtensionConfiguration(ConfigurationID configID, boolean errorOnNotFound) throws ConfigurationException {

        try {
            if(this.connection == null){
        	   Assertion.isNotNull(this.connection, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0152));
            }

 //           System.out.println("<IMPORT CONFIG> Loading extension configuration for " + configID);

            byte[] data = JDBCExtensionModuleReader.getSource(configID.getFullName(), connection);

            if (data == null) {
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0154, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0154, configID));

//                System.out.println("<IMPORT CONFIG>No Extension found for " + configID);
//                return null;
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            InputStream isContent = new BufferedInputStream(bais);

//            System.out.println("<IMPORT CONFIG> Loaded extension configuration " + configID);

        	ConfigurationModelContainer model = this.adapter.readConfigurationModel(isContent, configID);


            return model;
        } catch (ExtensionModuleNotFoundException notFound) {
            if (errorOnNotFound) {
                throw new ConfigurationException(notFound, ErrorMessageKeys.CONFIG_0154, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0154, configID));
            }
 //               System.out.println("<IMPORT CONFIG> No extension configuration found for " + configID );

            return null;
        } catch (Exception e) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0155, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0155, configID));
        }

    }

    /**
     * Called to clean out the system properties table before reloading
     */
     void cleanSystemPropertiesTable() throws SQLException{

        String sql = null;
        Statement statement = null;
        try{
            sql = SQL_Translator.EMPTY_CS_SYSTEM_PROPERTY_TABLE;
            statement = connection.createStatement();

            statement.execute(sql);

        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                }
            }
        }
    }
     
     void insertSystemProperties(Properties props) throws SQLException{

         String sql = null;
         PreparedStatement statement = null;
         try{
             sql = SQL_Translator.INSERT_INTO_CS_SYSTEM_PROPERTY_TABLE;
             statement = connection.prepareStatement(sql);
             
             for (Iterator it=props.keySet().iterator(); it.hasNext();) {
                 String key=(String) it.next();
                 String value= ((String) props.get(key)).trim();
                 
                 statement.setString(1, key);
                 if (value.length() > 255) {
                     value = value.substring(0, 255);
                 }
                 statement.setString(2, value);
                 if (!statement.execute()) {
                     //;
                 }
                 
                 statement.clearParameters();
             }

         } finally {
             if ( statement != null ) {
                 try {
                     statement.close();
                 } catch ( SQLException e ) {
                     e.printStackTrace();
                     System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                 }
             }
         }
     }
     
    


    /**
     * Called by {@link JDBCCurrentConfigurationReader#finishSystemInitialization}
     * @see JDBCCurrentConfigurationReader#finishSystemInitialization
     */
     void updateStartupState(int from, int to) throws StartupStateException, ConfigurationException{

        String sql = null;
        PreparedStatement statement = null;
        try{
            sql = SQL_Translator.UPDATE_STARTUP_STATE_CONDITIONAL;
            statement = connection.prepareStatement(sql);
            statement.setInt(1, to);
            statement.setString(2, DateUtil.getCurrentDateAsString());
            statement.setInt(3, from);

            statement.execute();
            int updateCount = statement.getUpdateCount();

            if (updateCount == 0){
                int startupState = getServerState();
                throw new StartupStateException(StartupStateController.STATE_STARTED, startupState);
            } else if (updateCount < 0){
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0158, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0158, sql));
            }

        } catch ( SQLException e ) {
            throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0150, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0150, sql));
        } catch ( Exception e ) {
            if (e instanceof ConfigurationException){
                throw (ConfigurationException)e;
            } else if (e instanceof StartupStateException){
                throw (StartupStateException)e;
            }
            throw new ConfigurationException(e);
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                }
            }
        }
    }

    /**
     * Called by {@link JDBCCurrentConfigurationReader#indicateSystemShutdown}
     * @see JDBCCurrentConfigurationReader#indicateSystemShutdown
     */
    void forceUpdateStartupState(int state) throws ConfigurationException{

        String sql = null;
        PreparedStatement statement = null;
        try{
            sql = SQL_Translator.UPDATE_STARTUP_STATE_UNCONDITIONAL;
            statement = connection.prepareStatement(sql);
            statement.setInt(1, state);
//            statement.setInt(1, StartupStateController.STATE_STOPPED);

            statement.setString(2, DateUtil.getCurrentDateAsString());

            statement.execute();
            int updateCount = statement.getUpdateCount();
            if (updateCount <= 0){
                throw new ConfigurationException(ErrorMessageKeys.CONFIG_0158, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0158, sql));
            }

        } catch ( SQLException e ) {
             throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0150, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0150, sql));
        } catch ( Exception e ) {
            if (e instanceof ConfigurationException){
                throw (ConfigurationException)e;
            }
            throw new ConfigurationException(e);
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0151, sql));
                }
            }
        }
    }

	@Override
	public void beginTransaction() throws ConfigurationException {
		try {
			this.connection.setAutoCommit(false);
		} catch (SQLException e) {
			throw new ConfigurationException(e);
		}
	}


	@Override
	public void commit() throws ConfigurationException {
		try {
			this.connection.commit();
		} catch (SQLException e) {
			throw new ConfigurationException(e);
		} finally {
			try {
				this.connection.setAutoCommit(true);
			} catch (SQLException e) {
			}
		}
	}


	@Override
	public void rollback() throws ConfigurationException {
		try {
			this.connection.rollback();
		} catch (SQLException e) {
			throw new ConfigurationException(e);
		} finally {
			try {
				this.connection.setAutoCommit(true);
			} catch (SQLException e) {
			}
		}
	}

}
