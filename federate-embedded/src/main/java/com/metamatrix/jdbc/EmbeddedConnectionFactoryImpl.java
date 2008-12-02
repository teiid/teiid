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

package com.metamatrix.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.application.ClientConnectionListener;
import com.metamatrix.dqp.config.DQPConfigSource;
import com.metamatrix.dqp.embedded.DQPListener;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.jdbc.transport.LocalTransportHandler;


/** 
 * A factory class which creates the connections to the embedded DQP instance.
 * This is also responsible for initializing the DQP if the DQP instance is not
 * already alive.
 */
public class EmbeddedConnectionFactoryImpl implements EmbeddedConnectionFactory {
    private static final int ACTIVE = 3;
    private boolean initialized = false;
    private LocalTransportHandler handler = null;    
    private boolean shutdownInProgress = false;
    
    // List of Connection Listeners for the DQP
    private ArrayList connectionListeners = new ArrayList(); 
        
    private EmbeddedConnectionListener listener = new EmbeddedConnectionListener();    

    /**
     * Factory Constructor 
     */
    static EmbeddedConnectionFactoryImpl newInstance() {
        return new EmbeddedConnectionFactoryImpl();        
    }
    
    /** 
     * @see com.metamatrix.jdbc.EmbeddedConnectionFactory#createConnection()
     */
    public Connection createConnection(Properties props) throws SQLException {
		Injector injector = Guice.createInjector(new EmbeddedGuiceModule());
		ResourceFinder.setInjector(injector); 
    	    	
        // Initialize the transport
        initialize(props);

        try {            
            // create a server connection. If the VDB_VERSION used as "UseLatest" the client
            // connection is based on String "UseLatest"
        	this.handler.initManager(props);
            // check for the valid connection properties
            checkConnectionProperties (props);

            ServerConnection serverConn = this.handler.createConnection(props);
                        
            // Should occur every time in classloader using existing attributes
            return EmbeddedConnection.newInstance(this.handler.getManager(), serverConn, props, listener);            
        } catch (ConnectionException e) {
            throw new EmbeddedSQLException(e);
        } catch (CommunicationException e) {
            throw new EmbeddedSQLException(e);            
        }
    }
        
    /**
     * When the DQP is restarted using the admin API, it only shuts it down, it gets
     * restarted when the next time connection is made, however this factory may be
     * holding on to a previous transport handler, so we need to check if the DQP is 
     * still alive and create a new one if necessary. 
     * @param props
     * @throws SQLException
     * @since 4.3
     */
    private void initialize(Properties props) throws SQLException {
        if (!initialized || !this.handler.isAlive()) {
            
            // This monitors the life cycle events for the DQP
            DQPListener dqpListener = new DQPListener() {
                public void onStart() {
                }
                public void onShutdown() {
                    try {
                        shutdown();
                    }catch (SQLException e) {
                        DriverManager.println(e.getMessage());
                    }
                }                
            };
            
            // This monitors the lifecyle events for the connections inside a DQP
            // these are DQP side connections.
            ClientConnectionListener connectionListener = new ClientConnectionListener() {
                public void connectionAdded(ServerConnection connection) {
                }
                public void connectionRemoved(ServerConnection connection) {
                	listener.connectionTerminated(connection.getLogonResult().getSessionID().toString());
                }
            };
            
            //in new classloader - all of these should be created lazily and held locally
            this.handler = new LocalTransportHandler(dqpListener, connectionListener); 
            this.initialized = true;            
        }
    }
        
    public void registerConnectionListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }
    
    /**  
     * A shutdown could happen when somebody calls stop/restart on DQP
     * or in normal course of process, we may have just closed the last connection
     * in both cases we want to handle the situation of graceful/proper shutdown.
     * 
     * @see com.metamatrix.jdbc.EmbeddedConnectionFactory#shutdown()
     */
    public void shutdown() throws SQLException{
        // Make sure shutdown is not already in progress; as call to shutdown will close
        // connections; and after the last connection closes, the listener also calls shutdown
        // for normal route.
        if (!shutdownInProgress) {

            // this will by pass, and only let shutdown called once.
            shutdownInProgress = true;
            
            // First close any connections hanging around; this will only happen if 
            // connections are not properly closed; or somebody called shutdown.
            listener.closeConnections();
                    
            // then close the dqp handler it self, which root for the factory.
            this.handler.shutdown();
        }
    }
     
    /**
     * Are the connection properties supplied for connection match with those of the
     * DQP   
     * @param props
     * @return
     * @since 4.3
     */
    private void checkConnectionProperties(Properties props) throws SQLException {
        String vdbName = props.getProperty(BaseDataSource.VDB_NAME);
        String vdbVersion = props.getProperty(BaseDataSource.VDB_VERSION, EmbeddedDataSource.USE_LATEST_VDB_VERSION);
                        
        try {
            DQPConfigSource configuration = handler.getManager().getDQPConfig();
            VDBService service = (VDBService)configuration.getService(DQPServiceNames.VDB_SERVICE);
            List<VDBArchive> vdbs = service.getAvailableVDBs();

            // We are looking for the latest version find that now 
            if (vdbVersion.equals(EmbeddedDataSource.USE_LATEST_VDB_VERSION)) {
                vdbVersion = findLatestVersion(vdbName, vdbs);
            }

            props.setProperty(BaseDataSource.VDB_VERSION, vdbVersion);
            
            // This below call will load the VDB from configuration into VDB service 
            // if not already done so.
            int status = service.getVDBStatus(vdbName, vdbVersion);
            if (status != ACTIVE) {
                throw new EmbeddedSQLException(JDBCPlugin.Util.getString("EmbeddedConnectionFactory.vdb_notactive", new Object[] {vdbName, vdbVersion})); //$NON-NLS-1$
            }
        } catch (MetaMatrixComponentException e) {
            throw new EmbeddedSQLException(e, JDBCPlugin.Util.getString("EmbeddedConnectionFactory.vdb_notavailable", new Object[] {vdbName, vdbVersion})); //$NON-NLS-1$
        } catch (ApplicationInitializationException e) {
            throw new EmbeddedSQLException(e, JDBCPlugin.Util.getString("EmbeddedConnectionFactory.vdb_notavailable", new Object[] {vdbName, vdbVersion})); //$NON-NLS-1$            
        }
    }
        
    /**
     * Find the latest version of the VDB available in the deployment. 
     * @param vdbName
     * @param vdbs
     * @return
     * @throws EmbeddedSQLException
     * @since 4.3
     */
    String findLatestVersion(String vdbName, List<VDBArchive> vdbs) throws EmbeddedSQLException{        
        int latestVersion = 0;
        for (VDBArchive vdb:vdbs) {
            if(vdb.getName().equalsIgnoreCase(vdbName)) {
                // Make sure the VDB Name and version number are the only parts of this vdb key
                latestVersion = Math.max(latestVersion, Integer.parseInt(vdb.getVersion()));
            }
        }
        if(latestVersion != 0) {
            return String.valueOf(latestVersion);
        }
        throw new EmbeddedSQLException(JDBCPlugin.Util.getString("EmbeddedConnectionFactory.vdb_notavailable", new Object[] {vdbName, EmbeddedDataSource.USE_LATEST_VDB_VERSION})); //$NON-NLS-1$        
    }    
    
    /**
     * Notify all the connection listeners that a connection is added 
     * @param connection
     */
    void notifyConnectionAdded(String id, Connection connection) {
        for (Iterator i = connectionListeners.iterator(); i.hasNext();) {
            ConnectionListener listner = (ConnectionListener)i.next();
            listner.connectionAdded(id, connection);
        }
    }
    
    /**
     * Notify all the connection listeners that a connection is added 
     * @param connection
     */
    void notifyConnectionRemoved(String id, Connection connection) {
        for (Iterator i = connectionListeners.iterator(); i.hasNext();) {
            ConnectionListener listner = (ConnectionListener)i.next();
            listner.connectionRemoved(id, connection);
        }
    }


    /**
     * A internal connection listener for the connections; based on this 
     * it manages the DQP instance. These are client side (JDBC) connections
     */
    private class EmbeddedConnectionListener implements ConnectionListener{

        // List of connections created
        HashMap connections = new HashMap();

        public void connectionAdded(String id, Connection connection) {
            // Add the connection to locol count
            connections.put(id, connection);
            
            // then also notify all the listeners
            notifyConnectionAdded(id, connection);
        }
    
        public void connectionRemoved(String id, Connection connection) {
            // remove from local count 
            connections.remove(id);
            
            // also notify all the listeners
            notifyConnectionRemoved(id, connection);            
        }
        
        /**
         * Loop through all the connections and close the connections. 
         * @throws EmbeddedSQLException
         */
        private void closeConnections() throws SQLException {
            Exception firstException = null;
            
            // loop all the available connections and close them; make sure we avoid the
            // concurrent modification of the list of connections.
            while (this.connections.size() != 0) {
                try {
                    Iterator i = this.connections.keySet().iterator();
                    if (i.hasNext()) {
                        Connection connection = (Connection)this.connections.get(i.next());
                        connection.close();
                    }
                } catch (Exception ex) {
                    if (firstException == null) {
                        firstException = ex;
                    }
                }                
            }
                        
            // if there was any exception then let them know.
            if (firstException != null) {
                throw new EmbeddedSQLException(firstException);
            }
        }      
        
        /**
         * A hook which notifies the client connections that a server connection
         * has been terminated 
         * @param connection
         */
        private void connectionTerminated(String id) {
            // remove from local count 
            Connection connection = (Connection)connections.remove(id);

            notifyConnectionRemoved(id, connection);
        }      
    }
}
