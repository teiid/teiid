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

package com.metamatrix.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.dqp.internal.process.DQPCore;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.jdbc.transport.LocalTransportHandler;


/** 
 * A factory class which creates the connections to the embedded DQP instance.
 * This is also responsible for initializing the DQP if the DQP instance is not
 * already alive.
 */
public class EmbeddedConnectionFactoryImpl implements EmbeddedConnectionFactory {
    private static final int ACTIVE = 3;
    private LocalTransportHandler handler = null;    
    private volatile boolean shutdownInProgress = false;
    private DQPCore dqp;
    private long starttime = -1L;
    private Thread shutdownThread;

    private EmbeddedConnectionListener listener = new EmbeddedConnectionListener();    

    /** 
     * @see com.metamatrix.jdbc.EmbeddedConnectionFactory#createConnection()
     */
    public Connection createConnection(Properties props) throws SQLException {

    	try {
        	// check for the valid connection properties
            checkConnectionProperties (props);

            ServerConnection serverConn = this.handler.createConnection(props);
                        
            // Should occur every time in class loader using existing attributes
            return new EmbeddedConnection(this, serverConn, props, listener);            
        } catch (ConnectionException e) {
            throw new EmbeddedSQLException(e);
		}
    }
        
    /**
     * When the DQP is restarted using the admin API, it only shuts it down, it gets
     * restarted when the next time connection is made, however this factory may be
     * holding on to a previous transport handler, so we need to check if the DQP is 
     * still alive and create a new one if necessary. 
     * @param props
     * @throws ApplicationInitializationException 
     * @since 4.3
     */
    public void initialize(URL bootstrapURL, Properties props) throws SQLException {
		Injector injector = Guice.createInjector(new EmbeddedGuiceModule(bootstrapURL, props));
		ResourceFinder.setInjector(injector); 
		DQPConfigSource configSource = injector.getInstance(DQPConfigSource.class);

		// start the DQP
		this.dqp = new DQPCore();
		try {
			this.dqp.start(configSource);
		} catch (ApplicationInitializationException e) {
			throw new EmbeddedSQLException(e);
		}
		
		// make the configuration service listen for the connection life-cycle events
		// used during VDB delete
        ConfigurationService configService = (ConfigurationService)findService(DQPServiceNames.CONFIGURATION_SERVICE);
		
        //in new class loader - all of these should be created lazily and held locally
        this.handler = new LocalTransportHandler(this.dqp);
    	this.handler.registerListener(configService.getConnectionListener());
    	this.shutdownThread = new ShutdownWork();
    	Runtime.getRuntime().addShutdownHook(this.shutdownThread);
        
        this.starttime = System.currentTimeMillis();
        DQPEmbeddedPlugin.logInfo("DQPEmbeddedManager.start_dqp", new Object[] {new Date(System.currentTimeMillis()).toString()}); //$NON-NLS-1$
    }
    
    class ShutdownWork extends Thread {
    	ShutdownWork(){
    		super("embedded-shudown-thread"); //$NON-NLS-1$
    	}
    	
		@Override
		public void run() {
			try {
				shutdown(false);
			} catch (SQLException e) {
				// ignore
			}
		}
    }
   
    public synchronized boolean isAlive() {
        return (dqp != null);
    }
    
    public long getStartTime() {
    	return this.starttime;
    }
    
    public Properties getProperties() {
        if (isAlive()) {
            return ((ConfigurationService)findService(DQPServiceNames.CONFIGURATION_SERVICE)).getSystemProperties();
        }
        return null;
    }
        
    public synchronized DQPCore getDQP() {
        if (!isAlive()) {
            throw new MetaMatrixRuntimeException(JDBCPlugin.Util.getString("LocalTransportHandler.Transport_shutdown")); //$NON-NLS-1$
        }
        return this.dqp;
    }    
        
    public ApplicationService findService(String type) {
    	return getDQP().getEnvironment().findService(type);
    }
    
    /**  
     * A shutdown could happen when somebody calls stop/restart on DQP
     * or in normal course of process, we may have just closed the last connection
     * in both cases we want to handle the situation of graceful/proper shutdown.
     * 
     * @see com.metamatrix.jdbc.EmbeddedConnectionFactory#shutdown()
     */
    public void shutdown() throws SQLException {
    	shutdown(true);
    }
     
    private synchronized void shutdown(boolean undoShutdownHook) throws SQLException {
    	
    	if (undoShutdownHook) {
    		Runtime.getRuntime().removeShutdownHook(this.shutdownThread);
    	}
    	
        // Make sure shutdown is not already in progress; as call to shutdown will close
        // connections; and after the last connection closes, the listener also calls shutdown
        // for normal route.
        if (!this.shutdownInProgress) {

            // this will by pass, and only let shutdown called once.
            shutdownInProgress = true;
            
            // First close any connections hanging around; this will only happen if 
            // connections are not properly closed; or somebody called shutdown.
            listener.closeConnections();
                    
        	try {
				this.dqp.stop();
			} catch (ApplicationLifecycleException e) {
				LogManager.logWarning(LogConstants.CTX_DQP, e, e.getMessage());
			}
            
            this.dqp = null;
            
            this.handler = null;

            // shutdown the cache.
            ResourceFinder.getCacheFactory().destroy();
            
            shutdownInProgress = false;
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
            VDBService service = (VDBService)findService(DQPServiceNames.VDB_SERVICE);
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
     * A internal connection listener for the connections; based on this 
     * it manages the DQP instance. These are client side (JDBC) connections
     */
    private class EmbeddedConnectionListener implements ConnectionListener{

        // List of connections created
        HashMap connections = new HashMap();

        public void connectionAdded(String id, Connection connection) {
            // Add the connection to locol count
            connections.put(id, connection);
        }
    
        public void connectionRemoved(String id, Connection connection) {
            // remove from local count 
            connections.remove(id);
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
    }
}
