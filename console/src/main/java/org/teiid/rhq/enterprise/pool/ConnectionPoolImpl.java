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
package org.teiid.rhq.enterprise.pool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionFactory;
import org.teiid.rhq.comm.ConnectionPool;




/** 
 *  This is the connection pool used to manage connections to the MetaMatrix server.
 */
public class ConnectionPoolImpl implements ConnectionPool {
    private final Log LOG = LogFactory.getLog(ConnectionPoolImpl.class);
    
    /**
     * The default connection factory if one is not specified in the environment
     */
    private static final String CONNECTION_FACTORY_DEFAULT=ConnectionFactory.CONNECTION_FACTORY_DEFAULT;  //$NON-NLS-1$
    
    public static final String WAIT_TIME_FOR_RESOURCE= "teiid.pool.wait.time"; //$NON-NLS-1$
    public static final String MAXIMUM_RESOURCE_POOL_SIZE = "teiid.pool.maximum.size"; //$NON-NLS-1$
    public static final String RESOURCE_TEST_INTERVAL = "teiid.pool.test.interval"; //$NON-NLS-1$


     private static final int DEFAULT_LIVE_AND_UNUSED_TIME = 60;
     private static final int DEFAULT_CLEANING_INTERVAL = 60;
     private static final boolean DEFAULT_ENABLE_SHRINKING = true; 

     
     private int liveAndUnusedTime = DEFAULT_LIVE_AND_UNUSED_TIME;
     private int cleaningInterval = DEFAULT_CLEANING_INTERVAL;
     private boolean enableShrinking = DEFAULT_ENABLE_SHRINKING;
     
 	 private int timeout;
	 private int testInterval;

     
    
	private Set<ConnectionWrapper>  availConnections          = Collections.synchronizedSet( new HashSet(10) );
    private Set<Connection>  inuseConnections   = Collections.synchronizedSet( new HashSet(10) );
    
	private Semaphore connectionLock;
    
    private ConnectionFactory factory = null;
//    private String installDir = null;
    private Properties envProps = null;
    
    private volatile boolean shuttingDownPool;
    
    private CleanUpThread cleaningThread;   
    
    private ClassLoader clzzloader;
    
    
 //   private Object lock = new Object();
    
    public ConnectionPoolImpl() {
        
        LOG.info("Creating Connection Pool");  //$NON-NLS-1$ 
    }
    

    /** 
     * @see org.teiid.rhq.comm.ConnectionPool#getClassLoader()
     *  
     */
    public ClassLoader getClassLoader() {
        return clzzloader;
    }
    
    


    @Override
	public int getAvailableConnectionCount() {
		return availConnections.size();
	}
    
    


	@Override
	public int getConnectionsInUseCount() {
		return inuseConnections.size();
	}


	public String getKey() {
		// TODO Auto-generated method stub
		return factory.getURL();
	}


	/** 
     * @see com.metamatrix.rhq.admin.pool.ConnectionPool#close(org.teiid.rhq.comm.Connection)
     *  
     */
    public void close(Connection connection) throws ConnectionException {
        if (this.shuttingDownPool) {
             return;
        }        
		try {
			if (!connectionLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				throw new ConnectionException("Timeout waiting for lock"); //$NON-NLS-1$
			}
		} catch (InterruptedException e) {
			throw new ConnectionException(e);
		}
		try {
            inuseConnections.remove(connection);
            if (connection.isValid()) {
                ConnectionWrapper cw = new ConnectionWrapper(connection);
                availConnections.add(cw);
                LOG.debug("Connection checked in for system "); //$NON-NLS-1$
            } else {
               this.closeConnection(connection);  
               LOG.debug("Connection returned and closed for system "); //$NON-NLS-1$                

            }



		} finally {
			connectionLock.release();
		}

    }
      

    /** 
     * @see com.metamatrix.rhq.admin.pool.ConnectionPool#getConnection()
     *  
     */
    public Connection getConnection() throws ConnectionException {
        
        if (this.shuttingDownPool) {
            return null;
        }

		try {
			if (!connectionLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				throw new ConnectionException("Timeout waiting for lock trying to get connection"); //$NON-NLS-1$
			}
		} catch (InterruptedException e) {
			throw new ConnectionException(e);
		}
		
		try {
            if (availConnections.isEmpty()) {
                Connection conn = createConnection();                    
                inuseConnections.add(conn);
                return conn;
            }                
            
        
            for (Iterator it=availConnections.iterator(); it.hasNext();) {
                ConnectionWrapper conn = (ConnectionWrapper) it.next();
                it.remove();
                if (conn.originalConnection.isValid()) {
                    inuseConnections.add(conn.originalConnection);
                    LOG.debug("Existing connection obtained for system "); //$NON-NLS-1$
                    return conn.originalConnection;
                } 
                this.closeConnection(conn.originalConnection);                               
                                       
            }  
            
            Connection conn = createConnection();
            inuseConnections.add(conn);
            return conn;                

		} finally {
			connectionLock.release();
		}
       
    }
    
    /** 
     * @see com.metamatrix.rhq.admin.pool.ConnectionPool#initialize(java.lang.String, java.util.Properties)
     *  
     */
    public void initialize(Properties env, ClassLoader cl) throws ConnectionException {
        this.envProps = env;
 //       this.installDir = env.getProperty(EnterpriseConnectionConstants.INSTALL_DIR);
               
        this.clzzloader = cl; 
 		this.connectionLock = new Semaphore(getIntProperty(MAXIMUM_RESOURCE_POOL_SIZE, 15));
		
		this.timeout = getIntProperty( WAIT_TIME_FOR_RESOURCE, 30000);
		this.testInterval = getIntProperty( RESOURCE_TEST_INTERVAL, 30000);
		this.connectionLock = new Semaphore(getIntProperty( MAXIMUM_RESOURCE_POOL_SIZE, 15));


        initializeProps();        
        createFactory();
    }

    /** 
     * @see com.metamatrix.rhq.admin.pool.ConnectionPool#shutdown()
     *  
     */
    public void shutdown()  throws ConnectionException {
        shuttingDownPool = true;
        
		try {
			if (!connectionLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				throw new ConnectionException("Timeout waiting for lock trying to get connection"); //$NON-NLS-1$
			}
		} catch (InterruptedException e) {
			throw new ConnectionException(e);
		}
        
		try {
	        //close cleaning thread
	        if (this.cleaningThread != null) {
	            this.cleaningThread.stopCleanup();
	            this.cleaningThread.interrupt();
	        }        
	        
	        // cleanup, passing true, will close all available connections
	        this.cleanUp(true);
	        	
	        
	        for (Iterator i = inuseConnections.iterator(); i.hasNext();) {
	            Connection conn = (Connection)i.next();
	            this.closeConnection(conn);
	        }
	        inuseConnections.clear();
	        
	
	        
	        envProps.clear();
	        factory = null;
		} finally {
			connectionLock.release();
		}
        
        
    }
    
    
    private void createFactory() throws ConnectionException {
        
        String factoryclass = envProps.getProperty(ConnectionPool.CONNECTION_FACTORY, CONNECTION_FACTORY_DEFAULT);       
        
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(this.clzzloader);
            try {
                Class c = Class.forName(factoryclass, true, this.clzzloader);
                factory = (ConnectionFactory)c.newInstance();
            } catch (Exception err) {
                throw new ConnectionException(err.getMessage());
            }
            
             // Initialize connector instance...
            factory.initialize(this.envProps, this);

 
        } finally {
            currentThread.setContextClassLoader(threadContextLoader);
        }
        
   }
    
    private void initializeProps() throws ConnectionException {
            liveAndUnusedTime = getIntProperty(LIVE_AND_UNUSED_TIME, DEFAULT_LIVE_AND_UNUSED_TIME);
            cleaningInterval = getIntProperty(CLEANING_INTERVAL, DEFAULT_CLEANING_INTERVAL);

            
            if (!this.shuttingDownPool) {
                this.cleaningThread = new CleanUpThread(cleaningInterval * 1000);
                this.cleaningThread.setDaemon(true);
                this.cleaningThread.start();
            }
            
            String value = envProps.getProperty(ENABLE_SHRINKING);
            if ( value != null ) {
                enableShrinking = Boolean.valueOf(value).booleanValue();
            }            
               
    }    

    private int getIntProperty(String propertyName, int defaultValue) throws ConnectionException {
        String value = this.envProps.getProperty(propertyName );
        if (value == null || value.trim().length() == 0) {
            return defaultValue;
        }
        return Integer.parseInt(value); 
        
    }
        
    
    private Connection createConnection() throws ConnectionException {
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(this.clzzloader);
            
            // Initialize connector instance...
            return factory.createConnection();

 
        } finally {
            currentThread.setContextClassLoader(threadContextLoader);
        }
        
        

    }
    
    private void closeConnection(Connection conn) {
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(this.clzzloader);
            // Initialize connector instance...
            factory.closeConnection(conn);

 
        } finally {
            currentThread.setContextClassLoader(threadContextLoader);
        }
        
    }
    
    protected void cleanUp(boolean forceClose) {        
        Set values = new HashSet(this.availConnections);

            for (Iterator i = values.iterator(); i.hasNext();) {
                ConnectionWrapper conn = (ConnectionWrapper)i.next();
                
                    if (forceClose || (enableShrinking && conn.getIdelTime() >= this.liveAndUnusedTime)
                                    || !conn.originalConnection.isAlive()) {
                       availConnections.remove(conn);
                       this.closeConnection(conn.originalConnection);   
 
                    }
            }

    }    
    
    /**
     * ConnectionWrapper is used to store the connection in the availableConnections and
     * will provide the amount of idletime a connection has been unused so that
     * it can be determined if the pool can be shrunk 
     * 
     */
    class ConnectionWrapper {
        Connection originalConnection;
        private long timeReturnedToPool;

        ConnectionWrapper(Connection originalConn) {
            originalConnection = originalConn;
            timeReturnedToPool = System.currentTimeMillis();
        }

        int getIdelTime() {
            return (int) (System.currentTimeMillis() - timeReturnedToPool) / 1000;
        }
    }        
    
    class CleanUpThread extends Thread {
        private long sleepTime;
        private boolean continueChecks = true;

        CleanUpThread(long sleepTime) {
            super("MMConnectionPoolCleanUpThread");  //$NON-NLS-1$
            this.sleepTime = sleepTime;
        }

        public void stopCleanup() {
            this.continueChecks = false;
        }

        public void run() {
            while ( this.continueChecks ) {
                try {
                    sleep(sleepTime);
                } catch (InterruptedException e) {
                    // ignore it
                }
                ConnectionPoolImpl.this.cleanUp(false);
            }
        }
    }  
        
    

}
