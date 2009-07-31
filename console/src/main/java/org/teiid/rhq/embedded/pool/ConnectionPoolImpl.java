package org.teiid.rhq.embedded.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionFactory;
import org.teiid.rhq.comm.ConnectionPool;


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

/**
 * Simple pooling built on JDBCUtil.  Not compatible with DataSources, etc.
 * Added due to poor 1.6 support among common connection pool implementations.
 * 
 * TODO: Should be switched to proxool or some implementation
 */
public class ConnectionPoolImpl implements ConnectionPool

{

    public static final String WAIT_TIME_FOR_RESOURCE= "jbedsp.pool.wait.time"; //$NON-NLS-1$
    public static final String MAXIMUM_RESOURCE_POOL_SIZE = "jbedsp.pool.maximum.size"; //$NON-NLS-1$
    public static final String RESOURCE_TEST_INTERVAL = "jbedsp.pool.test.interval"; //$NON-NLS-1$
    
	private final class ConnectionProxy implements InvocationHandler {
		private Connection c;
		private long lastTest = System.currentTimeMillis();
		private Boolean valid = Boolean.TRUE;
		
		public ConnectionProxy(Connection c) {
			this.c = c;
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			try {
				if (method.getName().equals("close")) { //$NON-NLS-1$
					boolean isShutdown = shutdown;
					boolean success = false;
					try {
						if (!isShutdown) {
							connections.add((Connection)proxy);
							success = true;
						}
					} finally {
						connectionLock.release();
						if (!success) {
							c.close();
							return null;
						}
					}
					if (success) {
						return null;
					}
				} else if (method.getName().equals("isValid")) { //$NON-NLS-1$
					long now = System.currentTimeMillis();
					if (lastTest + testInterval > now) {
						return c.isValid();
					} 
					lastTest = now;
					try {
						valid = c.isAlive();						
					} catch (AbstractMethodError e) {
						valid = c.isValid();
					} 
					return valid;
				}
				return method.invoke(c, args);
			} catch (InvocationTargetException e) {
				valid = false;
				throw e.getCause();
			}
		}
	}
	
   /**
     * The default connection factory if one is not specified in the environment
     */
     static final String CONNECTION_FACTORY_DEFAULT=ConnectionFactory.CONNECTION_FACTORY_DEFAULT;  //$NON-NLS-1$
	
	
	private Semaphore connectionLock;
	private ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<Connection>();
	private Properties p;
	private int timeout;
	private int testInterval;
	private volatile boolean shutdown;
    private ConnectionFactory factory = null;
    
    private ClassLoader loader = null;
			
	public void close(Connection connection) {

		
	}

	

	@Override
	public int getAvailableConnectionCount() {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	public int getConnectionsInUseCount() {
		// TODO Auto-generated method stub
		return 0;
	}



	public ClassLoader getClassLoader() {
		return this.loader;
	}


	public String getKey() {
		return EmbeddedConnectionConstants.SYSTEM_KEY;
	}



	public void initialize(Properties env, ClassLoader cl) throws ConnectionException {
		this.p = env;
		this.loader = cl;
		this.timeout = getIntProperty(p, WAIT_TIME_FOR_RESOURCE, 30000);
		this.testInterval = getIntProperty(p, RESOURCE_TEST_INTERVAL, 30000);
		this.connectionLock = new Semaphore(getIntProperty(p, MAXIMUM_RESOURCE_POOL_SIZE, 15));
		
//        liveAndUnusedTime = getIntProperty(LIVE_AND_UNUSED_TIME, DEFAULT_LIVE_AND_UNUSED_TIME);
//        cleaningInterval = getIntProperty(CLEANING_INTERVAL, DEFAULT_CLEANING_INTERVAL);

		
        createFactory();
	}



	public Connection getConnection() throws ConnectionException {
		if (shutdown) {
			throw new ConnectionException("pool shutdown"); //$NON-NLS-1$
		}
		try {
			if (!connectionLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				throw new ConnectionException("Timeout waiting for connection"); //$NON-NLS-1$
			}
		} catch (InterruptedException e) {
			throw new ConnectionException(e);
		}
		boolean releaseLock = true;
		try {
			boolean valid = false;
			Connection c = connections.poll();
			if (c != null) {

				valid  = c.isValid();
				if (!valid) {
					try {
						factory.closeConnection(c);
					} catch (Exception e) {
						
					}
					c = null;
				}
			}
			if (c == null) {
				if (shutdown) {
					throw new ConnectionException("pool shutdown"); //$NON-NLS-1$
				}
				c = factory.createConnection();
				c = (Connection) Proxy.newProxyInstance(this.loader, new Class[] {Connection.class}, new ConnectionProxy(c));
				connections.add(c);
			}

			releaseLock = false;
			return c;
		} catch (ConnectionException ce) {
			throw ce;
		} finally {
			if (releaseLock) {
				connectionLock.release();
			}
		}
	}
	
	public void shutdown() {
		this.shutdown = true;
		Connection c = connections.poll();
		while (c != null) {
			try {
				c.close();
			} catch (Exception e) {
				
			}
			c = connections.poll();
		}		
		
	}


    private void createFactory() throws ConnectionException {
        
        String factoryclass = p.getProperty(ConnectionPool.CONNECTION_FACTORY, CONNECTION_FACTORY_DEFAULT);
  
            try {
            	Class<?> c = Class.forName(factoryclass, true, this.loader);
                factory = (ConnectionFactory)c.newInstance();
            } catch (Exception err) {
                throw new ConnectionException(err.getMessage());
            }
            
             // Initialize connector instance...
            factory.initialize(p, this);
      
   }
    
    public static int getIntProperty(Properties props, String propName, int defaultValue) throws ConnectionException {
        int val = defaultValue;
        String stringVal = props.getProperty(propName);
        if(stringVal != null && stringVal.trim().length() > 0) {
        	try {
        		val = Integer.parseInt(stringVal);
        	} catch (NumberFormatException nfe) {
        		throw new ConnectionException(nfe.getMessage());
        	}
        }
        return val;
    }
    

    
}


