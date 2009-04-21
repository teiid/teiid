package com.metamatrix.common.jdbc;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
//## JDBC4.0-begin ##
import java.sql.SQLFeatureNotSupportedException;
//## JDBC4.0-end ##

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.util.PropertiesUtils;

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
public class SimplePooledConnectionSource 
	//## JDBC4.0-begin ##
	implements DataSource 
	//## JDBC4.0-end ##
{
	//## JDBC4.0-begin ##
    public static final String WAIT_TIME_FOR_RESOURCE= "pooling.resource.pool.wait.time"; //$NON-NLS-1$
    public static final String MAXIMUM_RESOURCE_POOL_SIZE = "pooling.resource.pool.maximum.size"; //$NON-NLS-1$
    public static final String RESOURCE_TEST_INTERVAL = "pooling.resource.pool.test.interval"; //$NON-NLS-1$
    
	private final class ConnectionProxy implements InvocationHandler {
		private Connection c;
		private long lastTest = System.currentTimeMillis();
		private Boolean valid = Boolean.TRUE;
		
		public ConnectionProxy(Connection c) {
			this.c = c;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			try {
				if (method.getName().equals("close")) { //$NON-NLS-1$
					boolean isShutdown = shutdown;
					boolean success = false;
					try {
						if (!isShutdown) {
							if (!c.getAutoCommit()) {
								Logger.getLogger("org.teiid.common.jdbc").warning("Uncommitted connection returned to the pool"); //$NON-NLS-1$ //$NON-NLS-2$
								c.rollback();
								c.setAutoCommit(true);
							}
							if (c.isReadOnly()) {
								c.setReadOnly(false);
							}
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
						return valid;
					} 
					lastTest = now;
					try {
						valid = c.isValid((Integer)args[0]);
					} catch (AbstractMethodError e) {
						valid = !c.isClosed();
					} catch (SQLFeatureNotSupportedException e) {
						valid = !c.isClosed();
					}
					return valid;
				}
				return method.invoke(c, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			}
		}
	}
	
	private Semaphore connectionLock;
	private ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<Connection>();
	private Properties p;
	private int timeout;
	private int testInterval;
	private volatile boolean shutdown;
		
	public SimplePooledConnectionSource(Properties p) {
		this.p = p;
		this.timeout = PropertiesUtils.getIntProperty(p, WAIT_TIME_FOR_RESOURCE, 30000);
		this.testInterval = PropertiesUtils.getIntProperty(p, RESOURCE_TEST_INTERVAL, 30000);
		this.connectionLock = new Semaphore(PropertiesUtils.getIntProperty(p, MAXIMUM_RESOURCE_POOL_SIZE, 15));
	}

	public Connection getConnection() throws SQLException {
		try {
			if (!connectionLock.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
				throw new SQLException("Timeout waiting for connection"); //$NON-NLS-1$
			}
		} catch (InterruptedException e) {
			throw new SQLException(e);
		}
		boolean releaseLock = true;
		try {
			boolean valid = false;
			Connection c = connections.poll();
			if (c != null) {
				try {
					valid  = c.isValid(timeout);
				} catch (SQLException e) {
					
				}
				if (!valid) {
					try {
						c.unwrap(Connection.class).close();
					} catch (SQLException e) {
						
					}
					c = null;
				}
			}
			if (c == null) {
				if (shutdown) {
					throw new SQLException("pool shutdown"); //$NON-NLS-1$
				}
				try {
					c = createConnection();
				} catch (MetaMatrixException e) {
					throw new SQLException(e);
				}
				c = (Connection) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {Connection.class}, new ConnectionProxy(c));
			}
			try {
				c.setAutoCommit(true);
			} catch (SQLException e) {
				//ignore
			}
			releaseLock = false;
			return c;
		} finally {
			if (releaseLock) {
				connectionLock.release();
			}
		}
	}
	
	protected Connection createConnection() throws MetaMatrixException {
		return JDBCUtil.createJDBCConnection(p);
	}
	
	public void shutDown() {
		this.shutdown = true;
		Connection c = connections.poll();
		while (c != null) {
			try {
				c.unwrap(Connection.class).close();
			} catch (SQLException e) {
				
			}
			c = connections.poll();
		}
	}

	@Override
	public Connection getConnection(String username, String password)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws SQLException {
	}

	@Override
	public void setLoginTimeout(int arg0) throws SQLException {
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##
}


