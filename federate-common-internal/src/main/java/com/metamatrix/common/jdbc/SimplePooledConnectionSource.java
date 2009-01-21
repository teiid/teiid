package com.metamatrix.common.jdbc;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.util.PropertiesUtils;

/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008-2009 Red Hat, Inc.
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

/**
 * Simple pooling built on JDBCUtil.  Not compatible with DataSources, etc.
 * Added due to poor 1.6 support among common connection pool implementations.
 * 
 * TODO: Should be switched to proxool or some implementation
 */
public class SimplePooledConnectionSource implements DataSource {
	
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
				if (method.getName().equals("close")) {
					boolean isShutdown = shutdown;
					if (!isShutdown) {
						connections.add((Connection)proxy);
					}
					connectionLock.release();
					if (!isShutdown) {
						return null;
					}
				} else if (method.getName().equals("isValid")) {
					long now = System.currentTimeMillis();
					if (lastTest + testInterval > now) {
						return valid;
					} 
					lastTest = now;
					valid = (Boolean)method.invoke(c, args);
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
					throw new SQLException("pool shutdown");
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

}
