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

package com.metamatrix.common.pooling.jdbc;


import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.impl.BaseResource;

public class JDBCConnectionResource extends BaseResource implements Connection {
	
	public static final String SQL_TRACING_PROPERTY = "sqlTracing"; //$NON-NLS-1$
    
    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String DRIVER = "metamatrix.common.pooling.jdbc.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol that is to be used.  For JDBC, the connection
     * URL information is created of the form "jdbc:subprotocol:subname", where the value
     * of the PROTOCOL property is used for the "subprotocol:subname" portion.
     * This property is required.
     */
    public static final String PROTOCOL = "metamatrix.common.pooling.jdbc.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the database name.  This may include the server name and port number,
     * per the driver's requirements.
     * This property is required.
     */
        
    public static final String DATABASE = "metamatrix.common.pooling.jdbc.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String USERNAME = "metamatrix.common.pooling.jdbc.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String PASSWORD = "metamatrix.common.pooling.jdbc.Password"; //$NON-NLS-1$
    
    /**
     * The environment property name that indicates what the autocommit on the connection should be set to.
     * Defaults to <code>false</code>
     */
    public static final String AUTOCOMMIT = "metamatrix.common.pooling.jdbc.autocommit"; //$NON-NLS-1$
    
    
    private JDBCPlatform platform;
        
    private Long resource_ID;
    

    private Connection conn;

    public JDBCConnectionResource(Connection conn, JDBCPlatform jdbcPlatform, Long id) {
        super();
        this.conn=conn;
        this.platform = jdbcPlatform;
        this.resource_ID = id;
    }
        
    public JDBCPlatform getPlatform() {
        return this.platform;
    }
    
    public Long getResourceID() {
        return this.resource_ID;
    }

    public boolean validate() {
		try {
			conn.getMetaData();
		} catch (Exception e) {
			return false;
		}
		return true;
	}

    public void close() throws SQLException {      
        
      try {
        this.closeResource();
      } catch (ResourcePoolException rpe) {
          throw new SQLException(rpe.getFullMessage());
      }
    }
    
    protected Connection getConnection() {
        return conn;
    }

    protected void performInit() throws ResourcePoolException {

    }

    protected boolean checkIsResourceAlive() {

        boolean result = true;
        try {
	        if (platform.isClosed(conn)) {
    	        return false;
        	} 
        } catch (Throwable e) {
        	return false;	
        }
        

        return result;

    }

	public void clearWarnings() throws SQLException {
		conn.clearWarnings();
	}

	public void commit() throws SQLException {
		conn.commit();
	}

	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		return conn.createArrayOf(arg0, arg1);
	}

	public Blob createBlob() throws SQLException {
		return conn.createBlob();
	}

	public Clob createClob() throws SQLException {
		return conn.createClob();
	}

	public NClob createNClob() throws SQLException {
		return conn.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return conn.createSQLXML();
	}

	public Statement createStatement() throws SQLException {
		return conn.createStatement();
	}

	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		return conn.createStatement(arg0, arg1, arg2);
	}

	public Statement createStatement(int arg0, int arg1) throws SQLException {
		return conn.createStatement(arg0, arg1);
	}

	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		return conn.createStruct(arg0, arg1);
	}

	public boolean getAutoCommit() throws SQLException {
		return conn.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return conn.getCatalog();
	}

	public Properties getClientInfo() throws SQLException {
		return conn.getClientInfo();
	}

	public String getClientInfo(String arg0) throws SQLException {
		return conn.getClientInfo(arg0);
	}

	public int getHoldability() throws SQLException {
		return conn.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return conn.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return conn.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return conn.getWarnings();
	}

	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	public boolean isValid(int arg0) throws SQLException {
		return conn.isValid(arg0);
	}

	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return conn.isWrapperFor(arg0);
	}

	public String nativeSQL(String arg0) throws SQLException {
		return conn.nativeSQL(arg0);
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		return conn.prepareCall(arg0, arg1, arg2, arg3);
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		return conn.prepareCall(arg0, arg1, arg2);
	}

	public CallableStatement prepareCall(String arg0) throws SQLException {
		return conn.prepareCall(arg0);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		return conn.prepareStatement(arg0, arg1, arg2, arg3);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		return conn.prepareStatement(arg0, arg1, arg2);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException {
		return conn.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		return conn.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException {
		return conn.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0) throws SQLException {
		return conn.prepareStatement(arg0);
	}

	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		conn.releaseSavepoint(arg0);
	}

	public void rollback() throws SQLException {
		conn.rollback();
	}

	public void rollback(Savepoint arg0) throws SQLException {
		conn.rollback(arg0);
	}

	public void setAutoCommit(boolean arg0) throws SQLException {
		conn.setAutoCommit(arg0);
	}

	public void setCatalog(String arg0) throws SQLException {
		conn.setCatalog(arg0);
	}

	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		conn.setClientInfo(arg0);
	}

	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		conn.setClientInfo(arg0, arg1);
	}

	public void setHoldability(int arg0) throws SQLException {
		conn.setHoldability(arg0);
	}

	public void setReadOnly(boolean arg0) throws SQLException {
		conn.setReadOnly(arg0);
	}

	public Savepoint setSavepoint() throws SQLException {
		return conn.setSavepoint();
	}

	public Savepoint setSavepoint(String arg0) throws SQLException {
		return conn.setSavepoint(arg0);
	}

	public void setTransactionIsolation(int arg0) throws SQLException {
		conn.setTransactionIsolation(arg0);
	}

	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		conn.setTypeMap(arg0);
	}

	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return conn.unwrap(arg0);
	}

}

