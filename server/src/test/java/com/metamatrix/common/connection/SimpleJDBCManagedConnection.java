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

package com.metamatrix.common.connection;

import java.sql.Connection;
import java.util.Properties;

import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * A trivial implementation of a ManagedConnection that contains one connection.
 * This is useful for testcases that need a ManagedConnection but
 * don't have access to a running server with resource pools.
 */
public final class SimpleJDBCManagedConnection extends JDBCMgdResourceConnection {

private final Connection connection;
    
    /**
     * Create a new instance of a connection.
     * @param env the environment properties for the new connection.
     */
    public SimpleJDBCManagedConnection(Connection connection) {
        super(new Properties(), "fakeUser"); //$NON-NLS-1$
        
        this.connection = connection;
    }
    
    

    // ----------------------------------------------------------------------------------------
    //                 I N I T I A L I Z A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be established.  <i>This method is implemented as a no-op.</i>
     * @throws ManagedConnectionException if there is an error establishing the connection.
     */
    protected void openConnection() { /*do nothing*/ }

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be terminated.  <i>This method is implemented as a no-op.</i>
     * @throws ManagedConnectionException if there is an error terminating the connection.
     */
    protected void closeConnection() {
        try {
            connection.close();
        } catch (Exception e) {            
        }
    }

    /**
     * Create the JDBC connection that is being managed.
     * @return the JDBC connection object.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Prepare this connection for read-only transactions.
     * <i>This method is implemented as a no-op.</i>
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void prepareForRead() { /*do nothing*/ }

    /**
     * Prepare this connection for write transactions.
     * <i>This method is implemented as a no-op.</i>
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void prepareForWrite() { /*do nothing*/ }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void performCommit() {
        try {
            connection.commit();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void performRollback() { 
        try{
            connection.rollback();
        } catch (Exception e) {
        	throw new MetaMatrixRuntimeException(e);
        }

    }


}





