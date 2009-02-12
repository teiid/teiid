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

import java.util.Properties;

import com.metamatrix.common.properties.UnmodifiableProperties;

public abstract class ManagedConnection {
    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String DRIVER = "com.metamatrix.common.connection.ManagedConnection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol that is to be used.  For JDBC, the connection
     * URL information is created of the form "jdbc:subprotocol:subname", where the value
     * of the PROTOCOL property is used for the "subprotocol:subname" portion.
     * This property is required.
     */
    public static final String PROTOCOL = "com.metamatrix.common.connection.ManagedConnection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the database name.  This may include the server name and port number,
     * per the driver's requirements.
     * This property is required.
     */
    public static final String DATABASE = "com.metamatrix.common.connection.ManagedConnection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String USERNAME = "com.metamatrix.common.connection.ManagedConnection.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String PASSWORD = "com.metamatrix.common.connection.ManagedConnection.Password"; //$NON-NLS-1$

    // the name of the user of this connection
    private String userName;
    private Properties environment;
    private boolean isOpen = false;
    private ConnectionStatistics stats;

    private static final String NOT_ASSIGNED = "NoUserNameAssigned"; //$NON-NLS-1$

    protected void finalize() {
        try {
            this.close();
        } catch ( Exception e ) {
        }
    }

    /**
     * Create a new instance of a metadata connection.
     * @param context the metadata context for the connection.
     * @param env the environment properties for the new connection.
     */
    protected ManagedConnection( Properties env ) {
        Properties p = null;
        this.userName= NOT_ASSIGNED;
        if (env != null) {
            synchronized(env) {
                p = (Properties) env.clone();
            }
        } else {
            p = new Properties();
        }
        if ( p instanceof UnmodifiableProperties ) {
            this.environment = p;
        } else {
            this.environment = new UnmodifiableProperties(p);
        }
        this.stats = new ConnectionStatistics();
    }

    /**
     * This is a new method added as part of the ResourcePooling framework.
     * New extended ManagedConnection classes should set the userName
     */
    protected void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the name of the user using this managed connection
     * @return String user name
     */
    public String getUserName() {
        return this.userName;
    }

    /**
     * Obtain the environment properties for this metadata connection.
     * These properties cannot be modified while the metadata connection is in existance.
     * @return the environment for this metadata connection.
     */
    public final Properties getEnvironment() {
        return this.environment;
    }

    // ----------------------------------------------------------------------------------------
    //                 I N I T I A L I Z A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be established.
     * @throws ManagedConnectionException if there is an error establishing the connection.
     */
    public final synchronized void open() throws ManagedConnectionException {
        if ( this.isOpen == false ) {
            this.openConnection();
            this.isOpen = true;
        }
    }

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be terminated.
     * @throws ManagedConnectionException if there is an error terminating the connection.
     */
    public final synchronized void close() throws ManagedConnectionException {
        if ( this.isOpen ) {
            this.closeConnection();
            this.isOpen = false;
        }
    }

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be established.
     * @throws ManagedConnectionException if there is an error establishing the connection.
     */
    protected abstract void openConnection() throws ManagedConnectionException;

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be terminated.
     * @throws ManagedConnectionException if there is an error terminating the connection.
     */
    protected abstract void closeConnection() throws ManagedConnectionException;

    // ----------------------------------------------------------------------------------------
    //             T R A N S A C T I O N   M A N A G E M E N T    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * Prepare this connection for read-only transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    public final void setForRead() throws ManagedConnectionException {
        this.prepareForRead();
    }

    /**
     * Prepare this connection for write transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    public final void setForWrite() throws ManagedConnectionException {
        this.prepareForWrite();
    }

    /**
     * Prepare this connection for read-only transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected abstract void prepareForRead() throws ManagedConnectionException;

    /**
     * Prepare this connection for write transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected abstract void prepareForWrite() throws ManagedConnectionException;

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    public final void commit() throws ManagedConnectionException {
        this.performCommit();
    }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    public final void rollback() throws ManagedConnectionException {
        this.performRollback();
    }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected abstract void performCommit() throws ManagedConnectionException;

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected abstract void performRollback() throws ManagedConnectionException;

    public final ConnectionStatistics getStats() {
        return this.stats;
    }


public static class ConnectionStatistics {
    private int concurrentUsers;
    private long lastUsed;

    public ConnectionStatistics() {
        this.concurrentUsers = 0;
        this.markAsUsed();
    }

    public synchronized int getConcurrentUserCount() {
        return this.concurrentUsers;
    }

    public synchronized boolean hasConcurrentUsers() {
        return this.concurrentUsers > 0;
    }

    public synchronized void addConcurrentUser() {
        ++this.concurrentUsers;
        this.markAsUsed();
    }

    public synchronized void removeConcurrentUser() {
        if ( this.concurrentUsers > 0 ) {
            --this.concurrentUsers;
        }
    }

    public synchronized long getLastUsed() {
        return this.lastUsed;
    }

    public synchronized void markAsUsed() {
        this.lastUsed = System.currentTimeMillis();
    }
}

}
