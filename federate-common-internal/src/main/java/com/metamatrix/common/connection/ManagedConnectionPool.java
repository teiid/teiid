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

package com.metamatrix.common.connection;

import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.ReflectionHelper;

public class ManagedConnectionPool {

    /**
     * The environment property name for the class that is to be used for the ManagedConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String FACTORY = "com.metamatrix.common.connection.ManagedConnectionPool.Factory"; //$NON-NLS-1$

    public static final String MAXIMUM_AGE = "com.metamatrix.common.connection.ManagedConnectionPool.MaximumAge"; //$NON-NLS-1$
    public static final String DEFAULT_MAXIMUM_AGE = "600000";  // in milliseconds, or 10 minutes //$NON-NLS-1$

    public static final String MAXIMUM_CONCURRENT_USERS = "com.metamatrix.common.connection.ManagedConnectionPool.MaximumConcurrentUsers"; //$NON-NLS-1$
    public static final String DEFAULT_MAXIMUM_CONCURRENT_USERS = "1"; // Default 1 because Oracle has problems sharing. :) //$NON-NLS-1$

    public static final String MINIMUM_CONNECTIONS = "com.metamatrix.common.connection.ManagedConnectionPool.MinConnections"; //$NON-NLS-1$
    public static final String DEFAULT_MINIMUM_CONNECTIONS = "1"; //$NON-NLS-1$
    /**
     *@link dependency
     * @stereotype use
     */

    // since this pooling will be replaced in 3.0, it was determined to set the
    // cleanup time to 20 mins because in a production environment there should
    // be no need to cleanup so often.
    private static final long CLEANUP_TIME = 1200000; // 20 minutes

    private Properties environment;
    private Properties connectionProperties;
    private long maximumAge;
    private long minimumConnections;
    private int maximumConcurrentUsers;
    private ManagedConnectionFactory connectionFactory;

    private Set  writeLockedConnections      = Collections.synchronizedSet( new HashSet() );
    private Set  connectionsAvailableForRead = Collections.synchronizedSet( new HashSet() );
    private Set  unusedConnections           = Collections.synchronizedSet( new HashSet() );

    private CleanUpThread cleanerThread;
    private boolean shutdownRequested = false;

    protected void finalize() {
        this.shutdownPool();
    }

    public ManagedConnectionPool( Properties env, Properties connectionProperties ) throws ManagedConnectionException {
        if ( env == null ) {
            this.environment = new Properties();
        } else {
            synchronized(env) {
                this.environment = (Properties) env.clone();
            }
        }
        if ( !(this.environment instanceof UnmodifiableProperties) ) {
            this.environment = new UnmodifiableProperties(this.environment);
        }

        // Create the proper factory instance
        String connectionFactoryClassName = this.environment.getProperty(ManagedConnectionPool.FACTORY);
        try {
        	this.connectionFactory = (ManagedConnectionFactory) ReflectionHelper.create(connectionFactoryClassName, null, getClass().getClassLoader());

        } catch(Exception e) {
            throw new ManagedConnectionException(e);
        }

        // Check for the required parameters ...
        String maximumAgeValue = this.environment.getProperty(MAXIMUM_AGE);
        if ( maximumAgeValue == null ) {
            maximumAgeValue = DEFAULT_MAXIMUM_AGE;
        }
        try {
            this.maximumAge = Long.parseLong(maximumAgeValue);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0007,
            		new Object[] {maximumAgeValue, MAXIMUM_AGE}  ));
        }

        // Check for the required parameters ...
        String minConnValue = this.environment.getProperty(MINIMUM_CONNECTIONS);
        if ( minConnValue == null ) {
            minConnValue = DEFAULT_MINIMUM_CONNECTIONS;
        }
        try {
            this.minimumConnections = Long.parseLong(minConnValue);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0007,
            		new Object[] {minConnValue, MINIMUM_CONNECTIONS}  ));
        }


        String maximumConcurrentUsersValue = this.environment.getProperty(MAXIMUM_CONCURRENT_USERS);
        if ( maximumConcurrentUsersValue == null ) {
            maximumConcurrentUsersValue = DEFAULT_MAXIMUM_CONCURRENT_USERS;
        }
        try {
            this.maximumConcurrentUsers = Integer.parseInt(maximumConcurrentUsersValue);
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0007,
            		new Object[] {maximumConcurrentUsersValue, MAXIMUM_CONCURRENT_USERS}  ));
        }

        // Create the properties for the connections ...
        if ( connectionProperties == null ) {
            connectionProperties = new Properties();
        }
        this.connectionProperties = (Properties) connectionProperties.clone();
        if ( !(this.connectionProperties instanceof UnmodifiableProperties) ) {
            this.connectionProperties = new UnmodifiableProperties(this.connectionProperties);
        }

        // Create the cleaner thread
        cleanerThread = new CleanUpThread( this, CLEANUP_TIME );
        cleanerThread.start();
    }

    public TransactionInterface getReadTransaction() throws ManagedConnectionException {
        // Get a connection for the read (this may fail) ...
        ManagedConnection connection = this.checkOutForRead();

        // Upon success, create transaction instance ...
        return this.connectionFactory.createTransaction(this,connection,true);
    }

    public TransactionInterface getWriteTransaction() throws ManagedConnectionException {
        // Get a connection for the read (this may fail) ...
        ManagedConnection connection = this.checkOutForWrite();

        // Upon success, create transaction instance ...
        return this.connectionFactory.createTransaction(this,connection,false);
    }

    public synchronized void shutdownPool() {
        this.shutdownRequested = true;
        this.cleanerThread.stopCleanup();
        this.cleanUp();
    }

    public void cleanUp() {
        Iterator iter = null;

//        LogManager.logTrace("MCP", "Starting managed connection cleanup");

        // Lock the unused connections (always in the same order to prevent deadlock!)
        List closeConns=null;

        // track the minimum connection to keep while closing the others

        if (this.shutdownRequested) {

            synchronized( this.unusedConnections ) {
                closeConns = new ArrayList(this.unusedConnections.size());

                ManagedConnection connection = null;

                iter = this.unusedConnections.iterator();
                while ( iter.hasNext() ) {
                    connection = (ManagedConnection) iter.next();
                    iter.remove();
                    closeConns.add(connection);
                }

            }

        } else {

          // no need to close connections or synch when there are not enough
          // connections to close
          if (this.unusedConnections.size() <= minimumConnections) {

          } else {


            synchronized( this.unusedConnections ) {
                long maxTimeAllowed = System.currentTimeMillis() - this.maximumAge;

    //            System.out.println("Start ManagedConnection Cleanup");

                // if there are more connections than the minimum, then close

                    closeConns = new ArrayList(this.unusedConnections.size());

                    ManagedConnection connection = null;
                    boolean gotMin = false;

                    iter = this.unusedConnections.iterator();
                    while ( iter.hasNext() ) {
                    // the process keeps the first connection as the minimum connection
                        connection = (ManagedConnection) iter.next();
                        if (!gotMin) {
                            // keep the first one as the minimum connection
                            gotMin = true;
                        } else {

                           ManagedConnection.ConnectionStatistics stats = connection.getStats();
                            if ( stats.getLastUsed() < maxTimeAllowed ) {
                                iter.remove();
                                closeConns.add(connection);
                            }
                        }

                    } // end of while

                }  // end of synch
            } // end of having min connections already
        }  // end of not being shutdown

        ManagedConnection conn = null;
        if (closeConns != null && closeConns.size() > 0) {

//            System.out.println("Close Connections");
            for (Iterator it=closeConns.iterator(); it.hasNext(); ) {
                conn = (ManagedConnection) it.next();

                try {
                    conn.close();
                } catch ( ManagedConnectionException e ) {
                    LogManager.logError(LogCommonConstants.CTX_POOLING, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0008, conn.getEnvironment().getProperty(ManagedConnection.PROTOCOL, "NoProtocol"))); //$NON-NLS-1$
                }
            }

        }

    }

    ManagedConnection checkOutForRead() throws ManagedConnectionException {
        if ( this.shutdownRequested ) {
            throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0009, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0009));
        }
        ManagedConnection connection = null;
        ManagedConnection.ConnectionStatistics stats = null;

        // Lock the read connection pool and the availability list (always in the same order to prevent deadlock!)
        synchronized( this.connectionsAvailableForRead ) {

            // If there is a read-locked connection available ...
            Iterator iter = this.connectionsAvailableForRead.iterator();
            if ( iter.hasNext() ) {
                connection = (ManagedConnection) iter.next();
                stats = connection.getStats();
                stats.addConcurrentUser();

                // If the connection can't handle any more concurrent users, remove it from the availability list
                if ( stats.getConcurrentUserCount() >= this.maximumConcurrentUsers ) {
                    this.connectionsAvailableForRead.remove(connection);
                }

                return connection;
            }

            // There was not one available, so grab an unused one ...
            // Lock the unused connections (always in the same order to prevent deadlock!)
            synchronized( this.unusedConnections ) {

                // Iterate over the unused connections until one is found that isn't too old ...
//                long maxTimeAllowed = System.currentTimeMillis() + this.maximumAge;
                Iterator unusedIter = this.unusedConnections.iterator();
                while ( unusedIter.hasNext() ) {
                    connection = (ManagedConnection) unusedIter.next();
                    // grab the first available connection
                        unusedIter.remove();
                        break;

/*
      //*** LET the cleanup thread clean the connections
                    stats = connection.getStats();
                    if ( stats.getLastUsed() > maxTimeAllowed ) {
                        try {
                            connection.close();
                        } catch ( Exception e ) {
                              LogManager.logError("MCP", e, "Unable to close read connection in checkOutForRead");
                        }
                        unusedIter.remove();
                        connection = null;
                    } else {
                        unusedIter.remove();
                        break;
                    }
*/
                }
            }   // end of inner synchronization

            // If there were no unused connections, create a new one ...
            if ( connection == null ) {
                connection = this.connectionFactory.createConnection(this.connectionProperties);
                connection.open();
//                System.out.println("Create Read Managed Connection");
            }

            connection.getStats().addConcurrentUser();

            // Lock the read connection pool and the availability list (always in the same order to prevent deadlock!)
            if ( this.maximumConcurrentUsers > 1 ) {
                this.connectionsAvailableForRead.add(connection);
            }

        }   // end of outer synchronization

        //just a guarantee that the connection is open; it does not hurt if
        //it already is open
        connection.open();

        connection.setForRead();

        return connection;
    }

    ManagedConnection checkOutForWrite() throws ManagedConnectionException {
        if ( this.shutdownRequested ) {
            throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0009, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0009));
        }

        // Get a new connection from the unused pool ...
        ManagedConnection connection = null;

        // Get an unused connection from the unused pool ...
        // Lock the unused connections (always in the same order to prevent deadlock!)
        synchronized( this.unusedConnections ) {

            // Iterate over the unused connections until one is found that isn't too old ...
//            long maxTimeAllowed = System.currentTimeMillis() + this.maximumAge;
            Iterator iter = this.unusedConnections.iterator();
            while ( iter.hasNext() ) {
                connection = (ManagedConnection) iter.next();
                // grab the first connection
					          iter.remove();
                    break;

/*
    // let the cleaner thread close connections
                ManagedConnection.ConnectionStatistics stats = connection.getStats();
                if ( stats.getLastUsed() > maxTimeAllowed ) {
                    try {
                        connection.close();
                    } catch ( Exception e ) {
                        LogManager.logError("MCP", e, "Unable to close write connection in checkoutforwrite");

                    }

                    iter.remove();
                    connection = null;
                } else {

					          iter.remove();
                    break;
                }
*/

            }
        }

        // If a connection were not found, create one ...
        if ( connection == null ) {
            connection = this.connectionFactory.createConnection(this.connectionProperties);
            connection.open();
//            System.out.println("Create Write Managed Connection");

        }

        // Move the connection to the write-locked pool ...
        synchronized( this.writeLockedConnections ) {
            connection.getStats().addConcurrentUser();
            this.writeLockedConnections.add(connection);
        }

        //just a guarantee that the connection is open; it does not hurt if
        //it already is open
        connection.open();

        connection.setForWrite();
        return connection;
    }

    boolean checkInReadConnection( ManagedConnection connection ) {
        boolean removed = false;
//        System.out.println("**** Checking in Read ManagedConnection ***");

        // Check the read-locked pool ...
        // Lock the read connection pool and the availability list (always in the same order to prevent deadlock!)
        ManagedConnection.ConnectionStatistics stats = null;
        synchronized( this.connectionsAvailableForRead ) {
            stats = connection.getStats();
            // If the caller is the only user of the connection, move it to the unused pool
            stats.removeConcurrentUser();
            if ( ! stats.hasConcurrentUsers() ) {
                this.connectionsAvailableForRead.remove(connection);
                removed = true;
                if ( ! this.shutdownRequested ) {
                    synchronized( this.unusedConnections ) {
//                        System.out.println("**** Add Read ManagedConnection to Unused Pool ***");

                        this.unusedConnections.add( connection );
                    }
                }
            } else {
                // The caller is not the only user, and it still can be used by others in the future
                if ( ! this.shutdownRequested ) {
                    this.connectionsAvailableForRead.add(connection);
                }
            }
        }

        // If found and removed from a locked pool, decide whether to shut the connection down or to just unlock it ...
        if ( removed && this.shutdownRequested ) {
            try {
                connection.close();
            } catch ( ManagedConnectionException e ) {
            }
        }

        return true;
    }

    boolean checkInWriteConnection( ManagedConnection connection ) {
        boolean found = false;
//        System.out.println("**** Checking in Write ManagedConnection ***");

        // Check the write-locked pool ...
        synchronized( this.writeLockedConnections ) {
            found = this.writeLockedConnections.remove(connection);
        }

        // If not found, then the connection doesn't belong to this pool ...
        if ( !found ) {
            return false;
        }

        connection.getStats().removeConcurrentUser();

        // If found and removed from a locked pool, decide whether to shut the connection down or to just unlock it ...
        if ( this.shutdownRequested ) {
            try {
                connection.close();
            } catch ( ManagedConnectionException e ) {
            }
        } else {
            // Lock the unused connections (always in the same order to prevent deadlock!)
            synchronized( this.unusedConnections ) {
//                        System.out.println("**** Add Write ManagedConnection to Unused Pool ***");

                this.unusedConnections.add( connection );
            }
        }

        return true;
    }

}

class CleanUpThread extends Thread
{
    private ManagedConnectionPool pool;
    private long sleepTime;
    private boolean continueChecks = true;

    CleanUpThread( ManagedConnectionPool pool, long sleepTime )
    {
        this.pool = pool;
        this.sleepTime = sleepTime;
    }

    public void stopCleanup() {
        this.continueChecks = false;
    }

    public void run()
    {
        while( this.continueChecks ) {
            try {
                sleep( sleepTime );
            } catch( InterruptedException e ) {
                // ignore it
            }
            if ( pool != null ) {
//System.out.println("Clean-up thread is instructing the pool to clean up");
                pool.cleanUp();
            }
        }
    }
}




