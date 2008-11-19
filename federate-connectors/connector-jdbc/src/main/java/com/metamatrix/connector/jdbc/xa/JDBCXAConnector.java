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

/*
 */
package com.metamatrix.connector.jdbc.xa;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;

import com.metamatrix.common.xa.TransactionContext;
import com.metamatrix.connector.jdbc.JDBCConnector;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.ConnectionPoolException;
import com.metamatrix.data.pool.DisabledConnectionPool;
import com.metamatrix.data.pool.SourceConnectionFactory;
import com.metamatrix.data.xa.api.XAConnector;


public class JDBCXAConnector extends JDBCConnector implements XAConnector{

    private final class RemovalCallback implements
                                       Synchronization {

        private final TransactionContext transactionContext;
        private final JDBCSourceXAConnection conn;

        /** 
         * @param transactionContext
         */
        private RemovalCallback(TransactionContext transactionContext, JDBCSourceXAConnection conn) {
            this.transactionContext = transactionContext;
            this.conn = conn;
        }

        public void afterCompletion(int arg0) {
            idToConnections.remove(this.transactionContext.getTxnID());
            synchronized (conn) {
                conn.setInTxn(false);
                if (!conn.isLeased()) {
                    conn.release();
                    environment.getLogger().logTrace("released connection for transaction " + transactionContext.getTxnID()); //$NON-NLS-1$
                }
            }
        }

        public void beforeCompletion() {}
    }

    private ConnectionPool xaPool; 
    private Map idToConnections;
    
    public void stop() {        
        if(xaPool != null) {
            xaPool.shutDown();
            xaPool = null;
        }
        super.stop();
    }

    public void start() throws ConnectorException {
        if(xaPool == null){  
            Properties appEnvProps = environment.getProperties();          
            String scfClassName = appEnvProps.getProperty(JDBCPropertyNames.EXT_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory");  //$NON-NLS-1$

            // Get and parse URL for some DataSource properties - add to connectionProps
            final String url = appEnvProps.getProperty(JDBCPropertyNames.URL);
            if ( url == null || url.trim().length() == 0 ) {
                throw new ConnectorException("Missing required property: " + JDBCPropertyNames.URL); //$NON-NLS-1$
            }
            
            parseURL(url, appEnvProps);
            idToConnections = Collections.synchronizedMap(new HashMap());
            
            try {
                //create xa source connection factory
                Class scfClass = Thread.currentThread().getContextClassLoader().loadClass(scfClassName);
                SourceConnectionFactory factory = (SourceConnectionFactory) scfClass.newInstance();           

                appEnvProps.setProperty(XAJDBCPropertyNames.IS_XA, Boolean.TRUE.toString());
                factory.initialize(environment);
                appEnvProps.setProperty(XAJDBCPropertyNames.IS_XA, Boolean.FALSE.toString());

                if(this.connectionPoolEnabled) {
                    // If ConnectionPool is enabled, create DefaultConnectionPool
                	xaPool = new ConnectionPool(factory);
                } else {
                    // If ConnectionPool is disabled, create DisabledConnectionPool                    
                	xaPool = new DisabledConnectionPool(factory);
                }
                
                xaPool.initialize(appEnvProps);
                
            } catch (ClassNotFoundException e1) {
                throw new ConnectorException(e1);
            } catch (InstantiationException e2) {
                throw new ConnectorException(e2);
            } catch (IllegalAccessException e3) {
                throw new ConnectorException(e3);
            } catch (ConnectionPoolException e4) {
                throw new ConnectorException(e4);
            }      
        }
        
        //try a connection. Do we need to consider security context?
        Connection conn = this.getXAConnection(null, null);
        conn.release();
        
        super.start();
    }
    
    /*
     * @see com.metamatrix.data.api.xa.XAConnector#getXAConnection(com.metamatrix.data.api.SecurityContext)
     */
    public Connection getXAConnection(SecurityContext context, final TransactionContext transactionContext) throws ConnectorException {
        JDBCSourceXAConnection conn = null;
        
        if(transactionContext != null){
            synchronized (idToConnections) {
                conn  = (JDBCSourceXAConnection)idToConnections.get(transactionContext.getTxnID());
                if (conn != null){
                    environment.getLogger().logTrace("Transaction " + transactionContext.getTxnID() + " already has connection, using the same connection"); //$NON-NLS-1$ //$NON-NLS-2$
                    conn.setLeased(true);
                    return conn;
                }
            }
        }
    
        conn = (JDBCSourceXAConnection)xaPool.obtain(context);
        conn.setConnectionPool(xaPool);

        if (transactionContext != null) {
        	environment.getLogger().logTrace("Obtained new connection for transaction " + transactionContext.getTxnID()); //$NON-NLS-1$
            
            synchronized (idToConnections) {                
                try { //add a synchronization to remove the map entry
                    transactionContext.getTransaction().registerSynchronization(new RemovalCallback(transactionContext, conn));
                } catch (RollbackException err) {
                    conn.release();
                    throw new ConnectorException(err);
                } catch (SystemException err) {
                    conn.release();
                    throw new ConnectorException(err);
                }
                idToConnections.put(transactionContext.getTxnID(), conn);
                conn.setInTxn(true);
                conn.setLeased(true);
            }
        } else {
            conn.setLeased(true);
        }
                
        return conn;
    }

    /**
     * Parse URL for DataSource connection properties and add to connectionProps.
     * @param url
     * @param connectionProps
     */
    static void parseURL(final String url, final Properties connectionProps) {
        // Will be: [jdbc:mmx:dbType://aHost:aPort], [DatabaseName=aDataBase], [CollectionID=aCollectionID], ...
        final List urlParts = StringUtil.split(url, ";"); //$NON-NLS-1$

        // Will be: [jdbc:mmx:dbType:], [aHost:aPort]
        final List protoHost = StringUtil.split((String)urlParts.get(0), "//"); //$NON-NLS-1$

        // Will be: [aHost], [aPort]
        final List hostPort = StringUtil.split((String) protoHost.get(1), ":"); //$NON-NLS-1$
        connectionProps.setProperty(XAJDBCPropertyNames.SERVER_NAME, (String)hostPort.get(0));
        connectionProps.setProperty(XAJDBCPropertyNames.PORT_NUMBER, (String)hostPort.get(1));

        // For "databaseName", "SID", and all optional props
        // (<propName1>=<propValue1>;<propName2>=<propValue2>;...)
        for ( int i = 1; i < urlParts.size(); i++ ) {
            final String nameVal = (String) urlParts.get( i );
            // Will be: [propName], [propVal]
            final List aProp = StringUtil.split(nameVal, "="); //$NON-NLS-1$
            if ( aProp.size() > 1) {
                final String propName = (String) aProp.get(0);
                if ( propName.equalsIgnoreCase(XAJDBCPropertyNames.DATABASE_NAME) ) {
                    connectionProps.setProperty(XAJDBCPropertyNames.DATABASE_NAME, (String) aProp.get(1));
                } else {
                    // Set optional prop names lower case so that we can find
                    // set method names for them when we introspect the DataSource
                    connectionProps.setProperty(propName.toLowerCase(), (String) aProp.get(1));
                }
            }
        }
    }
}
