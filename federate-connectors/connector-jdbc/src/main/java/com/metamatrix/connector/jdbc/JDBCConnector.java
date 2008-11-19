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
package com.metamatrix.connector.jdbc;

import java.util.Properties;

import com.metamatrix.data.api.GlobalCapabilitiesProvider;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.data.monitor.ConnectionStatus;
import com.metamatrix.data.monitor.MonitoredConnector;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.ConnectionPoolException;
import com.metamatrix.data.pool.DisabledConnectionPool;
import com.metamatrix.dqp.internal.datamgr.ConnectorPropertyNames;

/**
 * JDBC implementation of Connector interface.
 */
public class JDBCConnector implements Connector, MonitoredConnector, GlobalCapabilitiesProvider {
    protected ConnectorEnvironment environment;
    protected boolean connectionPoolEnabled = true;
    private ConnectorLogger logger;
    private ConnectionPool pool;
    private JDBCSourceConnectionFactory factory;
    
    private String deregisterDriver;
    private boolean initializedClean = false;
    
    private ConnectorCapabilities capabilities;
    
    public void initialize(ConnectorEnvironment environment) throws ConnectorException {
        logger = environment.getLogger();
        this.environment = environment;
        
        Properties props = environment.getProperties();
        this.deregisterDriver = props.getProperty(ConnectorPropertyNames.DEREGISTER_DRIVER); 

        this.connectionPoolEnabled = true;
        String connectionPoolEnabledStr = props.getProperty(ConnectorPropertyNames.CONNECTION_POOL_ENABLED); 
        if(connectionPoolEnabledStr!=null) {
        	this.connectionPoolEnabled = Boolean.valueOf(connectionPoolEnabledStr).booleanValue();
        }
        
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_initialized._1")); //$NON-NLS-1$
        
        capabilities = createCapabilities(environment, Thread.currentThread().getContextClassLoader()); 
    }

	static ConnectorCapabilities createCapabilities(ConnectorEnvironment environment, ClassLoader loader)
			throws ConnectorException {
		//create Capabilities
        String className = environment.getProperties().getProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS);  
        if(className == null){
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnection.Property_{0}_is_required,_but_not_defined_1", JDBCPropertyNames.EXT_CAPABILITY_CLASS)); //$NON-NLS-1$
        }
        
        try {
            Class capabilitiesClass = loader.loadClass(className);
			ConnectorCapabilities result = (ConnectorCapabilities) capabilitiesClass.newInstance();
	        if(result instanceof JDBCCapabilities) {
	            String setCriteriaBatchSize = environment.getProperties().getProperty(JDBCPropertyNames.SET_CRITERIA_BATCH_SIZE);
	            if(setCriteriaBatchSize != null) {
	                int maxInCriteriaSize = Integer.parseInt(setCriteriaBatchSize);
	                if(maxInCriteriaSize > 0) {
	                    ((JDBCCapabilities)result).setMaxInCriteriaSize(maxInCriteriaSize);
	                }
	            } 
	        }
	        return result;
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

    public void stop() {     
        cleanUp();
                
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_stopped._3")); //$NON-NLS-1$
    }
    
    public void cleanUp() {
        if(pool != null) {
            pool.shutDown();
            pool = null;
        }
        
        //Shutdown Factory
        if(this.deregisterDriver != null && this.factory != null) {
            this.factory.shutdown();
        }
    }

    public void start() throws ConnectorException {
        if(pool == null){
            String scfClassName = environment.getProperties().getProperty(JDBCPropertyNames.EXT_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory");  //$NON-NLS-1$

            try {
                //create source connection factory
                Class scfClass = Thread.currentThread().getContextClassLoader().loadClass(scfClassName);
                this.factory = (JDBCSourceConnectionFactory) scfClass.newInstance();
                factory.initialize(environment);
                
                // If ConnectionPool is enabled, create DefaultConnectionPool
                if(this.connectionPoolEnabled) {
                	pool = new ConnectionPool(factory);
                // If ConnectionPool is disabled, create DisabledConnectionPool
                } else {
                	pool = new DisabledConnectionPool(factory);
                }
                pool.initialize(environment.getProperties());
                initializedClean = true;
            } catch (ClassNotFoundException e1) {
                throw new ConnectorException(e1);
            } catch (InstantiationException e2) {
                throw new ConnectorException(e2);
            } catch (IllegalAccessException e3) {
                throw new ConnectorException(e3);
            } catch (ConnectionPoolException e4) {
                throw new ConnectorException(e4);
            } catch (ConnectorException e5) {
                this.cleanUp();
                throw e5;
            }
        }

        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_started._4")); //$NON-NLS-1$
    }

    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(SecurityContext context) throws ConnectorException {
        JDBCSourceConnection conn = (JDBCSourceConnection)pool.obtain(context);
        conn.setConnectionPool(pool);
        return conn;
    }
    
    /** 
     * @see com.metamatrix.data.monitor.MonitoredConnector#getStatus()
     */
    public ConnectionStatus getStatus() {
        if (pool != null) {
            ConnectionStatus status = pool.getStatus();
            if (status.aliveStatus == AliveStatus.UNKNOWN && !initializedClean) {
                status.aliveStatus = AliveStatus.DEAD;
            }
            return status;
        }
        return new ConnectionStatus(AliveStatus.DEAD);
    }

	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}
}
