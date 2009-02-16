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

/*
 */
package com.metamatrix.connector.jdbc;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.connector.pool.ConnectorIdentity;
import com.metamatrix.connector.pool.ConnectorIdentityFactory;

/**
 * JDBC implementation of Connector interface.
 */
@ConnectionPooling
public class JDBCConnector implements Connector, ConnectorIdentityFactory {
    protected ConnectorEnvironment environment;
    private ConnectorLogger logger;
    private JDBCSourceConnectionFactory factory;
    
    private ConnectorCapabilities capabilities;
    
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
        //Shutdown Factory
        if(this.factory != null) {
            this.factory.shutdown();
        }
    }

    @Override
    public void start(ConnectorEnvironment environment)
    		throws ConnectorException {
    	logger = environment.getLogger();
        this.environment = environment;
        
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_initialized._1")); //$NON-NLS-1$
        
        capabilities = createCapabilities(environment, Thread.currentThread().getContextClassLoader());
        String scfClassName = environment.getProperties().getProperty(JDBCPropertyNames.EXT_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory");  //$NON-NLS-1$

        try {
            //create source connection factory
            Class scfClass = Thread.currentThread().getContextClassLoader().loadClass(scfClassName);
            this.factory = (JDBCSourceConnectionFactory) scfClass.newInstance();
            factory.initialize(environment);
        } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        } catch (ConnectorException e5) {
            this.cleanUp();
            throw e5;
        }

        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_started._4")); //$NON-NLS-1$
    }

    /*
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(ExecutionContext context) throws ConnectorException {
    	return factory.getConnection(context);
    }

    @Override
	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}

	@Override
	public ConnectorIdentity createIdentity(ExecutionContext context)
			throws ConnectorException {
		return factory.createIdentity(context);
	}
}
