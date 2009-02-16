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

package com.metamatrix.connector.object;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.connector.pool.ConnectorIdentity;
import com.metamatrix.connector.pool.ConnectorIdentityFactory;

/**
 * Implmentation of the connector interface.
 */
public class ObjectConnector implements Connector, ConnectorIdentityFactory {
    
    private ConnectorLogger logger;
    private ConnectorEnvironment env;
    private boolean start = false;
    private SourceConnectionFactory factory;
    private ConnectorCapabilities capabilities;

    /**
     * Initialization with environment.
     */
    public void start( ConnectorEnvironment environment ) throws ConnectorException {
        logger = environment.getLogger();
        this.env = environment;

        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            this.capabilities = ObjectConnectorUtil.createCapabilities(environment, loader);
            factory = ObjectConnectorUtil.createFactory(this.env, loader);
	     } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        }         
               
        //test connection
        getConnection(null);

        start = true;
        logger.logInfo(ObjectPlugin.Util.getString("ObjectConnector.Connector_started_4"));//$NON-NLS-1$
    }

    public void stop() {
        if(!start){
            return;
        }

        env = null;
        start = false;
        
        logger.logInfo(ObjectPlugin.Util.getString("ObjectConnector.Connector_stopped_3"));//$NON-NLS-1$
        
        logger = null;
    }
    
    /* 
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    @Override
    public Connection getConnection(final ExecutionContext context) throws ConnectorException {
        return factory.createConnection(factory.createIdentity(context));
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

