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

package com.metamatrix.connector.object;

import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.SourceConnection;
import com.metamatrix.data.pool.SourceConnectionFactory;

/**
 * Implmentation of the connector interface.
 */
public class ObjectConnector implements Connector {
    

    // used when connection pooling is not used @see usePooling
    private SourceConnection connection = null;
    private SourceConnectionFactory factory=null;
    
    private ConnectorLogger logger;
    private ConnectorEnvironment env;
    private boolean start = false;
    private boolean usePooling = false;


    /**
     * Initialization with environment.
     */
    public void initialize( ConnectorEnvironment environment ) throws ConnectorException {
        logger = environment.getLogger();
        this.env = environment;

        // License checking for Text Connector
//        final boolean licensed = LicenseChecker.hasValidProductLicense(MetaMatrixProductNames.ConnectorProduct.TEXT,
//                                                                MetaMatrixProductNames.VERSION_NUMBER,
//                                                                false);
//        if ( ! licensed ) {
//            Object[] params = new Object[] {MetaMatrixProductNames.ConnectorProduct.TEXT};
//            final String msg = SystemPlugin.Util.getString("SystemConnector.No_license_found_for_{0}", params); //$NON-NLS-1$
//            final ConnectorException e = new ConnectorException(msg);
//            logger.logError(msg, e);
//            throw e;
//        }
        
        try {
            ClassLoader loader = this.getClass().getClassLoader();

            factory = ObjectConnectorUtil.createFactory(this.env, loader);
//            String scfClassName = env.getProperties().getProperty(ObjectPropertyNames.EXT_CONNECTION_FACTORY_CLASS);  //$NON-NLS-1$
//            if (scfClassName == null) {
//                String msg = ObjectPlugin.Util.getString("ObjectConnector.General.Property_{0}_is_required", ObjectPropertyNames.EXT_CONNECTION_FACTORY_CLASS);//$NON-NLS-1$
//                logger.logError(msg);
//                throw new ConnectorException(msg);
//            }
//            
//            //create source connection factory
//            Class scfClass = this.getClass().getClassLoader().loadClass(scfClassName);
//            factory = (SourceConnectionFactory) scfClass.newInstance();
//            factory.initialize(environment);

//            //create pool
//            pool = new ConnectionPool(factory);
//            pool.initialize(environment.getProperties());

//            factory.initialize(env);
            
           
         } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        }         
               
        //test connection
        getConnection(null);

        logger.logInfo(ObjectPlugin.Util.getString("ObjectConnector.Connector_initialized_1"));//$NON-NLS-1$
    }

    public void stop() {
        if(!start){
            return;
        }

        if (!usePooling) {
            try {
                connection.closeSource();
            } catch (ConnectorException err) {
            }
        }
        connection = null;
        factory = null;   
        env = null;
        start = false;
        
        logger.logInfo(ObjectPlugin.Util.getString("ObjectConnector.Connector_stopped_3"));//$NON-NLS-1$
        
        logger = null;


    }

    public void start() throws ConnectorException {
  //           if(pool == null){
        
        start = true;
        logger.logInfo(ObjectPlugin.Util.getString("ObjectConnector.Connector_started_4"));//$NON-NLS-1$
    } 

    
    /* 
     * @see com.metamatrix.data.Connector#getConnection(com.metamatrix.data.SecurityContext)
     */
    public Connection getConnection(final SecurityContext context) throws ConnectorException {
        if (!usePooling) {
            return (Connection) createConnection(context); 
        }
        return null;
    }
   
    protected SourceConnection createConnection(SecurityContext context) throws ConnectorException {
        if (connection == null) {
            connection = factory.createConnection(factory.createIdentity(context));
        }
        return connection;
        }    
    
}

