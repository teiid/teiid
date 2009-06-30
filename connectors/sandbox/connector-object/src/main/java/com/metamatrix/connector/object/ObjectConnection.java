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


import org.teiid.adminapi.AdminException;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.object.extension.IObjectSource;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.core.util.ArgCheck;

/**
 * Implementation of Connection interface for Object connection.
 */
public class ObjectConnection extends BasicConnection {
    
//    private static final String DEFAULT_TRANSLATOR = "com.metamatrix.connector.object.extension.source.BasicSourceTranslator";//$NON-NLS-1$
    
    private ISourceTranslator translator;
    private IObjectSource api;


    private ConnectorEnvironment env;
    
    // Connector logger
     ConnectorLogger logger;

    /**
     * Constructor.
     * @param env
     * @throws AdminException 
     */    
    public ObjectConnection(ConnectorEnvironment environment, IObjectSource objectSource) throws ConnectorException {
        if (objectSource == null) {
            ArgCheck.isNotNull(objectSource, "ObjectSource is null");//$NON-NLS-1$
        }
        this.api = objectSource;
        this.env = environment;
        this.logger = environment.getLogger();
        
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            
            this.translator = ObjectConnectorUtil.createTranslator(environment, loader);
        } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        }
        
              
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new ObjectProcedureExecution(command, getObjectSource(), translator, metadata, env);
    }
    
    /** 
     * @see com.metamatrix.connector.object.ObjectConnection#getAPI()
     * @since 4.3
     */
    public IObjectSource getObjectSource() {
        return api;
    }    
    
    /** 
     * @see com.metamatrix.data.pool.SourceConnection#closeSource()
     * @since 4.2
     */
    public void close() { 
        try {
            api.closeSource();
        } catch(Exception e) {
            
        }
        
        translator=null;
        logger =null;
        api=null;
        env=null;
    }

    /** 
     * @see com.metamatrix.data.pool.SourceConnection#isAlive()
     * @since 4.3
     */
    public boolean isAlive() {
        return api.isAlive();
    }

    @Override
    public void closeCalled() {
    	
    }
    
}
