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


import com.metamatrix.connector.object.extension.IObjectSource;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.SourceConnection;

/**
 * Implementation of Connection interface for Object connection.
 */
public class ObjectConnection implements Connection, SourceConnection{
    
//    private static final String DEFAULT_TRANSLATOR = "com.metamatrix.connector.object.extension.source.BasicSourceTranslator";//$NON-NLS-1$
    
    private ConnectorCapabilities capabilities;
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
            ClassLoader loader = this.getClass().getClassLoader();
            
            this.capabilities = ObjectConnectorUtil.createCapabilities(environment, loader);
            
            this.translator = ObjectConnectorUtil.createTranslator(environment, loader);
// 
//            //create ResultsTranslator
//            String className = environment.getProperties().getProperty(ObjectPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS);  //$NON-NLS-1$
//            if (className == null) {
//                this.logger.logInfo( ObjectPlugin.Util.getString("ObjectConnection.Property_{0}_is_not_defined_use_default", new Object[] {ObjectPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS, DEFAULT_TRANSLATOR} )); //$NON-NLS-1$
//                className = DEFAULT_TRANSLATOR;
//            }
//            
//            Class sourceTransClass = loader.loadClass(className);
//            translator = (ISourceTranslator) sourceTransClass.newInstance();
//            translator.initialize(environment);           
//
////            //create Capabilities
////            className = environment.getProperties().getProperty(ObjectPropertyNames.EXT_CAPABILITY_CLASS);  //$NON-NLS-1$
////            if(className == null){
////                throw new ConnectorException(ObjectPlugin.Util.getString("ObjectConnection.Property_{0}_is_required,_but_not_defined_1", ObjectPropertyNames.EXT_CAPABILITY_CLASS)); //$NON-NLS-1$
////            }
////            Class capabilitiesClass = loader.loadClass(className);
////            capabilities = (ConnectorCapabilities) capabilitiesClass.newInstance();           
//            
        } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        }
        
              
    }
    
    /**
     * Create soap execution.
     * @param command ICommand containing the query 
     */
    public Execution createExecution( int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata )throws ConnectorException {


         switch(executionMode) {
            case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
            {
                throw new ConnectorException(ObjectPlugin.Util.getString("ObjectConnection.Only_procedures_are_supported")); //$NON-NLS-1$")
            }
            case ConnectorCapabilities.EXECUTION_MODE.BATCHED_UPDATES:
            case ConnectorCapabilities.EXECUTION_MODE.UPDATE:
            case ConnectorCapabilities.EXECUTION_MODE.BULK_INSERT:                
            {
                throw new ConnectorException(ObjectPlugin.Util.getString("ObjectConnection.Updates_not_supported")); //$NON-NLS-1$")
            }
            case ConnectorCapabilities.EXECUTION_MODE.PROCEDURE:
            {
                return new ObjectProcedureExecution(getObjectSource(), translator, metadata, env);
            }
            default:
            {
                throw new ConnectorException(ObjectPlugin.Util.getString("ObjectConnection.Only_procedures_are_supported")); //$NON-NLS-1$ 
            } 
         }
    }
    
    /**
     * Get the metadata of the source the connector is connected to.
     * @return ConnectorMetadata
     */
    public ConnectorMetadata getMetadata() { 
        return null;  
    }
    
    /* 
     * @see com.metamatrix.data.Connection#getCapabilities()
     */
    public ConnectorCapabilities getCapabilities() {
        return capabilities;
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
    public void closeSource() { 
        try {
            api.closeSource();
        } catch(Exception e) {
            
        }
        
        capabilities=null;
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

    /** 
     * @see com.metamatrix.data.pool.SourceConnection#isFailed()
     * @since 4.3
     */
    public boolean isFailed() {
        return api.isFailed();
    }

    /** 
     * @see com.metamatrix.data.api.Connection#release()
     * @since 4.3
     */
    public void release() {
    }   

    

    
}
