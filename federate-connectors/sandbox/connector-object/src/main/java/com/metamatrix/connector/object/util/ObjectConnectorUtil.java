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

package com.metamatrix.connector.object.util;

import java.util.Properties;

import com.metamatrix.connector.object.ObjectPlugin;
import com.metamatrix.connector.object.ObjectPropertyNames;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IMetadataReference;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.MetadataObject;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.SourceConnectionFactory;


/** 
 * @since 4.3
 */
public class ObjectConnectorUtil {
    private static final String DEFAULT_TRANSLATOR = "com.metamatrix.connector.object.extension.source.BasicSourceTranslator";//$NON-NLS-1$
    private static final String DEFAULT_CAPABILITIES = "com.metamatrix.connector.object.ObjectConnectorCapabilities";//$NON-NLS-1$

    
    public static final String getMetadataObjectNameInSource(final RuntimeMetadata metadata, final ICommand command, IMetadataReference reference) throws ConnectorException {
        if(reference == null) {
            return null;
        }
        MetadataID id = reference.getMetadataID();
        MetadataObject obj = metadata.getObject(id);
        if (obj != null) {
            if (obj.getNameInSource() != null) {
                return obj.getNameInSource();
            }
            return null;
        } 
            throw new MetaMatrixRuntimeException(
                              ObjectPlugin.Util.getString("ObjectConnector.Could_not_resolve_name_for_query___1", //$NON-NLS-1$
                              new Object[] {command.toString()}));

    }
    
    public static final SourceConnectionFactory createFactory(final ConnectorEnvironment environment, ClassLoader loader) 
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectorException {
        if (environment == null || loader == null || environment.getProperties() == null) {
            return null;
        }
        
        Properties props = environment.getProperties();
        String scfClassName = props.getProperty(ObjectPropertyNames.EXT_CONNECTION_FACTORY_CLASS);  
        
          //create source connection factory
          Class scfClass = loader.loadClass(scfClassName);
          SourceConnectionFactory adminFactory = (SourceConnectionFactory) scfClass.newInstance();
          adminFactory.initialize(environment);
          
          return adminFactory;
        
    }
    
    public static final ISourceTranslator createTranslator(final ConnectorEnvironment environment, ClassLoader loader) 
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectorException {

    //create ResultsTranslator
        ISourceTranslator translator = null;
        String className = environment.getProperties().getProperty(ObjectPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS);  
        if (className == null) {
            environment.getLogger().logInfo( ObjectPlugin.Util.getString("ObjectConnector.Property_{0}_is_not_defined_use_default", new Object[] {ObjectPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS, DEFAULT_TRANSLATOR} )); //$NON-NLS-1$
            className = DEFAULT_TRANSLATOR;
        }
        
        Class sourceTransClass = loader.loadClass(className);
        translator = (ISourceTranslator) sourceTransClass.newInstance();
        translator.initialize(environment);   
        
        return translator;

    }   
    
    
    public static final ConnectorCapabilities createCapabilities(final ConnectorEnvironment environment, ClassLoader loader) 
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectorException {
    
        //create Capabilities
        ConnectorCapabilities capabilities;
        String className = environment.getProperties().getProperty(ObjectPropertyNames.EXT_CAPABILITY_CLASS);  
        if(className == null){
            environment.getLogger().logInfo( ObjectPlugin.Util.getString("ObjectConnector.Property_{0}_is_not_defined_use_default", new Object[] {ObjectPropertyNames.EXT_CAPABILITY_CLASS, DEFAULT_CAPABILITIES} )); //$NON-NLS-1$
            className = DEFAULT_CAPABILITIES;
        }
        Class capabilitiesClass = loader.loadClass(className);
        capabilities = (ConnectorCapabilities) capabilitiesClass.newInstance();           

        return capabilities;
    }

}
