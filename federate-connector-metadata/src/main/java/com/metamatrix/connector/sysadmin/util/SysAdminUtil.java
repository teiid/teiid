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

package com.metamatrix.connector.sysadmin.util;

import java.util.Properties;

import com.metamatrix.connector.sysadmin.SysAdminPropertyNames;
import com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IMetadataReference;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.MetadataObject;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.3
 */
public class SysAdminUtil {

    public static ISysAdminConnectionFactory createFactory(final ConnectorEnvironment environment, ClassLoader loader) throws ConnectorException {
        if (environment == null || loader == null || environment.getProperties() == null) {
            return null;
        }
        
        Properties props = environment.getProperties();
        try {
            String scfClassName = props.getProperty(SysAdminPropertyNames.SYSADMIN_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.sysadmin.SysAdminConnectionFactory");  //$NON-NLS-1$                      
            
              //create source connection factory
              Class scfClass = loader.loadClass(scfClassName);
              ISysAdminConnectionFactory adminFactory = (ISysAdminConnectionFactory) scfClass.newInstance();
              adminFactory.init(environment);
              
              return adminFactory;
        
          } catch (ClassNotFoundException e1) {
              throw new ConnectorException(e1);
          } catch (InstantiationException e2) {
              throw new ConnectorException(e2);
          } catch (IllegalAccessException e3) {
              throw new ConnectorException(e3);
          }         
    }
    
    /**
     * A helper method used to get the name to use when execution is performed on the source. 
     * @since 4.2
     */
    public static String getExecutionName(RuntimeMetadata metadata, IMetadataReference reference) throws ConnectorException {
        if(reference == null) {
            return null;
        }
        String refName =getMetadataObjectNameInSource(metadata, reference);

        if (refName == null) {
            MetadataID id = reference.getMetadataID();
            refName = id.getName();
        }
        return refName;
    }   
    
    public static String getMetadataObjectNameInSource(RuntimeMetadata metadata, IMetadataReference reference) throws ConnectorException {
        if(reference == null) {
            return null;
        }
        MetadataID id = reference.getMetadataID();
        MetadataObject obj = metadata.getObject(id);
        if (obj != null && obj.getNameInSource() != null) {
                return obj.getNameInSource();
        } 
        return null;
    }      
    
}
