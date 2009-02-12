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

package com.metamatrix.common.config.api;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;

/**
 * The ResourceModel provides the PropertyDefinitions for the
 * PropertiedObjectEditor use.  The model defines the
 * viewing and editable nature of each related resource.
 */
public final class ResourceModel {
    

    private static Map resourceDefns = new HashMap(20);
    
    private static final String RESOURCE_MODEL_FILE = "com/metamatrix/common/config/api/resourcetypemodel.xml"; //$NON-NLS-1$

 
    //************************************************************************
    //initializer
    static {

        try {
            InputStream input =
                ClassLoader.getSystemResourceAsStream(RESOURCE_MODEL_FILE);
            
            if (input == null) {
                throw new RuntimeException(CommonPlugin.Util.getString("ResourceModel.Resource_model_file_not_found",RESOURCE_MODEL_FILE)); //$NON-NLS-1$                
                
             }
            
            XMLConfigurationImportExportUtility importutil = new XMLConfigurationImportExportUtility();
            
            Collection componentTypes = importutil.importComponentTypes(input, new BasicConfigurationObjectEditor(false));
            
            for (Iterator it=componentTypes.iterator(); it.hasNext(); ) {
                ComponentType ct = (ComponentType) it.next();
                resourceDefns.put(ct.getFullName(), ct);
                
            }
            


            //no other work to be done here
        } catch (final Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e.getMessage());
        }
        //If something goes awry loading the properties file, we still have a
        //Properties object.  Method calls to getType and getMessage will return
        //null for everything.

    }

    public static ComponentType getComponentType(String resourceName) {
        if (resourceDefns.containsKey(resourceName)) {
            return (ComponentType) resourceDefns.get(resourceName);
        }
        
        return null;
        
    }
    
    public static Properties getDefaultProperties(String resourceName) {
        ComponentType ct = null;
        if (resourceDefns.containsKey(resourceName)) {
            ct = (ComponentType) resourceDefns.get(resourceName);
            return ct.getDefaultPropertyValues();
                
        }
        
        return new Properties();
        
    }    
    
}
