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

package com.metamatrix.common.pooling.api;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ResourceComponentType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.object.Multiplicity;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;

public class ConfigUtil {
    
    private static BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
    
    public static ResourceDescriptor createDescriptor(String resourceName, ComponentTypeID  compTypeID) {
        ConfigurationID configID = new ConfigurationID("TestConfig"); //$NON-NLS-1$
        
        ResourceDescriptor descriptor = editor.createResourceDescriptor(configID, compTypeID, resourceName);

        return descriptor;
        
    }
    
    
    public static ResourceComponentType createComponentType(String name, ComponentTypeID superID, ComponentTypeID parentID) throws Exception {
        
        
        
        ResourceComponentType resourceType = (ResourceComponentType) editor.createComponentType(ComponentType.RESOURCE_COMPONENT_TYPE_CODE,
                                name, parentID, superID, false, false); 
                                
       
            createComponentTypeDefn(ResourcePoolPropertyNames.ALLOW_SHRINKING, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, resourceType);
    //        createComponentTypeDefn(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.SHRINK_PERIOD, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.SHRINK_INCREMENT, resourceType); 
            createComponentTypeDefn(ResourcePoolPropertyNames.LIVE_AND_UNUSED_TIME, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.WAIT_TIME_FOR_RESOURCE, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_MODE, resourceType);
            createComponentTypeDefn(ResourcePoolPropertyNames.EXTEND_MAXIMUM_POOL_SIZE_PERCENT, resourceType); 
                             
        return resourceType;
        
    }
    
    public static void createComponentTypeDefn(String name, ComponentType type) throws Exception {
        
 
        String nameString = name;
        String displayNameString = name;
        String shortDescriptionString = name + " description"; //$NON-NLS-1$
        String defaultValueString = "Default Value"; //$NON-NLS-1$
        String multiplicityString = "1"; //$NON-NLS-1$
        String propertyTypeString = "String"; //$NON-NLS-1$
        String valueDelimiterString = ","; //$NON-NLS-1$
        String isConstrainedToAllowedValuesString = "false"; //$NON-NLS-1$
        String isExpertString = "false"; //$NON-NLS-1$
        String isHiddenString = "false"; //$NON-NLS-1$
        String isMaskedString = "false"; //$NON-NLS-1$
        String isModifiableString = "true"; //$NON-NLS-1$
        String isPreferredString = "true"; //$NON-NLS-1$
        
        Multiplicity mult = null;

            mult = Multiplicity.getInstance(multiplicityString);
        
        PropertyType proptype = PropertyType.getInstance(propertyTypeString);
        
        boolean isConstrainedToAllowedValues = (Boolean.valueOf(isConstrainedToAllowedValuesString)).booleanValue();
        boolean isExpert = (Boolean.valueOf(isExpertString)).booleanValue();
        boolean isHidden = (Boolean.valueOf(isHiddenString)).booleanValue();
        boolean isMasked = (Boolean.valueOf(isMaskedString)).booleanValue();
        boolean isModifiable = (Boolean.valueOf(isModifiableString)).booleanValue();
        boolean isPreferred = (Boolean.valueOf(isPreferredString)).booleanValue();
        
        
        PropertyDefinitionImpl defn = new PropertyDefinitionImpl(nameString, 
                            displayNameString, proptype,
                        mult,  shortDescriptionString, defaultValueString,
                        null, valueDelimiterString,
                        isHidden, isPreferred, isExpert, isModifiable);
                
        defn.setMasked(isMasked);    


        defn.setConstrainedToAllowedValues(isConstrainedToAllowedValues);   
 
        
        editor.createComponentTypeDefn(type, defn,false); 


        
    }
    

}
