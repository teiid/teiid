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

package com.metamatrix.common.config.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.core.util.StringUtil;

/**
 */
public class ConfigUtil implements StringUtil.Constants {
    
    // this editor does not create actions for any editing operations and therefore,
    // can be shared.
    private static ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();

    
    /**
     * Returns a COnfigurationObjectEditor that does not create actions.  This editor,
     * because it doesn't maintain any state, is sharable for reuse. 
     * @return
     * @since 4.3
     */
    public static ConfigurationObjectEditor getEditor() {
        return editor;
    }    
    
public static final Properties buildDefaultPropertyValues(ComponentTypeID componentTypeID, ConfigurationModelContainer model ) {
   Properties result = new Properties();
   
   Collection defns = model.getAllComponentTypeDefinitions(componentTypeID);
   
   for (Iterator it=defns.iterator(); it.hasNext();) {
       ComponentTypeDefn ctd = (ComponentTypeDefn) it.next();
       
       Object value = ctd.getPropertyDefinition().getDefaultValue();
       if (value != null) {
       		if (value instanceof String) {
       			String v = (String) value;
       			if (v.trim().length() > 0) {
					result.put(ctd.getPropertyDefinition().getName(), v );
       			}
       		} else {
				result.put(ctd.getPropertyDefinition().getName(), value.toString() );
       			
       		}
       }   
   }
   
   return result; 
    
}


public static ProductServiceConfig getFirstDeployedConnectorProductTypePSC(ConfigurationModelContainer cmc) throws Exception {

    ProductTypeID prodType = new ProductTypeID(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME);
    Iterator it = cmc.getConfiguration().getDeployedComponents().iterator();
    while(it.hasNext()) {
        final DeployedComponent dc = (DeployedComponent) it.next();
        if (dc.isDeployedConnector()) {
            ProductServiceConfigID pscID = dc.getProductServiceConfigID();

            ProductServiceConfig psc = cmc.getConfiguration().getPSC(pscID);
            if (psc.getComponentTypeID().equals(prodType)) {
                return psc;
            }
        }
        
    }
    return null;
}

 
}
