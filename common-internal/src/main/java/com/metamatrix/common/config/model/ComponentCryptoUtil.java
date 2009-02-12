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

package com.metamatrix.common.config.model;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.Assertion;


/** 
 * Utilities for encrypting and decrypting properties of configuration ComponentDefns
 * @since 4.3
 */
public class ComponentCryptoUtil {

    
    /**
     * Check whether the encrypted properties for the specified ComponentDefn can be decrypted.
     * @param defn The ComponentDefn to check.
     * @param componentTypeDefns Collection<ComponentTypeDefn>  The ComponentTypeDefns containing the 
     * PropertyDefinitions for the specified ComponentDefn.
     * @return true if the properties could be decrypted for that defn.
     * @since 4.3
     */
    public static boolean checkPropertiesDecryptable(ComponentDefn defn, Collection componentTypeDefns) {
        boolean result = true;
        
        try {
            decryptProperties(defn, componentTypeDefns);
        } catch (CryptoException e) {
            result = false;                
        }   
          
        return result;
    }

    /** 
     * Bails on the first masked property name that's contained in props but the value
     * can't be decrypted.  All masked properties that exist in props must be decryptable.
     * @param props
     * @param componentTypeIdentifier
     * @return
     * @since 4.3
     */
    public static boolean checkPropertiesDecryptable(Properties props,
                                                     Collection maskedPropertyNames) {
        if (maskedPropertyNames != null) {
            Iterator propItr = maskedPropertyNames.iterator();
            while (propItr.hasNext()) {
                String maskedPropName = (String)propItr.next();
                String maskedValue = null;
                if (props != null) {
                    maskedValue = props.getProperty(maskedPropName);
                }
                if (maskedValue != null && !CryptoUtil.canDecrypt(maskedValue)) {
                    return false;
                }
            }
        }
        
        return true;
    }       
    
    public static Properties getDecryptedProperties(ComponentDefn defn) throws ConfigurationException, CryptoException {
        ConfigurationModelContainer configModel = CurrentConfiguration.getInstance().getConfigurationModel();
        
        ComponentType componentType = configModel.getComponentType(defn.getComponentTypeID().getName());
        Assertion.isNotNull(componentType, "unknown component type"); //$NON-NLS-1$

        Collection compTypeDefns = componentType.getComponentTypeDefinitions();
        return decryptProperties(defn, compTypeDefns);
    }

    private static Properties decryptProperties(ComponentDefn defn,
                                         Collection compTypeDefns) throws CryptoException {
        Properties result = PropertiesUtils.clone(defn.getProperties(), false);
        for ( Iterator compTypeDefnItr = compTypeDefns.iterator(); compTypeDefnItr.hasNext(); ) {
            ComponentTypeDefn typeDefn = (ComponentTypeDefn) compTypeDefnItr.next();
            PropertyDefinition propDefn =  typeDefn.getPropertyDefinition();
            String propName = propDefn.getName();
            String propValue = result.getProperty(propName);
            if ( propValue != null && propDefn.isMasked() ) {
            	propValue = CryptoUtil.getDecryptor().decrypt(propValue);
                result.setProperty(propName, propValue);
            }
        }
        
        return result;
    }

}
