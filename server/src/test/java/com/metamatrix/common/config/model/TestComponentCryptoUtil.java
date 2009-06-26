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


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeDefnID;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.reader.PropertiesConfigurationReader;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.util.crypto.CryptoUtil;


public class TestComponentCryptoUtil extends TestCase {

    
    private static String ENCRYPTED;
    private final static String NOT_ENCRYPTED = "not decrypted"; //$NON-NLS-1$

    
    public TestComponentCryptoUtil(String name) throws Exception {
        super(name);        
    }

    
    public void setUp() throws Exception {
        ENCRYPTED = new String(CryptoUtil.stringEncrypt("password"));  //$NON-NLS-1$
    }

    public void testCheckPropertiesDecryptable() throws Exception {
        BasicComponentDefn defn = new BasicConnectorBinding(null, new ConnectorBindingID(null, "binding"), null);  //$NON-NLS-1$

        //positive case
        List componentTypeDefns = new ArrayList();
        
        helpSetProperty(defn, componentTypeDefns, "prop1", ENCRYPTED, true);  //$NON-NLS-1$
        helpSetProperty(defn, componentTypeDefns, "prop2", NOT_ENCRYPTED, false);  //$NON-NLS-1$
        
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable(defn, componentTypeDefns);
        assertTrue(decryptable);
        

        
        //negative case
        componentTypeDefns = new ArrayList();
        
        helpSetProperty(defn, componentTypeDefns, "prop1", ENCRYPTED, true);  //$NON-NLS-1$
        helpSetProperty(defn, componentTypeDefns, "prop2", NOT_ENCRYPTED, true);  //$NON-NLS-1$
        
        decryptable = ComponentCryptoUtil.checkPropertiesDecryptable(defn, componentTypeDefns);
        assertFalse(decryptable);

    }

    
    
    /**
     * Helper method that sets a property on a ComponentDefn, and adds it to a list of ComponentTypeDefns 
     * 
     * @since 4.3
     */
    private void helpSetProperty(BasicComponentDefn defn, List componentTypeDefns, String key, String value, boolean isMasked) {
        //set on the ComponentDefn
        defn.addProperty(key, value);

        //create a ComponentTypeDefn and add to the list
        PropertyDefinitionImpl pd = new PropertyDefinitionImpl();
        pd.setName(key);
        pd.setMasked(isMasked);
        ComponentTypeDefn ctd = new BasicComponentTypeDefn(new ComponentTypeDefnID(key), null, pd, false, false);
        componentTypeDefns.add(ctd);
    } 
    
    public void testCheckPropertiesDecryptableEmpty() throws Exception {
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable(new Properties(), new ArrayList());
        assertTrue("Expected true ", decryptable); //$NON-NLS-1$
    }
    
    public void testCheckPropertiesDecryptableNull() throws Exception {
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable((Properties)null, (Collection)null);
        assertTrue("Expected true ", decryptable); //$NON-NLS-1$
    }

    public void testCheckPropertiesDecryptableNullProperties() throws Exception {
        Collection maskedNames = new ArrayList();
        maskedNames.add("Roberto"); //$NON-NLS-1$
        maskedNames.add("Pietro"); //$NON-NLS-1$
        maskedNames.add("Digiorno"); //$NON-NLS-1$
        maskedNames.add("Digragorio"); //$NON-NLS-1$
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable((Properties)null, maskedNames);
        assertTrue("Expected true ", decryptable); //$NON-NLS-1$
    }
    
    public void testCheckPropertiesDecryptableFail() throws Exception {
        String maskedPropName = "password"; //$NON-NLS-1$
        Collection maskedNames = new ArrayList();
        maskedNames.add(maskedPropName);
        
        Properties props = new Properties();
        props.setProperty(maskedPropName, "mm"); //$NON-NLS-1$
        
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable(props, maskedNames);
        assertFalse("Expected not decryptable ", decryptable); //$NON-NLS-1$
    }
    
    public void testCheckPropertiesDecryptablePass() throws Exception {
        String maskedPropName = "password"; //$NON-NLS-1$
        Collection maskedNames = new ArrayList();
        maskedNames.add(maskedPropName);
        
        Properties props = new Properties();
        props.setProperty(maskedPropName, ENCRYPTED); //$NON-NLS-1$
        
        boolean decryptable = ComponentCryptoUtil.checkPropertiesDecryptable(props, maskedNames);
        assertTrue("Expected decryptable ", decryptable); //$NON-NLS-1$
    }
    
    
    
}


