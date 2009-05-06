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

package com.metamatrix.connector.metadata.adapter;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.basic.BasicConnectorCapabilities;


/**
 * Describes the capabilities of the object connector, which are few.
 */
public class ObjectConnectorCapabilities extends BasicConnectorCapabilities {

    private static ObjectConnectorCapabilities INSTANCE = new ObjectConnectorCapabilities(); 

    public static ConnectorCapabilities getInstance() {
        return INSTANCE;
    }

    private ObjectConnectorCapabilities() {
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsCompareCriteriaEquals()
     */
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#supportsLikeCriteria()
     * @since 4.3
     */
    public boolean supportsLikeCriteria() {
        return true;
    }

    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#getSupportedFunctions()
     * @since 4.3
     */
    public List getSupportedFunctions() {
        List supportedFunctions = new ArrayList();
        List superFunctions = super.getSupportedFunctions();
        if(superFunctions != null) {
            supportedFunctions.addAll(superFunctions);
        }
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        return supportedFunctions;
    }
    
    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#supportsLikeCriteriaEscapeCharacter()
     * @since 5.0
     */
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return true;
    }    
    
}