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

/*
 */
package com.metamatrix.connector.jdbc.informix;

import java.util.*;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.impl.*;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class InformixSQLTranslator extends BasicSQLTranslator {

    private Map functionModifiers;

    public void initialize(ConnectorEnvironment env,
                           RuntimeMetadata metadata) throws ConnectorException {
        
        super.initialize(env, metadata);
        initializeFunctionModifiers();  
    }

    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        functionModifiers.put("cast", new DropFunctionModifier());        //$NON-NLS-1$ 
        functionModifiers.put("convert", new DropFunctionModifier());      //$NON-NLS-1$       
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return this.functionModifiers;
    }
}
