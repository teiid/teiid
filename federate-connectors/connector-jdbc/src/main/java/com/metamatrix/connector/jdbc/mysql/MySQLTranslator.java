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

package com.metamatrix.connector.jdbc.mysql;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.3
 */
public class MySQLTranslator extends BasicSQLTranslator {

    private Map functionModifiers;
    private Properties connectorProperties;
    private ILanguageFactory languageFactory;

    public void initialize(ConnectorEnvironment env,
                           RuntimeMetadata metadata) throws ConnectorException {
        
        super.initialize(env, metadata);
        ConnectorEnvironment connEnv = getConnectorEnvironment();
        this.connectorProperties = connEnv.getProperties();
        this.languageFactory = connEnv.getLanguageFactory();
        initializeFunctionModifiers();  

    }

    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        
        functionModifiers.put("cast", new MySQLConvertModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("convert", new MySQLConvertModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("nvl", new AliasModifier("ifnull")); //$NON-NLS-1$ //$NON-NLS-2$
    }  
 
    public Map getFunctionModifiers() {
        return functionModifiers;
    }

    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new MySQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }
}
