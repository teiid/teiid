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
package com.metamatrix.connector.jdbc.sybase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.connector.jdbc.extension.impl.SubstringFunctionModifier;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class SybaseSQLTranslator extends BasicSQLTranslator {
    
    private Map functionModifiers;
    private Properties connectorProperties;
    private ILanguageFactory languageFactory;

    public SybaseSQLTranslator() {
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#initialize(com.metamatrix.data.api.ConnectorEnvironment, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public void initialize(ConnectorEnvironment env, RuntimeMetadata metadata) throws ConnectorException {
        super.initialize(env, metadata);
        connectorProperties = getConnectorEnvironment().getProperties();
        languageFactory = getConnectorEnvironment().getLanguageFactory();
        initializeFunctionModifiers();
    }    

    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        functionModifiers.put("mod", new ModFunctionModifier()); //$NON-NLS-1$ 
        functionModifiers.put("chr", new AliasModifier("char")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("concat", new AliasModifier("+")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("||", new AliasModifier("+")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("lcase", new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("length", new AliasModifier("char_length")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ifnull", new AliasModifier("isnull")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ucase", new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("nvl", new AliasModifier("isnull")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("substring", new SubstringFunctionModifier(languageFactory, "substring", "char_length")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        functionModifiers.put("cast", new SybaseConvertModifier(languageFactory));        //$NON-NLS-1$ 
        functionModifiers.put("convert", new SybaseConvertModifier(languageFactory));      //$NON-NLS-1$   
        functionModifiers.put("formattimestamp", new FormatTimestampModifier(languageFactory));       //$NON-NLS-1$
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return functionModifiers;
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new SybaseSQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }
    
}
