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
package com.metamatrix.connector.jdbc.sqlserver;

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
public class SqlServerSQLTranslator extends BasicSQLTranslator {

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
        //FEDERATE-168 this is not necessary for SQL Server 2008
        functionModifiers.put("mod", new SqlServerModModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("chr", new AliasModifier("char")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("concat", new AliasModifier("+")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("||", new AliasModifier("+")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ifnull", new AliasModifier("isnull")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("nvl", new AliasModifier("isnull")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("lcase", new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("length", new AliasModifier("len")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofmonth", new AliasModifier("day")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ucase", new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("substring", new SubstringFunctionModifier(languageFactory, "substring", "len")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        functionModifiers.put("cast", new SqlServerConvertModifier(languageFactory));        //$NON-NLS-1$ 
        functionModifiers.put("convert", new SqlServerConvertModifier(languageFactory));      //$NON-NLS-1$       
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
        SQLConversionVisitor visitor = new SqlServerSQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }
    
    
}
