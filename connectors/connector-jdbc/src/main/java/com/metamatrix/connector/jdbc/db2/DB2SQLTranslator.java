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

/*
 */
package com.metamatrix.connector.jdbc.db2;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class DB2SQLTranslator extends BasicSQLTranslator {

    private Map functionModifiers;
    private ILanguageFactory languageFactory;

    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#initialize(com.metamatrix.data.api.ConnectorEnvironment, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public void initialize(ConnectorEnvironment env, RuntimeMetadata metadata) throws ConnectorException {
        super.initialize(env, metadata);
        languageFactory = getConnectorEnvironment().getLanguageFactory();
        initializeFunctionModifiers();
    }    

    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        functionModifiers.put("cast", new DB2ConvertModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("char", new AliasModifier("chr")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("convert", new DB2ConvertModifier(languageFactory)); //$NON-NLS-1$        
        functionModifiers.put("dayofmonth", new AliasModifier("day")); //$NON-NLS-1$ //$NON-NLS-2$        
        functionModifiers.put("ifnull", new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("nvl", new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("substring", new AliasModifier("substr")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return functionModifiers;
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#modifyCommand(com.metamatrix.connector.language.ICommand, com.metamatrix.data.SecurityContext)
     */
    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        // DB2-specific modification
        DB2SQLModificationVisitor visitor = new DB2SQLModificationVisitor(getConnectorEnvironment().getLanguageFactory());
        command.acceptVisitor(visitor);
        return command;
    }
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new DB2SQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(super.getConnectorEnvironment().getProperties());
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    } 
    
}
