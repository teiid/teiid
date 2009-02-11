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

package com.metamatrix.connector.jdbc.postgresql;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.connector.jdbc.oracle.MonthOrDayNameFunctionModifier;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.3
 */
public class PostgreSQLTranslator extends BasicSQLTranslator {

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

    /** 
     * @param modifier
     * @since 4.2
     */
    private void initializeFunctionModifiers() {
        functionModifiers = new HashMap();
        functionModifiers.putAll(super.getFunctionModifiers());
        functionModifiers.put("log", new AliasModifier("ln")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("log10", new AliasModifier("log")); //$NON-NLS-1$ //$NON-NLS-2$
        
        functionModifiers.put("char", new AliasModifier("chr")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("concat", new AliasModifier("||")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("lcase", new AliasModifier("lower")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("left", new SubstringFunctionModifier(languageFactory, true));//$NON-NLS-1$ 
        functionModifiers.put("right", new SubstringFunctionModifier(languageFactory, false));//$NON-NLS-1$ 
        functionModifiers.put("substring", new AliasModifier("substr")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("ucase", new AliasModifier("upper")); //$NON-NLS-1$ //$NON-NLS-2$
        
        functionModifiers.put("dayname", new MonthOrDayNameFunctionModifier(languageFactory, "Day"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofweek", new ModifiedDatePartFunctionModifier(languageFactory, "dow", "+", new Integer(1)));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        functionModifiers.put("dayofmonth", new DatePartFunctionModifier(languageFactory, "day"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("dayofyear", new DatePartFunctionModifier(languageFactory, "doy"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("hour", new DatePartFunctionModifier(languageFactory, "hour"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("minute", new DatePartFunctionModifier(languageFactory, "minute"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("month", new DatePartFunctionModifier(languageFactory, "month"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("monthname", new MonthOrDayNameFunctionModifier(languageFactory, "Month"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("quarter", new DatePartFunctionModifier(languageFactory, "quarter"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("second", new DatePartFunctionModifier(languageFactory, "second"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("week", new DatePartFunctionModifier(languageFactory, "week"));//$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("year", new DatePartFunctionModifier(languageFactory, "year"));//$NON-NLS-1$ //$NON-NLS-2$
        
        functionModifiers.put("ifnull", new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("nvl", new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        functionModifiers.put("convert", new PostgreSQLConvertModifier(languageFactory)); //$NON-NLS-1$
        functionModifiers.put("cast", new PostgreSQLConvertModifier(languageFactory)); //$NON-NLS-1$
    }    
    

    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new PostgreSQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(functionModifiers);
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }    
 
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return functionModifiers;
    }

}
