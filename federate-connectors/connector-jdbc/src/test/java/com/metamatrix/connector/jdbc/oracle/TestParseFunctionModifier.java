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

package com.metamatrix.connector.jdbc.oracle;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.language.ILiteral;

/**
 */
public class TestParseFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestMonthFunctionModifier.
     * @param name
     */
    public TestParseFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(ILiteral datetime, ILiteral format, Class targetClass, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("parse",  //$NON-NLS-1$
            new IExpression[] { datetime, format },
            targetClass);
        
        ParseFunctionModifier mod = new ParseFunctionModifier (LANG_FACTORY, targetClass);
        IExpression expr = mod.modify(func);

        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        OracleSQLConversionVisitor sqlVisitor = new OracleSQLConversionVisitor(); 

        // register this ExtractFunctionModifier with the OracleSQLConversionVisitor
        Map modifier = trans.getFunctionModifiers();
        modifier.put("parse", mod); //$NON-NLS-1$
        sqlVisitor.setFunctionModifiers(modifier);

        //sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);  
        //System.out.println(" expected: " + expectedStr + " \t actual: " + sqlVisitor.toString());
        assertEquals(expectedStr, sqlVisitor.toString());
        
        return expr;
    }
    public void test1() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral("2004-01-21", String.class); //$NON-NLS-1$
        ILiteral arg2 = LANG_FACTORY.createLiteral("YYYY-MM-DD", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, java.sql.Date.class, "to_date('2004-01-21', 'YYYY-MM-DD')" );   //$NON-NLS-1$ 
    }
    
    public void test2() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral("2004-01-21 17:05:00.0", String.class); //$NON-NLS-1$
        ILiteral arg2 = LANG_FACTORY.createLiteral("YYYY-MM-DD HH24:MI:SS.fffffffff", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, java.sql.Timestamp.class, "to_date('2004-01-21 17:05:00.0', 'YYYY-MM-DD HH24:MI:SS.fffffffff')"); //$NON-NLS-1$ 
    }
    
    public void test3() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral("12:01:21", String.class); //$NON-NLS-1$
        ILiteral arg2 = LANG_FACTORY.createLiteral("HH24:MI:SS", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, java.sql.Time.class, "to_date('12:01:21', 'HH24:MI:SS')"); //$NON-NLS-1$ 
    }

}


