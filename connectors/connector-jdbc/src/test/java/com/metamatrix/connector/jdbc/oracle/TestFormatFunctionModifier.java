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

import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestFormatFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestMonthFunctionModifier.
     * @param name
     */
    public TestFormatFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(ILiteral datetime, ILiteral format, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("format",  //$NON-NLS-1$
            new IExpression[] { datetime, format },
            String.class);
        
        FormatFunctionModifier mod = new FormatFunctionModifier (LANG_FACTORY);
        IExpression expr = mod.modify(func);

        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        OracleSQLConversionVisitor sqlVisitor = new OracleSQLConversionVisitor(); 

        // register this ExtractFunctionModifier with the OracleSQLConversionVisitor
        Map modifier = trans.getFunctionModifiers();
        modifier.put("format", mod); //$NON-NLS-1$
        sqlVisitor.setFunctionModifiers(modifier);

        //sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);  
        //System.out.println(" expected: " + expectedStr + " \t actual: " + sqlVisitor.toString());
        assertEquals(expectedStr, sqlVisitor.toString());
        
        return expr;
    }
    public void test1() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        ILiteral arg2 = LANG_FACTORY.createLiteral("YYYY-MM-DD", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, "to_char({d'2004-01-21'}, 'YYYY-MM-DD')" );   //$NON-NLS-1$
    }
    
    public void test2() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        ILiteral arg2 = LANG_FACTORY.createLiteral("YYYY-MM-DD HH24:MI:SS.fffffffff", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, "to_char({ts'2004-01-21 17:05:00.0'}, 'YYYY-MM-DD HH24:MI:SS.fffffffff')"); //$NON-NLS-1$ 
    }
    
    public void test3() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTime(12, 1, 21), java.sql.Time.class);
        ILiteral arg2 = LANG_FACTORY.createLiteral("HH24:MI:SS", String.class); //$NON-NLS-1$
        helpTestMod(arg1, arg2, "to_char({ts'1970-01-01 12:01:21'}, 'HH24:MI:SS')"); //$NON-NLS-1$ 
    }

}

