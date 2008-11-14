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
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestExtractFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestMonthFunctionModifier.
     * @param name
     */
    public TestExtractFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(IExpression c, String expectedStr, String target) throws Exception {
        IFunction func = LANG_FACTORY.createFunction(target, 
            new IExpression[] { c },
            Integer.class);
        
        ExtractFunctionModifier mod = new ExtractFunctionModifier (target);
        IExpression expr = mod.modify(func);

        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        OracleSQLConversionVisitor sqlVisitor = new OracleSQLConversionVisitor(); 

        // register this ExtractFunctionModifier with the OracleSQLConversionVisitor
        Map modifier = trans.getFunctionModifiers();
        modifier.put("extract", mod); //$NON-NLS-1$
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
        helpTestMod(arg1, "EXTRACT(MONTH FROM {d'2004-01-21'})" , "month");   //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test2() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(MONTH FROM {ts'2004-01-21 17:05:00.0'})", "month"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test3() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "EXTRACT(YEAR FROM {d'2004-01-21'})", "year"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void test4() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(YEAR FROM {ts'2004-01-21 17:05:00.0'})", "year"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test5() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "EXTRACT(DAY FROM {d'2004-01-21'})", "day"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void test6() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(DAY FROM {ts'2004-01-21 17:05:00.0'})", "day"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void test11() throws Exception {
        IGroup group = LANG_FACTORY.createGroup(null, "group", null); //$NON-NLS-1$
        IElement elem = LANG_FACTORY.createElement("col", group, null, TypeFacility.RUNTIME_TYPES.DATE); //$NON-NLS-1$
        helpTestMod(elem, "EXTRACT(DAY FROM col)", "day"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
}

