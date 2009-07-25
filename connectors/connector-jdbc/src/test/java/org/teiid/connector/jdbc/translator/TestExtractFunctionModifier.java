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

package org.teiid.connector.jdbc.translator;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
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
            Arrays.asList(c),
            Integer.class);
        
        ExtractFunctionModifier mod = new ExtractFunctionModifier ();
        IExpression expr = mod.modify(func);
        Translator trans = new Translator();
        trans.registerFunctionModifier(target, mod);
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 

        sqlVisitor.append(expr);  
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
        helpTestMod(arg1, "EXTRACT(DAY FROM {d'2004-01-21'})", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void test6() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(DAY FROM {ts'2004-01-21 17:05:00.0'})", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void test11() throws Exception {
        IGroup group = LANG_FACTORY.createGroup(null, "group", null); //$NON-NLS-1$
        IElement elem = LANG_FACTORY.createElement("col", group, null, TypeFacility.RUNTIME_TYPES.DATE); //$NON-NLS-1$
        helpTestMod(elem, "EXTRACT(DAY FROM col)", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test12() throws Exception {
        IGroup group = LANG_FACTORY.createGroup(null, "group", null); //$NON-NLS-1$
        IElement elem = LANG_FACTORY.createElement("col", group, null, TypeFacility.RUNTIME_TYPES.DATE); //$NON-NLS-1$
        helpTestMod(elem, "(EXTRACT(DOW FROM col) + 1)", SourceSystemFunctions.DAYOFWEEK); //$NON-NLS-1$
    }
    
}

