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

package org.teiid.connector.jdbc.oracle;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

import org.teiid.connector.jdbc.oracle.MonthOrDayNameFunctionModifier;
import org.teiid.connector.jdbc.oracle.OracleSQLTranslator;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestMonthOrDayNameFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestHourFunctionModifier.
     * @param name
     */
    public TestMonthOrDayNameFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(ILiteral c, String format, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction(format.toLowerCase()+"name",  // "monthname" //$NON-NLS-1$ 
            Arrays.asList( c ),
            String.class);
        
        MonthOrDayNameFunctionModifier mod = new MonthOrDayNameFunctionModifier (LANG_FACTORY, format);
        IExpression expr = mod.modify(func);
        
        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        assertEquals(expectedStr, sqlVisitor.toString());
        return expr;
    }

    public void test1() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "Month", //$NON-NLS-1$
            "RTRIM(TO_CHAR({ts'2004-01-21 10:05:00.0'}, 'Month'))"); //$NON-NLS-1$
    }

    public void test2() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "Month", //$NON-NLS-1$
            "RTRIM(TO_CHAR({d'2004-01-21'}, 'Month'))"); //$NON-NLS-1$
    }
    
    public void test3() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "Day",  //$NON-NLS-1$
            "RTRIM(TO_CHAR({ts'2004-01-21 10:05:00.0'}, 'Day'))"); //$NON-NLS-1$
    }

    public void test4() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "Day", //$NON-NLS-1$
            "RTRIM(TO_CHAR({d'2004-01-21'}, 'Day'))"); //$NON-NLS-1$
    }
}