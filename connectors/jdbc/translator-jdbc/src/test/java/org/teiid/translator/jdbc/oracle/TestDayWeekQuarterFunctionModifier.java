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

package org.teiid.translator.jdbc.oracle;

import java.sql.Timestamp;
import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
public class TestDayWeekQuarterFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestHourFunctionModifier.
     * @param name
     */
    public TestDayWeekQuarterFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Literal c, String format, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction(format,  
            Arrays.asList(c),
            String.class);
        
        OracleExecutionFactory trans = new OracleExecutionFactory();
        trans.start();
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(func);  
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    public void test1() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFYEAR, 
            "to_number(TO_CHAR({ts '2004-01-21 10:05:00.01'}, 'DDD'))"); //$NON-NLS-1$
    }

    public void test2() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, SourceSystemFunctions.DAYOFYEAR, 
            "to_number(TO_CHAR({d '2004-01-21'}, 'DDD'))"); //$NON-NLS-1$
    }
    
    public void test9() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, SourceSystemFunctions.QUARTER,
            "to_number(TO_CHAR({ts '2004-01-21 10:05:00.01'}, 'Q'))"); //$NON-NLS-1$
    }

    public void test10() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, SourceSystemFunctions.QUARTER, 
            "to_number(TO_CHAR({d '2004-01-21'}, 'Q'))"); //$NON-NLS-1$
    }
}