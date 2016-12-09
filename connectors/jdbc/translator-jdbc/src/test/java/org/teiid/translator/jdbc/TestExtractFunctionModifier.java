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

package org.teiid.translator.jdbc;

import java.sql.Timestamp;
import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;


/**
 */
public class TestExtractFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestMonthFunctionModifier.
     * @param name
     */
    public TestExtractFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Expression c, String expectedStr, String target) throws Exception {
        Function func = LANG_FACTORY.createFunction(target, 
            Arrays.asList(c),
            Integer.class);
        
        ExtractFunctionModifier mod = new ExtractFunctionModifier ();
        JDBCExecutionFactory trans = new JDBCExecutionFactory();
        trans.registerFunctionModifier(target, mod);
        trans.start();
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 

        sqlVisitor.append(func);  
        assertEquals(expectedStr, sqlVisitor.toString());
    }
    public void test1() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "EXTRACT(MONTH FROM {d '2004-01-21'})" , "month");   //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test2() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(MONTH FROM {ts '2004-01-21 17:05:00.0'})", "month"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test3() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "EXTRACT(YEAR FROM {d '2004-01-21'})", "year"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void test4() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(YEAR FROM {ts '2004-01-21 17:05:00.0'})", "year"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test5() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "EXTRACT(DAY FROM {d '2004-01-21'})", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void test6() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 17, 5, 0, 0), Timestamp.class);
        helpTestMod(arg1, "EXTRACT(DAY FROM {ts '2004-01-21 17:05:00.0'})", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    public void test11() throws Exception {
        NamedTable group = LANG_FACTORY.createNamedTable("group", null, null); //$NON-NLS-1$
        ColumnReference elem = LANG_FACTORY.createColumnReference("col", group, null, TypeFacility.RUNTIME_TYPES.DATE); //$NON-NLS-1$
        helpTestMod(elem, "EXTRACT(DAY FROM group.col)", "dayofmonth"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test12() throws Exception {
        NamedTable group = LANG_FACTORY.createNamedTable("group", null, null); //$NON-NLS-1$
        ColumnReference elem = LANG_FACTORY.createColumnReference("col", group, null, TypeFacility.RUNTIME_TYPES.DATE); //$NON-NLS-1$
        helpTestMod(elem, "(EXTRACT(DOW FROM group.col) + 1)", SourceSystemFunctions.DAYOFWEEK); //$NON-NLS-1$
    }
    
}

