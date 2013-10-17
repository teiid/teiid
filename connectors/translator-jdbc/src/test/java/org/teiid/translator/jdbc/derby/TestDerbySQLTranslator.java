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

package org.teiid.translator.jdbc.derby;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestDerbySQLTranslator {

    private static DerbyExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new DerbyExecutionFactory();        
        TRANSLATOR.start();
    }
    
    @Test
    public void testConcat_useLiteral() throws Exception {
        String input = "select concat(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(SmallA.StringNum, '_xx')} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat() throws Exception {
        String input = "select concat(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT {fn concat(SmallA.StringNum, SmallA.StringNum)} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }    
    
    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(coalesce(SmallA.StringNum, ''), '_xx')} FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL AND SmallA.StringNum IS NULL THEN NULL ELSE {fn concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, ''))} END FROM SmallA";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    
    @Test
    public void testSelectWithNoFrom() throws Exception {
        String input = "select 1, 2"; //$NON-NLS-1$       
        String output = "VALUES(1, 2)";  //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }    
    
    @Test public void testTempTable() throws Exception {
    	assertEquals("declare global temporary table foo (COL1 integer, COL2 varchar(100)) not logged", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }
    
    @Test public void testXmlSelect() throws Exception {
        String input = "SELECT col as x, col1 as y from test"; //$NON-NLS-1$
        String output = "SELECT XMLSERIALIZE(test.col AS CLOB) AS x, test.col1 AS y FROM test";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table test (col xml, col1 integer);",
                input, output, 
                TRANSLATOR);
        
        input = "SELECT * from test"; //$NON-NLS-1$
        output = "SELECT XMLSERIALIZE(test.col AS CLOB), test.col1 FROM test";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table test (col xml, col1 integer);",
                input, output, 
                TRANSLATOR);
    }
    
}
