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
package org.teiid.translator.jdbc.redshift;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.ExecutionFactory.Format;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestRedshiftTranslator {

    private static RedshiftExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new RedshiftExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.setDatabaseVersion(RedshiftExecutionFactory.NINE_3);
        TRANSLATOR.start();
    }
    
    @Test public void testLocate() throws Exception {
        String input = "SELECT INTKEY, STRINGKEY FROM bqt1.SmallA WHERE LOCATE('1', STRINGKEY, 2) IN (1, 2)"; 
        String output = "SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA WHERE (position('1' in substring(SmallA.StringKey, 2)) + 1) IN (1, 2)"; 

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    
    @Test public void testParseDate() throws Exception {
        String input = "SELECT INTKEY FROM bqt1.SmallA WHERE parsedate(stringkey, 'yyyy-MM dd') = {d '1999-12-01'}"; 
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE cast(TO_DATE(SmallA.StringKey, 'YYYY-MM DD') AS date) = DATE '1999-12-01'"; 

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    
    @Test public void testBigDecimalCast() throws Exception {
        String input = "SELECT cast(floatnum as bigdecimal) FROM bqt1.SmallA"; 
        String output = "SELECT cast(SmallA.FloatNum AS decimal(38, 19)) FROM SmallA"; 

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    
    @Test public void testTimezoneFormat() throws Exception {
    	assertFalse(TRANSLATOR.supportsFormatLiteral("hh:MM:ss Z", Format.DATE));
    }
    
    @Test public void testTempTable() throws Exception {
        assertEquals("create temporary table foo (COL1 int4, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
        assertEquals("create temporary table foo (COL1 int4, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, false));
    }

}
