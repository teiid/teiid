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

package org.teiid.translator.jdbc.sybase;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;

/**
 */
public class TestSybaseSQLConversionVisitor {

    private static SybaseExecutionFactory trans = new SybaseExecutionFactory();
    
    @BeforeClass
    public static void setup() throws TranslatorException {
    	trans.setUseBindVariables(false);
        trans.start();
    }

    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public String getBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        // Convert from sql to objects
        Command obj = TranslationHelper.helpTranslate(vdb, input);
        
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), trans);
		try {
			tc.translateCommand(obj);
		} catch (TranslatorException e) {
			throw new RuntimeException(e);
		}
        
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testModFunction() {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (cast(PARTS.PART_ID AS int) % 13) FROM PARTS";  //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    } 

    @Test
    public void testConcatFunction() {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN PARTS.PART_NAME IS NULL THEN NULL ELSE (PARTS.PART_NAME + 'b') END FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }    

    @Test
    public void testLcaseFunction() {
        String input = "SELECT lcase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT lower(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test
    public void testUcaseFunction() {
        String input = "SELECT ucase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT upper(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
     
    @Test
    public void testLengthFunction() {
        String input = "SELECT length(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT {fn length(PARTS.PART_NAME)} FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    @Test
    public void testSubstring2ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, {fn length(PARTS.PART_NAME)}) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    @Test
    public void testSubstring3ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test
    public void testConvertFunctionInteger() {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_ID AS int) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test public void testConvertTimestampTime() {
        String input = "SELECT convert(TIMESTAMPVALUE, time) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT cast(CASE WHEN SmallA.TimestampValue IS NOT NULL THEN '1970-01-01 ' + convert(varchar, SmallA.TimestampValue, 8) END AS datetime) FROM SmallA"; //$NON-NLS-1$
    
        helpTestVisitor(getBQTVDB(),
            input, 
            output);
    }
    
    @Test
    public void testConvertFunctionChar() {
        String input = "SELECT convert(PART_NAME, char) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_NAME AS char(1)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test
    public void testConvertFunctionBoolean() {
        String input = "SELECT convert(PART_ID, boolean) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN PARTS.PART_ID IN ('false', '0') THEN 0 WHEN PARTS.PART_ID IS NOT NULL THEN 1 END FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    @Test
    public void testIfNullFunction() {
        String input = "SELECT ifnull(PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT isnull(PARTS.PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }    

    @Test
    public void testDateLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {d '2002-12-31'} FROM parts", //$NON-NLS-1$
            "SELECT CAST('2002-12-31' AS DATE) FROM PARTS"); //$NON-NLS-1$
    }

    @Test
    public void testTimeLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {t '13:59:59'} FROM parts", //$NON-NLS-1$
            "SELECT CAST('1970-01-01 13:59:59.0' AS DATETIME) FROM PARTS"); //$NON-NLS-1$
    }

    @Test
    public void testTimestampLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {ts '2002-12-31 13:59:59'} FROM parts", //$NON-NLS-1$
            "SELECT CAST('2002-12-31 13:59:59.0' AS DATETIME) FROM PARTS"); //$NON-NLS-1$
    }
    
    @Test
    public void testDefect12120() {
        helpTestVisitor(getBQTVDB(),
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.BooleanValue IN ({b'false'}, {b'true'})", //$NON-NLS-1$
            "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.BooleanValue IN (0, 1)"); //$NON-NLS-1$
                
    }
    
    @Test
    public void testConvertFunctionString() throws Exception {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_ID AS int) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
     
    @Test
    public void testNonIntMod() throws Exception {
    	String input = "select mod(intkey/1.5, 3) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ((cast(SmallA.IntKey AS double precision) / 1.5) - (sign((cast(SmallA.IntKey AS double precision) / 1.5)) * floor(abs(((cast(SmallA.IntKey AS double precision) / 1.5) / 3.0))) * abs(3.0))) FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);
    }
    
    @Test public void testOrderByDesc() throws Exception {
    	String input = "select intnum + 1 x from bqt1.smalla order by x desc"; //$NON-NLS-1$
        String output = "SELECT (SmallA.IntNum + 1) AS x FROM SmallA ORDER BY x DESC"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);
    }
    
    @Test public void testCrossJoin() throws Exception {
    	String input = "select smalla.intnum from (bqt1.smalla cross join bqt1.smallb) left outer join bqt1.mediuma on (smalla.intnum = mediuma.intnum)"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntNum FROM SmallA INNER JOIN SmallB ON 1 = 1 LEFT OUTER JOIN MediumA ON SmallA.IntNum = MediumA.IntNum"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(), input, output);
    }


}
