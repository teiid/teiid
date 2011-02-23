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

package org.teiid.translator.jdbc.postgresql;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestPostgreSQLTranslator {

    private static PostgreSQLExecutionFactory TRANSLATOR; 

    @BeforeClass public static void setupOnce() throws Exception {
        TRANSLATOR = new PostgreSQLExecutionFactory(); 
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }
    
    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }
    
    private String getTestBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }
        
    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
        TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, TRANSLATOR);
    }

    @Test public void testConversion1() throws Exception {
        String input = "SELECT char(convert(PART_WEIGHT, integer) + 100) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT chr((cast(PARTS.PART_WEIGHT AS integer) + 100)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
          
    @Test public void testConversion2() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, long) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS bigint) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
          
    @Test public void testConversion3() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, short) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS smallint) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
          
    @Test public void testConversion4() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, float) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS real) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion5() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, double) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS float8) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion6() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, biginteger) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS numeric(38)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion7() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, bigdecimal) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS decimal) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion8() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, boolean) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS boolean) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion8a() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, boolean), long) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS boolean) AS integer) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion9() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, date) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS date) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion10() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, time) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS time) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion11() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS timestamp) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion12() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(PARTS.PART_WEIGHT AS time), 'HH24:MI:SS') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion13() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(PARTS.PART_WEIGHT AS timestamp), 'YYYY-MM-DD HH24:MI:SS.US') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion14() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(PARTS.PART_WEIGHT AS date), 'YYYY-MM-DD') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion15() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), date) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS timestamp) AS date) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion16() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), time) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(date_trunc('second', cast(PARTS.PART_WEIGHT AS timestamp)) AS time) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion17() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS time) + TIMESTAMP '1970-01-01' FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion18() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS date) AS timestamp) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testConversion19() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, boolean), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN cast(PARTS.PART_WEIGHT AS boolean) THEN 'true' WHEN not(cast(PARTS.PART_WEIGHT AS boolean)) THEN 'false' END FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test public void testLog() throws Exception {
        String input = "SELECT log(convert(PART_WEIGHT, double)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT ln(cast(PARTS.PART_WEIGHT AS float8)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
        input = "SELECT log10(convert(PART_WEIGHT, double)) FROM PARTS"; //$NON-NLS-1$
        output = "SELECT log(cast(PARTS.PART_WEIGHT AS float8)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test public void testLeft() throws Exception {
        String input = "SELECT left(PART_WEIGHT, 2) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT SUBSTR(PARTS.PART_WEIGHT, 1, 2) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testRight() throws Exception {
        String input = "SELECT right(PART_WEIGHT, 2) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT SUBSTR(PARTS.PART_WEIGHT, (-1 * 2)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    @Test public void testDayOfWeek() throws Exception {
        String input = "SELECT dayofweek(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (EXTRACT(DOW FROM cast(PARTS.PART_WEIGHT AS timestamp)) + 1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testDayOfMonth() throws Exception {
        String input = "SELECT dayofmonth(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(DAY FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testDayOfYear() throws Exception {
        String input = "SELECT dayofyear(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(DOY FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testHour() throws Exception {
        String input = "SELECT hour(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(HOUR FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testMinute() throws Exception {
        String input = "SELECT minute(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(MINUTE FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testMonth() throws Exception {
        String input = "SELECT month(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(MONTH FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testQuarter() throws Exception {
        String input = "SELECT quarter(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(QUARTER FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testSecond() throws Exception {
        String input = "SELECT second(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(SECOND FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testWeek() throws Exception {
        String input = "SELECT week(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(WEEK FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testYear() throws Exception {
        String input = "SELECT year(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT EXTRACT(YEAR FROM cast(PARTS.PART_WEIGHT AS timestamp)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testDayName() throws Exception {
        String input = "SELECT dayname(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT rtrim(TO_CHAR(cast(PARTS.PART_WEIGHT AS timestamp), 'Day')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testMonthName() throws Exception {
        String input = "SELECT monthname(convert(PART_WEIGHT, timestamp)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT rtrim(TO_CHAR(cast(PARTS.PART_WEIGHT AS timestamp), 'Month')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testIfnull() throws Exception {
        String input = "SELECT ifnull(PART_WEIGHT, 'otherString') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT coalesce(PARTS.PART_WEIGHT, 'otherString') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substr(PARTS.PART_WEIGHT, 1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substr(PARTS.PART_WEIGHT, 1, 5) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testBooleanAggregate() throws Exception {
        String input = "SELECT MIN(convert(PART_WEIGHT, boolean)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT bool_and(cast(PARTS.PART_WEIGHT AS boolean)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    @Test public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 100"; //$NON-NLS-1$
               
        helpTestVisitor(getTestBQTVDB(),
            input, 
            output);        
    }
    @Test public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 100 OFFSET 50"; //$NON-NLS-1$
               
        helpTestVisitor(getTestBQTVDB(),
            input, 
            output);        
    }    
    
    @Test public void testBitFunctions() throws Exception {
        String input = "select bitand(intkey, intnum), bitnot(intkey), bitor(intnum, intkey), bitxor(intnum, intkey) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT (SmallA.IntKey & SmallA.IntNum), ~(SmallA.IntKey), (SmallA.IntNum | SmallA.IntKey), (SmallA.IntNum # SmallA.IntKey) FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getTestBQTVDB(),
            input, 
            output);        
    }
    
    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate() throws Exception {
        String input = "SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT position(cast(SmallA.IntNum AS varchar(4000)) in 'chimp') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate2() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT position(SmallA.StringNum in 'chimp') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate3() throws Exception {
        String input = "SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26"; //$NON-NLS-1$
        String output = "SELECT position(cast(SmallA.IntNum AS varchar(4000)) in '234567890') FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate4() throws Exception {
        String input = "SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT 1 FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate5() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT position(SmallA.StringNum in 'chimp') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testLocate5a() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', 2) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT (position(SmallA.StringNum in substr('chimp', 2)) + 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate6() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT (position(SmallA.StringNum in substr('chimp', CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END)) + CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END - 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate7() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT (position(SmallA.StringNum in substr('chimp', CASE WHEN (position(SmallA.StringNum in 'chimp') + 1) < 1 THEN 1 ELSE (position(SmallA.StringNum in 'chimp') + 1) END)) + CASE WHEN (position(SmallA.StringNum in 'chimp') + 1) < 1 THEN 1 ELSE (position(SmallA.StringNum in 'chimp') + 1) END - 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testAggregate() throws Exception {
        String input = "SELECT count(*), max(booleanvalue), max(intnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT COUNT(*), bool_or(SmallA.BooleanValue), MAX(SmallA.IntNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testArrayFunctions() throws Exception {
        String input = "SELECT array_get(objectvalue, 3), array_length(objectvalue) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT SmallA.ObjectValue[3], array_length(SmallA.ObjectValue, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
}
