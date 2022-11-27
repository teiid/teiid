/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc.sybase;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory.Format;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;
import org.teiid.util.Version;

/**
 */
@SuppressWarnings("nls")
public class TestSybaseSQLConversionVisitor {

    private static SybaseExecutionFactory trans = new SybaseExecutionFactory();

    @BeforeClass
    public static void setup() throws TranslatorException {
        trans.setUseBindVariables(false);
        trans.setDatabaseVersion(Version.DEFAULT_VERSION);
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
    public void testProjectedCriteria() {
        String input = "SELECT part_name like '%b' FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN PARTS.PART_NAME LIKE '%b' THEN 1 WHEN NOT (PARTS.PART_NAME LIKE '%b') THEN 0 END FROM PARTS"; //$NON-NLS-1$

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
        String output = "SELECT ((cast(SmallA.IntKey AS numeric(38, 19)) / 1.5) - (sign((cast(SmallA.IntKey AS numeric(38, 19)) / 1.5)) * floor(abs(((cast(SmallA.IntKey AS numeric(38, 19)) / 1.5) / 3))) * abs(3))) FROM SmallA"; //$NON-NLS-1$

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

    @Test public void testTimestampFunctions() {
        helpTestVisitor(getBQTVDB(),
            "SELECT timestampadd(sql_tsi_second, 1, timestampvalue), timestampadd(sql_tsi_frac_second, 1000, timestampvalue), timestampdiff(sql_tsi_frac_second, timestampvalue, timestampvalue) from bqt1.smalla", //$NON-NLS-1$
            "SELECT {fn timestampadd(sql_tsi_second, 1, SmallA.TimestampValue)}, dateadd(millisecond, 1000/1000000, SmallA.TimestampValue), datediff(millisecond, SmallA.TimestampValue,SmallA.TimestampValue)*1000000 FROM SmallA"); //$NON-NLS-1$
    }

    @Test public void testLimitSupport() {
        SybaseExecutionFactory sybaseExecutionFactory = new SybaseExecutionFactory();
        sybaseExecutionFactory.setDatabaseVersion("12.5.4");
        assertTrue(sybaseExecutionFactory.supportsRowLimit());
        sybaseExecutionFactory.setDatabaseVersion("12.5.2");
        assertFalse(sybaseExecutionFactory.supportsRowLimit());
        sybaseExecutionFactory.setDatabaseVersion("15");
        assertFalse(sybaseExecutionFactory.supportsRowLimit());
        sybaseExecutionFactory.setDatabaseVersion("15.1");
        assertTrue(sybaseExecutionFactory.supportsRowLimit());
    }

    @Test public void testFormatSupport() {
        assertTrue(trans.supportsFormatLiteral("MM/dd/yy", Format.DATE));
        assertFalse(trans.supportsFormatLiteral("MMM/yyy", Format.DATE));

        helpTestVisitor(getBQTVDB(),
                "SELECT formattimestamp(timestampvalue, 'yy.MM.dd') from bqt1.smalla", //$NON-NLS-1$
                "SELECT CONVERT(VARCHAR, SmallA.TimestampValue, 2) FROM SmallA"); //$NON-NLS-1$
    }

    @Test public void testGroupBySelect() {
        helpTestVisitor(getBQTVDB(),
                "SELECT 1 from bqt1.smalla group by intkey", //$NON-NLS-1$
                "SELECT SmallA.IntKey FROM SmallA GROUP BY SmallA.IntKey"); //$NON-NLS-1$
    }

    @Test public void testBinaryLiteral() {
        helpTestVisitor(getBQTVDB(),
                "SELECT X'abcd1234'", //$NON-NLS-1$
                "SELECT 0xABCD1234"); //$NON-NLS-1$
    }

    @Test
    public void testPadFunctions() {
        String input = "SELECT lpad(PART_NAME,2), lpad(part_name,2,'x'), rpad(PART_NAME,2), rpad(part_name,2,'x') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN PARTS.PART_NAME IS NULL THEN NULL ELSE RIGHT(REPLICATE(' ', 2) + LEFT(PARTS.PART_NAME, 2), 2) END, CASE WHEN PARTS.PART_NAME IS NULL OR 'x' IS NULL THEN NULL ELSE RIGHT(REPLICATE('x', 2) + LEFT(PARTS.PART_NAME, 2), 2) END, CASE WHEN PARTS.PART_NAME IS NULL THEN NULL ELSE LEFT(PARTS.PART_NAME + REPLICATE(' ', 2), 2) END, CASE WHEN PARTS.PART_NAME IS NULL OR 'x' IS NULL THEN NULL ELSE LEFT(PARTS.PART_NAME + REPLICATE('x', 2), 2) END FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

}
