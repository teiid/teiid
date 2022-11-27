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

package org.teiid.translator.jdbc.db2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestDB2SqlTranslator {

    private static DB2ExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new DB2ExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/PartsSupplierJDBC.vdb"; //$NON-NLS-1$
    }

    public void helpTestVisitor(TranslationUtility util, String input, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        Command obj = util.parseCommand(input);

        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), TRANSLATOR);
        tc.translateCommand(obj);

        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA FETCH FIRST 100 ROWS ONLY";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input,
            output);
    }

    @Test
    public void testCrossJoin() throws Exception{
        String input = "SELECT bqt1.smalla.stringkey FROM bqt1.smalla cross join bqt1.smallb"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA INNER JOIN SmallB ON 1 = 1";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input,
            output);
    }

    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(coalesce(SmallA.StringNum, ''), '_xx') FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input,
                output);

    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL AND SmallA.StringNum IS NULL THEN NULL ELSE concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, '')) END FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input,
                output);
    }

    @Test
    public void testSelectNullLiteral() throws Exception {
        String input = "select null + 1 as x, null || 'a', char(null) from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT cast(NULL AS integer) AS x, cast(NULL AS varchar), cast(NULL AS char(1)) FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input,
                output);
    }

    @Test
    public void testSelectNullLiteral1() throws Exception {
        String input = "select x, intkey from (select null as x, intkey from BQT1.Smalla) y "; //$NON-NLS-1$
        String output = "SELECT y.x, y.intkey FROM (SELECT cast(NULL AS integer) AS x, SmallA.IntKey FROM SmallA) AS y";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
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
        String output = "SELECT LOCATE(varchar(SmallA.IntNum), 'chimp', 1) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp') FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(varchar(SmallA.IntNum), '234567890', 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', 1) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', CASE WHEN (LOCATE(SmallA.StringNum, 'chimp') + 1) < 1 THEN 1 ELSE (LOCATE(SmallA.StringNum, 'chimp') + 1) END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testBooleanToString() throws Exception {
        String input = "SELECT convert(convert(INTKEY, boolean), string) FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN CASE WHEN SmallA.IntKey = 0 THEN 0 WHEN SmallA.IntKey IS NOT NULL THEN 1 END = 0 THEN 'false' WHEN CASE WHEN SmallA.IntKey = 0 THEN 0 WHEN SmallA.IntKey IS NOT NULL THEN 1 END IS NOT NULL THEN 'true' END FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testSubstring() throws Exception {
        String input = "SELECT substring(STRINGNUM, 12, 10) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, CASE WHEN 12 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 12 END, CASE WHEN 10 > (length(SmallA.StringNum) - (CASE WHEN 12 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 12 END - 1)) THEN (length(SmallA.StringNum) - (CASE WHEN 12 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 12 END - 1)) ELSE 10 END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(STRINGNUM, 2, intnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, CASE WHEN 2 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 2 END, CASE WHEN SmallA.IntNum > (length(SmallA.StringNum) - (CASE WHEN 2 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 2 END - 1)) THEN (length(SmallA.StringNum) - (CASE WHEN 2 > length(SmallA.StringNum) THEN (length(SmallA.StringNum) + 1) ELSE 2 END - 1)) WHEN SmallA.IntNum > 0 THEN SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(STRINGNUM, 2, -1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT cast(NULL AS varchar) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testTrim() throws Exception {
        String input = "SELECT trim(leading 'x' from stringnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT STRIP(SmallA.StringNum, leading, 'x') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testDB2ForI() throws Exception {
        DB2ExecutionFactory db2 = new DB2ExecutionFactory();
        db2.setdB2ForI(true);
        db2.setDatabaseVersion(Version.DEFAULT_VERSION);
        assertFalse(db2.supportsFunctionsInGroupBy());
        assertFalse(db2.supportsElementaryOlapOperations());
        db2.setDatabaseVersion(DB2ExecutionFactory.SIX_1.toString());
        assertTrue(db2.supportsElementaryOlapOperations());
    }

    @Test public void testTempTable() throws Exception {
        assertEquals("declare global temporary table foo (COL1 integer, COL2 varchar(100)) not logged", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }

    @Test public void testRight() throws Exception {
        String input = "SELECT right(intkey, 2) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT right(varchar(SmallA.IntKey), 2) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testWeek() throws Exception {
        String input = "SELECT week(datevalue) FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT WEEK_ISO(MediumA.DateValue) FROM MediumA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
