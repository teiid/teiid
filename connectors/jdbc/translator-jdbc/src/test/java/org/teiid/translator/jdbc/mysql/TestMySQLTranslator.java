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

package org.teiid.translator.jdbc.mysql;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.GeometryType;
import org.teiid.query.function.GeometryUtils;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

/**
 */
public class TestMySQLTranslator {

    private static MySQLExecutionFactory TRANSLATOR;

    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
        TRANSLATOR = new MySQLExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

    private String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    private String getTestBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }

    @Test public void testConversion1() throws Exception {
        String input = "SELECT char(convert(PART_WEIGHT, integer) + 100) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char((cast(PARTS.PART_WEIGHT AS signed) + 100)) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testConversion2() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, long) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS signed) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testConversion3() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, long), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS signed) AS char) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testConversion4() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(DATE(PARTS.PART_WEIGHT), '%Y-%m-%d') FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testConversion5() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(TIME(PARTS.PART_WEIGHT), '%H:%i:%S') FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testConversion6() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(TIMESTAMP(PARTS.PART_WEIGHT), '%Y-%m-%d %H:%i:%S.%f') FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testConversion8() throws Exception {
        String input = "SELECT ifnull(PART_WEIGHT, 'otherString') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT ifnull(PARTS.PART_WEIGHT, 'otherString') FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testConversion7() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, integer), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS signed) AS char) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testInsert() throws Exception {
        String input = "SELECT insert(PART_WEIGHT, 1, 5, 'chimp') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT insert(PARTS.PART_WEIGHT, 1, 5, 'chimp') FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
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
        String output = "SELECT LOCATE(cast(SmallA.IntNum AS char), 'chimp', 1) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(cast(SmallA.IntNum AS char), '234567890', 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

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

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1, 5) FROM PARTS";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testUnionWithOrderBy() throws Exception {
        String input = "SELECT PART_ID FROM PARTS UNION SELECT PART_ID FROM PARTS ORDER BY PART_ID"; //$NON-NLS-1$
        String output = "(SELECT PARTS.PART_ID FROM PARTS) UNION (SELECT PARTS.PART_ID FROM PARTS) ORDER BY PART_ID";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 100"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 50, 100"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testBitAnd() throws Exception {
        String input = "select bitand(intkey, intnum) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast((SmallA.IntKey & SmallA.IntNum) AS signed) FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testJoins() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla inner join bqt1.smallb on smalla.stringkey=smallb.stringkey cross join bqt1.mediuma"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM (SmallA INNER JOIN SmallB ON SmallA.StringKey = SmallB.StringKey) CROSS JOIN MediumA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testTimestampLiteral() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla where smalla.timestampvalue = '2009-08-06 12:23:34.999'"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.TimestampValue = {ts '2009-08-06 12:23:34.0'}"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testDateToTimestamp() throws Exception {
        String input = "select convert(smalla.datevalue, timestamp) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast(SmallA.DateValue AS datetime) FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testWeek() throws Exception {
        String input = "select week(smalla.datevalue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT WEEKOFYEAR(SmallA.DateValue) FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testPad() throws Exception {
        String input = "select lpad(smalla.stringkey, 18), rpad(smalla.stringkey, 12) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT lpad(SmallA.StringKey, 18, ' '), rpad(SmallA.StringKey, 12, ' ') FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testChar() throws Exception {
        String input = "SELECT intkey, CHR(CONVERT(bigintegervalue, integer)) FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT MediumA.IntKey, char(cast(MediumA.BigIntegerValue AS signed)) FROM MediumA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testBooleanToString() throws Exception {
        String input = "SELECT convert(INTKEY, boolean) FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.IntKey = 0 THEN 0 WHEN SmallA.IntKey IS NOT NULL THEN 1 END FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testThreeUnionBranches() throws Exception {
        String input = "select part_id id FROM parts UNION ALL select part_name FROM parts UNION ALL select part_id FROM parts ORDER BY id"; //$NON-NLS-1$
        String output = "(SELECT PARTS.PART_ID AS id FROM PARTS) UNION ALL (SELECT PARTS.PART_NAME FROM PARTS) UNION ALL (SELECT PARTS.PART_ID FROM PARTS) ORDER BY id"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Ignore("There's no good workaround for this case on mysql 4 and for 5 can be done with a suquery, but only if the first union branch has no parens...")
    @Test public void testNestedSetQuery() throws Exception {
        String input = "select part_id id FROM parts UNION ALL (select part_name FROM parts UNION select part_id FROM parts)"; //$NON-NLS-1$
        String output = ""; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test
    public void testGeometrySelectConvert() throws Exception {
        String input = "select shape from cola_markets"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.SHAPE FROM COLA_MARKETS"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testMysqlGeometryRead() throws Exception {
        MySQLExecutionFactory msef = new MySQLExecutionFactory();
        msef.start();
        GeometryType gt = msef.toGeometryType(BlobType.createBlob(new byte[] {01,02,00,00,01,01,00,00,00,00,00,00,00,00,00,(byte)0xf0,(byte)0x3f,00,00,00,00,00,00,(byte)0xf0,0x3f }));
        assertEquals(513, gt.getSrid());
        GeometryUtils.getGeometry(gt);
    }

    @Test public void testStringUnion() throws Exception {
        String input = "select intkey FROM bqt1.smalla UNION select stringkey FROM bqt1.smallb"; //$NON-NLS-1$
        String output = "(SELECT SmallA.IntKey AS IntKey FROM SmallA) UNION (SELECT SmallB.StringKey FROM SmallB)"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);

        input = "select cast(intkey as string) FROM bqt1.smalla UNION select cast(intnum as string) FROM bqt1.smallb"; //$NON-NLS-1$
        output = "(SELECT cast(SmallA.IntKey AS char) FROM SmallA) UNION (SELECT cast(SmallB.IntNum AS char) FROM SmallB)"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testBigDecimalConversion() throws Exception {
        String input = "SELECT cast(stringkey as bigdecimal) * 2 FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ((SmallA.StringKey + 0.0) * 2) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

}
