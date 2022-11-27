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

package org.teiid.translator.jdbc.hana;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestHanaTranslator {

    private static HanaExecutionFactory TRANSLATOR;

    @BeforeClass public static void setupOnce() throws Exception {
        TRANSLATOR = new HanaExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.setDatabaseVersion(Version.DEFAULT_VERSION);
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
        String output = "SELECT to_nvarchar((cast(PARTS.PART_WEIGHT AS integer) + 100)) FROM PARTS";  //$NON-NLS-1$

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
        String output = "SELECT cast(PARTS.PART_WEIGHT AS float) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion5() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, double) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS double) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion6() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, biginteger) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(PARTS.PART_WEIGHT AS bigint) FROM PARTS";  //$NON-NLS-1$

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
        String output = "SELECT cast(cast(PARTS.PART_WEIGHT AS boolean) AS tinyint) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion9() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, date) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion10() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, time) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion11() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion12() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS') AS nvarchar) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion13() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS') AS nvarchar) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testConversion14() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD') AS nvarchar) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testConversion15() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), date) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS') AS date) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testConversion16() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), time) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS') AS time) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testConversion17() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS') AS timestamp) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testConversion18() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), timestamp) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD') AS timestamp) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testLog() throws Exception {
        String input = "SELECT log(convert(PART_WEIGHT, double)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT log(10, cast(PARTS.PART_WEIGHT AS double)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
        input = "SELECT log10(convert(PART_WEIGHT, double)) FROM PARTS"; //$NON-NLS-1$
        output = "SELECT log(10, cast(PARTS.PART_WEIGHT AS double)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testLeft() throws Exception {
        String input = "SELECT left(PART_WEIGHT, 2) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT left(PARTS.PART_WEIGHT, 2) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testRight() throws Exception {
        String input = "SELECT right(PART_WEIGHT, 2) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT right(PARTS.PART_WEIGHT, 2) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }

    @Test public void testDayOfWeek() throws Exception {
        String input = "SELECT dayofweek(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (MOD((WEEKDAY(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'))+1),7)+1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testDayOfMonth() throws Exception {
        String input = "SELECT dayofmonth(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT dayofmonth(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testDayOfYear() throws Exception {
        String input = "SELECT dayofyear(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT dayofyear(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testHour() throws Exception {
        String input = "SELECT hour(convert(PART_WEIGHT, time)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT hour(to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testMinute() throws Exception {
        String input = "SELECT minute(convert(PART_WEIGHT, time)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT minute(to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testMonth() throws Exception {
        String input = "SELECT month(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT month(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testQuarter() throws Exception {
        String input = "SELECT quarter(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT ((month(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'))+2)/3) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testSecond() throws Exception {
        String input = "SELECT second(convert(PART_WEIGHT, time)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT second(to_time(PARTS.PART_WEIGHT, 'HH24:MI:SS')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testWeek() throws Exception {
        String input = "SELECT week(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cast(substring(isoweek(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')), 7, 2) as integer) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testYear() throws Exception {
        String input = "SELECT year(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT year(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testDayName() throws Exception {
        String input = "SELECT dayname(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT initcap(lower(dayname(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')))) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testMonthName() throws Exception {
        String input = "SELECT monthname(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT monthname(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testIfnull() throws Exception {
        String input = "SELECT ifnull(PART_WEIGHT, 'otherString') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT ifnull(PARTS.PART_WEIGHT, 'otherString') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input,
            output);
    }
    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1, 5) FROM PARTS";  //$NON-NLS-1$

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
        String output = "SELECT bitand(SmallA.IntKey, SmallA.IntNum), bitnot(SmallA.IntKey), bitor(SmallA.IntNum, SmallA.IntKey), bitxor(SmallA.IntNum, SmallA.IntKey) FROM SmallA"; //$NON-NLS-1$

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
        String output = "SELECT locate(substring('chimp', 1), cast(SmallA.IntNum AS nvarchar)) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT locate('chimp', SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT locate(substring('234567890', 1), cast(SmallA.IntNum AS nvarchar)) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

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
        String input = "SELECT locate('h', 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT 2 FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT locate(substring('chimp', 1), SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testLocate5a() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', 2) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT locate(substring('chimp', 2), SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT locate(substring('chimp', CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END), SmallA.StringNum) FROM SmallA"; //$NON-NLS-1$

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
        String output = "SELECT locate(substring('chimp', CASE WHEN (locate('chimp', SmallA.StringNum) + 1) < 1 THEN 1 ELSE (locate('chimp', SmallA.StringNum) + 1) END), SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testAggregate() throws Exception {
        String input = "SELECT count(*), min(intnum), max(intnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT COUNT(*), MIN(SmallA.IntNum), MAX(SmallA.IntNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    /*
     * Geospatial tests
     */
    @Test
    public void testSTDistance() throws Exception {
        String input = "select ST_Distance(shape, shape) from cola_markets"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.SHAPE.st_distance(COLA_MARKETS.SHAPE) FROM COLA_MARKETS"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTDisjoint() throws Exception {
        String input = "select ST_Disjoint(shape, shape) from cola_markets"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.SHAPE.st_disjoint(COLA_MARKETS.SHAPE) FROM COLA_MARKETS"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTIntersects() throws Exception {
        String input = "select ST_Intersects(shape, shape) from cola_markets"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.SHAPE.st_intersects(COLA_MARKETS.SHAPE) FROM COLA_MARKETS"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTInsert() throws Exception {
        String input = "insert into cola_markets(name,shape) values('foo124', ST_GeomFromText('POINT (300 100)', 8307))"; //$NON-NLS-1$
        String output = "INSERT INTO COLA_MARKETS (NAME, SHAPE) VALUES ('foo124', st_geomfromwkb(?, 8307))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTInsertNull() throws Exception {
        String input = "insert into cola_markets(name,shape) values('foo124', null)"; //$NON-NLS-1$
        String output = "INSERT INTO COLA_MARKETS (NAME, SHAPE) VALUES ('foo124', ?)"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTSrid() throws Exception {
        String input = "select st_srid(shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_srid() FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTAsBinary() throws Exception {
        String input = "select st_asbinary(shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_asbinary() FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTAsWKT() throws Exception {
        String input = "select st_asewkt(shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_asewkt() FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTAsText() throws Exception {
        String input = "select st_astext(shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_astext() FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTAsGeoJSON() throws Exception {
        String input = "select st_asgeojson(shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_asgeojson() FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTContains() throws Exception {
        String input = "select st_contains(shape, shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_contains(c.SHAPE) FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTCrosses() throws Exception {
        String input = "select st_crosses(shape, shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_crosses(c.SHAPE) FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTEquals() throws Exception {
        String input = "select st_equals(shape, shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_equals(c.SHAPE) FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSTTouches() throws Exception {
        String input = "select st_touches(shape, shape) from cola_markets c"; //$NON-NLS-1$
        String output = "SELECT c.SHAPE.st_touches(c.SHAPE) FROM COLA_MARKETS AS c"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testVarbinaryComparison() throws Exception {
        String input = "select bin_col from binary_test where bin_col = x'ab'"; //$NON-NLS-1$
        String output = "SELECT binary_test.BIN_COL FROM binary_test WHERE binary_test.BIN_COL = to_binary('AB')"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testVarbinaryInsert() throws Exception {
        String input = "insert into binary_test (bin_col) values (x'bc')"; //$NON-NLS-1$
        String output = "INSERT INTO binary_test (BIN_COL) VALUES (to_binary('BC'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    /*
     * Pushdown function tests
     */
    @Test public void testPushDownAddDays() throws Exception {
        String input = "SELECT add_days(convert(PART_WEIGHT, date), 30) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT add_days(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'), 30) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownAddMonths() throws Exception {
        String input = "SELECT add_months(convert(PART_WEIGHT, date), 10) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT add_months(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'), 10) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownAddSeconds() throws Exception {
        String input = "SELECT add_seconds(convert(PART_WEIGHT, timestamp), 10) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT add_seconds(to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS'), 10) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownAddYears() throws Exception {
        String input = "SELECT add_years(convert(PART_WEIGHT, date), 10) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT add_years(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'), 10) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownCurrentUTCTime() throws Exception {
        String input = "SELECT current_utctime()"; //$NON-NLS-1$
        String output = "SELECT current_utctime()";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownCurrentUTCTimeStamp() throws Exception {
        String input = "SELECT current_utctimestamp()"; //$NON-NLS-1$
        String output = "SELECT current_utctimestamp()";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownDaysBetween() throws Exception {
        String input = "SELECT days_between(convert(PART_WEIGHT, date), convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT days_between(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'), to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownIsoWeek() throws Exception {
        String input = "SELECT isoweek(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT isoweek(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownLastDay() throws Exception {
        String input = "SELECT last_day(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT last_day(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownLocalToUtc() throws Exception {
        String input = "SELECT localtoutc(convert(PART_WEIGHT, timestamp), 'EST') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT localtoutc(to_timestamp(PARTS.PART_WEIGHT, 'YYYY-MM-DD HH24:MI:SS'), 'EST') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownNano100Between() throws Exception {
        String input = "SELECT nano100_between(convert(PART_WEIGHT, date), convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT nano100_between(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD'), to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownNextDay() throws Exception {
        String input = "SELECT next_day(convert(PART_WEIGHT, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT next_day(to_date(PARTS.PART_WEIGHT, 'YYYY-MM-DD')) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownCosh() throws Exception {
        String input = "SELECT cosh(0.5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT cosh(0.5) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
                input,
                output);
    }

    @Test public void testPushDownBitset() throws Exception {
        String input = "SELECT bitset(convert(bin_col, varbinary), 1, 3) FROM binary_test"; //$NON-NLS-1$
        String output = "SELECT bitset(binary_test.BIN_COL, 1, 3) FROM binary_test";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

    @Test public void testMaxBoolean() throws Exception {
        String input = "SELECT max(booleanvalue) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast(MAX(to_tinyint(SmallA.BooleanValue)) as boolean) FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

    @Test public void testCountBoolean() throws Exception {
        String input = "SELECT count(booleanvalue) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT COUNT(SmallA.BooleanValue) FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

    @Test public void testBooleanCastString() throws Exception {
        String input = "SELECT cast(booleanvalue as string) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.BooleanValue = true THEN 'true' WHEN SmallA.BooleanValue IS NOT NULL THEN 'false' END FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

    @Test public void testBooleanExpression() throws Exception {
        String input = "SELECT (CASE WHEN BooleanValue THEN 'a' ELSE 'b' END) FROM BQT1.smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.BooleanValue = true THEN 'a' ELSE 'b' END FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

    @Test public void testObjectCast() throws Exception {
        String input = "SELECT cast(intkey as object) FROM BQT1.smalla"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(getTestBQTVDB(),
                input,
                output);
    }

}