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
package org.teiid.translator.jdbc.teradata;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.TimeZone;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestTeradataTranslator {

    private static TeradataExecutionFactory TRANSLATOR;
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new TeradataExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",
            Arrays.asList( srcExpression,LANG_FACTORY.createLiteral(tgtType, String.class)),TypeFacility.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType,
            expectedExpression, helpGetString(func));
    }

    public String helpGetString(Expression expr) throws Exception {
        SQLConversionVisitor sqlVisitor = TRANSLATOR.getSQLConversionVisitor();
        sqlVisitor.append(expr);

        return sqlVisitor.toString();
    }

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT dayofmonth(datevalue) FROM BQT1.SMALLA";
        String output = "SELECT extract(DAY from SmallA.DateValue) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testTimestampToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(111, 4, 5, 9, 16, 34, 220000000), Timestamp.class), "time", "cast(TIMESTAMP '2011-05-05 09:16:34.22' AS TIME)");
    }

    @Test public void testIntegerToString() throws Exception {
        String input = "SELECT lcase(bigdecimalvalue) FROM BQT1.SMALLA";
        String output = "SELECT LOWER(cast(SmallA.BigDecimalValue AS varchar(4000))) FROM SmallA";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testSubString() throws Exception {
        String input = "SELECT intkey FROM BQT1.SmallA WHERE SUBSTRING(BQT1.SmallA.IntKey, 1) = '1' ORDER BY intkey";
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE substr(cast(SmallA.IntKey AS varchar(4000)), 1) = '1' ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testSubString2() throws Exception {
        String input = "SELECT intkey FROM BQT1.SmallA WHERE SUBSTRING(BQT1.SmallA.IntKey, 1, 2) = '1' ORDER BY intkey";
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE substr(cast(SmallA.IntKey AS varchar(4000)), 1, 2) = '1' ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testDateToString() throws Exception {
        String input = "SELECT intkey, UPPER(timevalue) AS UPPER FROM BQT1.SmallA ORDER BY intkey";
        String output = "SELECT SmallA.IntKey, UPPER(cast(cast(SmallA.TimeValue AS FORMAT 'HH:MI:SS') AS VARCHAR(9))) AS UPPER FROM SmallA ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testLocate() throws Exception {
        String input = "SELECT INTKEY, BIGDECIMALVALUE FROM BQT1.SmallA WHERE LOCATE('-', BIGDECIMALVALUE) = 1 ORDER BY intkey";
        String output = "SELECT SmallA.IntKey, SmallA.BigDecimalValue FROM SmallA WHERE position('-' in cast(SmallA.BigDecimalValue AS varchar(4000))) = 1 ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testPositionalOrderBy() throws Exception {
        String input = "SELECT INTKEY, BIGDECIMALVALUE FROM BQT1.SmallA ORDER BY intkey";
        String output = "SELECT g_0.IntKey AS c_0, g_0.BigDecimalValue AS c_1 FROM SmallA AS g_0 ORDER BY 1";

        TranslationUtility tu = TranslationHelper.getTranslationUtility(TranslationHelper.BQT_VDB);
        Command command = tu.parseCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, TRANSLATOR, command);
    }

    @Test public void testPositionalOrderByUnion() throws Exception {
        String input = "SELECT a as b, b as a from (SELECT intkey as a, stringkey as b from BQT1.smalla union SELECT intkey as a, stringkey as b from BQT1.smalla) as x order by a";
        String output = "SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_1.IntKey AS c_0, g_1.StringKey AS c_1 FROM SmallA AS g_1 UNION SELECT g_0.IntKey AS c_0, g_0.StringKey AS c_1 FROM SmallA AS g_0) AS v_0 ORDER BY 2";

        TranslationUtility tu = TranslationHelper.getTranslationUtility(TranslationHelper.BQT_VDB);
        Command command = tu.parseCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, TRANSLATOR, command);
    }

    @Test public void testPositionalOrderByUnion1() throws Exception {
        String input = "SELECT a as b from (SELECT intkey as a, stringkey as b from BQT1.smalla union SELECT intkey as a, stringkey as b from BQT1.smalla) as x order by x.b";
        String output = "SELECT v_0.c_0 FROM (SELECT g_1.IntKey AS c_0, g_1.StringKey AS c_1 FROM SmallA AS g_1 UNION SELECT g_0.IntKey AS c_0, g_0.StringKey AS c_1 FROM SmallA AS g_0) AS v_0 ORDER BY v_0.c_1";

        TranslationUtility tu = TranslationHelper.getTranslationUtility(TranslationHelper.BQT_VDB);
        Command command = tu.parseCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, TRANSLATOR, command);
    }

    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testByte2ToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)-1), Byte.class), "string", "cast(-1 AS varchar(4000))");
    }

    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.0), Double.class), "string", "cast(1.0 AS varchar(4000))");
    }

    @Test public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("1.0", String.class), "double", "cast('1.0' AS double precision)");
    }

    @Test public void testNullComapreNull() throws Exception {
        String input = "SELECT INTKEY, STRINGKEY, DOUBLENUM FROM bqt1.smalla WHERE NULL <> NULL";
        String out = "SELECT SmallA.IntKey, SmallA.StringKey, SmallA.DoubleNum FROM SmallA WHERE 1 = 0";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testPushDownFunction() throws Exception {
        String input = "SELECT teradata.HASHBAKAMP(STRINGKEY) DOUBLENUM FROM bqt1.smalla";
        String out = "SELECT HASHBAKAMP(SmallA.StringKey) AS DOUBLENUM FROM SmallA";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testRightFunction() throws Exception {
        String input = "SELECT INTKEY, FLOATNUM FROM BQT1.SmallA WHERE right(FLOATNUM, 2) <> 0 ORDER BY INTKEY";
        String out = "SELECT SmallA.IntKey, SmallA.FloatNum FROM SmallA WHERE substr(cast(SmallA.FloatNum AS varchar(4000)),(character_length(cast(SmallA.FloatNum AS varchar(4000)))-2+1)) <> '0' ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testLocateFunction() throws Exception {
        String input = "SELECT INTKEY, STRINGKEY, SHORTVALUE FROM BQT1.SmallA WHERE (LOCATE(0, STRINGKEY) = 2) OR (LOCATE(2, SHORTVALUE, 4) = 6) ORDER BY intkey";
        String out = "SELECT SmallA.IntKey, SmallA.StringKey, SmallA.ShortValue FROM SmallA WHERE position('0' in SmallA.StringKey) = 2 OR position('2' in substr(cast(SmallA.ShortValue AS varchar(4000)),4)) = 6 ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testInValues() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SmallA WHERE STRINGKEY IN (INTKEY, 'a', 'b') AND STRINGKEY NOT IN (SHORTVALUE, 'c') AND INTKEY IN (1, 2) ORDER BY intkey";
        String out = "SELECT SmallA.IntKey FROM SmallA WHERE (SmallA.StringKey = cast(SmallA.IntKey AS varchar(4000)) OR SmallA.StringKey IN ('a', 'b')) AND (SmallA.StringKey <> cast(SmallA.ShortValue AS varchar(4000)) AND SmallA.StringKey NOT IN ('c')) AND SmallA.IntKey IN (1, 2) ORDER BY 1";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testDateTimeLiterals() throws Exception {
        String input = "SELECT {t '12:00:00'}, {d '2015-01-01'}, {ts '2015-01-01 12:00:00.1'}";
        String out = "SELECT TIME '12:00:00', DATE '2015-01-01', TIMESTAMP '2015-01-01 12:00:00.1'";
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, out, TRANSLATOR);
    }

    @Test public void testLiteralWithDatabaseTimezone() throws TranslatorException {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT"));
        try {
            TeradataExecutionFactory jef = new TeradataExecutionFactory();
            jef.setDatabaseTimeZone("GMT+1");
            jef.start();
            assertEquals("TIMESTAMP '2015-02-03 04:00:00.0'", jef.translateLiteralTimestamp(TimestampUtil.createTimestamp(115, 1, 3, 4, 0, 0, 0)));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

}
