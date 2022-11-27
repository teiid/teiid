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

package org.teiid.translator.jdbc.intersystemscache;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslationHelper;
import org.teiid.translator.jdbc.intersyscache.InterSystemsCacheExecutionFactory;
/**
 */
public class TestInterSystemsCacheTranslation {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    private static InterSystemsCacheExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new InterSystemsCacheExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

    public String helpGetString(Expression expr) throws Exception {
        SQLConversionVisitor sqlVisitor = TRANSLATOR.getSQLConversionVisitor();
        sqlVisitor.append(expr);

        return sqlVisitor.toString();
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            Arrays.asList(
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)),
            TypeFacility.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$
            expectedExpression, helpGetString(func));
    }

    // Source = STRING
    @Test public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS character)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = Boolean

    @Test public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "cast(1 AS decimal(38,19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BYTE

    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "biginteger", "cast(1 AS decimal(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT

    @Test public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "cast(1.2 AS bigint)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE

    @Test public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "cast(1.2 AS smallint)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGDECIMAL

    @Test public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "cast(1.0 AS tinyint)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = DATE

    @Test public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "cast(to_date('2003-11-01', 'yyyy-mm-dd') AS timestamp)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    @Test public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "cast(to_date('23:59:59', 'hh:mi:ss') AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }


    // Source = TIMESTAMP

    @Test public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "cast(to_timestamp('2003-11-01 12:05:02.0', 'yyyy-mm-dd hh:mi:ss.fffffffff') AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

 // Source = LONG
    @Test public void testLongToBigInt() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(5, Long.class), "long", "cast(5 AS bigint)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT intnum/intkey FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT cast((SmallA.IntNum / SmallA.IntKey) AS integer) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

}
