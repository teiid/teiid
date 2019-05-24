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

package org.teiid.translator.jdbc.netezza;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;

@SuppressWarnings("nls")
public class TestNetezzaConvertModifier {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    public String helpGetString(Expression expr) throws Exception {
        NetezzaExecutionFactory trans = new NetezzaExecutionFactory();
        trans.start();
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();
        sqlVisitor.append(expr);

        return sqlVisitor.toString();
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            Arrays.asList(
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)),
            TypeFacility.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType,
            expectedExpression, helpGetString(func));
    }

    // Source = STRING
    @Test public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS char(1))");
    }

    @Test public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "CASE WHEN '5' IN ('false', '0') THEN '0' WHEN '5' IS NOT NULL THEN '1' END");
    }

    @Test public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "cast('5' AS byteint)");
    }

    @Test public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "cast('5' AS smallint)");
    }

    @Test public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "cast('5' AS integer)");
    }

    @Test public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "cast('5' AS bigint)");
    }

    @Test public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "cast('5' AS numeric(38))");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "cast('5' AS float)");
    }

    @Test public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "cast('5' AS double)");
    }

    @Test public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "to_date('2004-06-29', 'YYYY-MM-DD')");
    }

    @Test public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "to_timestamp('23:59:59', 'HH24:MI:SS')");
    }

    @Test public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "to_timestamp('2004-06-29 23:59:59.987', 'YYYY-MM-DD HH24:MI:SS.MS')");
    }

    @Test public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "cast('5' AS numeric(38,18))");
    }

    // Source = CHAR

    @Test public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'");
    }

    // Source = BOOLEAN

    @Test public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "CASE WHEN 1 = '0' THEN 'false' WHEN 1 IS NOT NULL THEN 'true' END");
    }

    @Test public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "byte", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "short", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "integer", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "long", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "biginteger", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "float", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "double", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    @Test public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "bigdecimal", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)");
    }

    // Source = BYTE

    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END");
    }

//    @Test public void testByteToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "1");
//    }
//
//    @Test public void testByteToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "integer(1)");
//    }
//
//    @Test public void testByteToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "bigint(1)");
//    }
//
//    @Test public void testByteToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "cast(1 AS numeric(31,0))");
//    }
//
//    @Test public void testByteToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "cast(1 AS real)");
//    }
//
//    @Test public void testByteToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "double(1)");
//    }
//
//    @Test public void testByteToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "cast(1 AS numeric(31,12))");
//    }

    // Source = SHORT

    @Test public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END");
    }

//    @Test public void testShortToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "1");
//    }
//
//    @Test public void testShortToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "integer(1)");
//    }
//
//    @Test public void testShortToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "bigint(1)");
//    }
//
//    @Test public void testShortToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "cast(1 AS numeric(31,0))");
//    }
//
//    @Test public void testShortToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "cast(1 AS real)");
//    }
//
//    @Test public void testShortToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "double(1)");
//    }
//
//    @Test public void testShortToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "cast(1 AS numeric(31,12))");
//    }

    // Source = INTEGER

    @Test public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END");
    }

//    @Test public void testIntegerToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "smallint(1)");
//    }
//
//    @Test public void testIntegerToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "smallint(1)");
//    }
//
//    @Test public void testIntegerToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "bigint(1)");
//    }
//
//    @Test public void testIntegerToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "cast(1 AS numeric(31,0))");
//    }
//
//    @Test public void testIntegerToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "cast(1 AS real)");
//    }
//
//    @Test public void testIntegerToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "double(1)");
//    }
//
//    @Test public void testIntegerToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "cast(1 AS numeric(31,12))");
//    }

    // Source = LONG

    @Test public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END");
    }

//    @Test public void testLongToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "smallint(1)");
//    }
//
//    @Test public void testLongToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "smallint(1)");
//    }
//
//    @Test public void testLongToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "integer(1)");
//    }
//
//    @Test public void testLongToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "cast(1 AS numeric(31,0))");
//    }
//
//    @Test public void testLongToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "cast(1 AS real)");
//    }
//
//    @Test public void testLongToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "double(1)");
//    }
//
//    @Test public void testLongToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "cast(1 AS numeric(31,12))");
//    }

    // Source = BIGINTEGER

    @Test public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "cast(1 AS varchar(4000))");
    }

    @Test public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END");
    }

//    @Test public void testBigIntegerToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "smallint(1)");
//    }
//
//    @Test public void testBigIntegerToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "smallint(1)");
//    }
//
//    @Test public void testBigIntegerToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "integer(1)");
//    }
//
//    @Test public void testBigIntegerToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "bigint(1)");
//    }
//
//    @Test public void testBigIntegerToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "cast(1 AS real)");
//    }
//
//    @Test public void testBigIntegerToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "double(1)");
//    }
//
//    @Test public void testBigIntegerToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "cast(1 AS numeric(31,12))");
//    }

    // Source = FLOAT

    @Test public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "cast(1.2 AS varchar(4000))");
    }

    @Test public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "CASE WHEN 1.2 = 0 THEN '0' WHEN 1.2 IS NOT NULL THEN '1' END");
    }

//    @Test public void testFloatToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "smallint(1.2)");
//    }
//
//    @Test public void testFloatToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "smallint(1.2)");
//    }
//
//    @Test public void testFloatToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "integer(1.2)");
//    }
//
//    @Test public void testFloatToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "bigint(1.2)");
//    }
//
//    @Test public void testFloatToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "cast(1.2 AS numeric(31,0))");
//    }
//
//    @Test public void testFloatToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "double(1.2)");
//    }
//
//    @Test public void testFloatToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "cast(1.2 AS numeric(31,12))");
//    }

    // Source = DOUBLE

    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "cast(1.2 AS varchar(4000))");
    }

    @Test public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "CASE WHEN 1.2 = 0 THEN '0' WHEN 1.2 IS NOT NULL THEN '1' END");
    }

//    @Test public void testDoubleToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "smallint(1.2)");
//    }
//
//    @Test public void testDoubleToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "smallint(1.2)");
//    }
//
//    @Test public void testDoubleToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "integer(1.2)");
//    }
//
//    @Test public void testDoubleToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "bigint(1.2)");
//    }
//
//    @Test public void testDoubleToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "cast(1.2 AS numeric(31,0))");
//    }
//
//    @Test public void testDoubleToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "cast(1.2 AS real)");
//    }
//
//    @Test public void testDoubleToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "cast(1.2 AS numeric(31,12))");
//    }

    // Source = BIGDECIMAL

    @Test public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "cast(1.0 AS varchar(4000))");
    }

    @Test public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "CASE WHEN 1.0 = 0 THEN '0' WHEN 1.0 IS NOT NULL THEN '1' END");
    }

//    @Test public void testBigDecimalToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "smallint(1.0)");
//    }
//
//    @Test public void testBigDecimalToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "smallint(1.0)");
//    }
//
//    @Test public void testBigDecimalToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "integer(1.0)");
//    }
//
//    @Test public void testBigDecimalToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "bigint(1.0)");
//    }
//
//    @Test public void testBigDecimalToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "cast(1.0 AS numeric(31,0))");
//    }
//
//    @Test public void testBigDecimalToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "cast(1.0 AS real)");
//    }
//
//    @Test public void testBigDecimalToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "double(1.0)");
//    }

//    // Source = DATE
//
//    @Test public void testDateToString() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "char({d '2003-11-01'})");
//    }
//
//    @Test public void testDateToTimestamp() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "timestamp({d '2003-11-01'}, '00:00:00')");
//    }
//
//    // Source = TIME
//
//    @Test public void testTimeToString() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "char({t '23:59:59'})");
//    }
//
//    @Test public void testTimeToTimestamp() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "timestamp('1970-01-01', {t '23:59:59'})");
//    }
//
//    // Source = TIMESTAMP
//
//    @Test public void testTimestampToString() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "char({ts '2003-11-01 12:05:02.0'})");
//    }
//
//    @Test public void testTimestampToDate() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "date({ts '2003-11-01 12:05:02.0'})");
//    }
//
//    @Test public void testTimestampToTime() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "time({ts '2003-11-01 12:05:02.0'})");
//    }

}
