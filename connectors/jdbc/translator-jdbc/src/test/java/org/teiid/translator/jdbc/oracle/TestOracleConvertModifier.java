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

package org.teiid.translator.jdbc.oracle;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.metadata.Column;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
@SuppressWarnings("nls")
public class TestOracleConvertModifier {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    private static OracleExecutionFactory TRANSLATOR = new OracleExecutionFactory();

    @BeforeClass public static void oneTimeSetup() throws Exception {
        TRANSLATOR.start();
    }

    public String helpGetString(Expression expr) throws Exception {
        OracleExecutionFactory trans = new OracleExecutionFactory();
        trans.start();

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
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS char(1))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToNChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("日", String.class), "char", "cast(N'日' AS nchar(1))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testLatinStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("é", String.class), "char", "cast('é' AS char(1))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "CASE WHEN '5' IN ('false', '0') THEN 0 WHEN '5' IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "trunc(to_number('5'))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "trunc(to_number('5'))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "trunc(to_number('5'))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "trunc(to_number('5'))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "trunc(to_number('5'))");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "to_number('5')");//$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "to_date('2004-06-29', 'YYYY-MM-DD')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "to_date('23:59:59', 'HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "to_timestamp('2004-06-29 23:59:59.987', 'YYYY-MM-DD HH24:MI:SS.FF')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = CHAR

    @Test public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('日'), Character.class), "string", "N'日'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNCharToStringConversion() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('日'), String.class), "string", "TO_NCHAR(N'日')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BOOLEAN

    @Test public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "CASE WHEN 1 = 0 THEN 'false' WHEN 1 IS NOT NULL THEN 'true' END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BYTE

    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = SHORT

    @Test public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = INTEGER

    @Test public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = LONG

    @Test public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGINTEGER

    @Test public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT

    @Test public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "to_char(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "CASE WHEN 1.2 = 0 THEN 0 WHEN 1.2 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE

    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "to_char(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "CASE WHEN 1.2 = 0 THEN 0 WHEN 1.2 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "trunc(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGDECIMAL

    @Test public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "to_char(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "CASE WHEN 1.0 = 0 THEN 0 WHEN 1.0 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "trunc(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "trunc(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "trunc(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "trunc(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "trunc(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToDoublel() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = DATE

    @Test public void testDateToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "to_char({d '2003-11-01'}, 'YYYY-MM-DD')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "cast({d '2003-11-01'} AS timestamp)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    @Test public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "to_char(to_date('1970-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), 'HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTimeToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "cast(to_date('1970-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS timestamp)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIMESTAMP

    @Test public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 10000000);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "to_char({ts '2003-11-01 12:05:02.01'}, 'YYYY-MM-DD HH24:MI:SS.FF')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTimestampToString1() throws Exception {
        Column column = new Column();
        column.setNativeType("DATE");
        column.setNameInSource("dt");
        helpTest(LANG_FACTORY.createColumnReference("dt", LANG_FACTORY.createNamedTable("x", null, null), column, Timestamp.class), "string", "to_char(x.dt, 'YYYY-MM-DD HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTimestampToDate() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 10000000);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "trunc(cast({ts '2003-11-01 12:05:02.01'} AS date))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTimestampToTime() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 10000000);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "case when {ts '2003-11-01 12:05:02.01'} is null then null else to_date('1970-01-01 ' || to_char({ts '2003-11-01 12:05:02.01'}, 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') end"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testClobToString() throws Exception {
        assertTrue(TRANSLATOR.supportsConvert(TypeFacility.RUNTIME_CODES.CLOB, TypeFacility.RUNTIME_CODES.STRING));
        helpTest(new ColumnReference(null, "x", null, DataTypeManager.DefaultDataClasses.CLOB), "string", "DBMS_LOB.substr(x, 4000)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
