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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
public class TestDB2ConvertModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestSybaseConvertModifier.
     * @param name
     */
    public TestDB2ConvertModifier(String name) {
        super(name);
    }

    public String helpGetString(Expression expr) throws Exception {
        DB2ExecutionFactory trans = new DB2ExecutionFactory();
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

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$
            expectedExpression, helpGetString(func));
    }

    // Source = STRING
    public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS char(1))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "CASE WHEN '5' IN ('false', '0') THEN 0 WHEN '5' IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "smallint('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "smallint('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "integer('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "bigint('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "cast('5' AS numeric(31,0))");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "cast(double('5') as real)");//$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "double('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "date('2004-06-29')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "time('23:59:59')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "timestamp('2004-06-29 23:59:59.987')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "cast('5' AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = CHAR

    public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BOOLEAN

    public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "CASE WHEN 1 = 0 THEN 'false' WHEN 1 IS NOT NULL THEN 'true' END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "integer", "integer(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "long", "bigint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "biginteger", "cast(1 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BYTE

    public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "varchar(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "integer(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "bigint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "cast(1 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = SHORT

    public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "varchar(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "integer(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "bigint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "cast(1 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = INTEGER

    public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "varchar(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "bigint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "cast(1 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = LONG

    public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "varchar(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "integer(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "cast(1 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGINTEGER

    public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "varchar(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "smallint(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "integer(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "bigint(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "double(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "cast(1 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT

    public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "varchar(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "CASE WHEN 1.2 = 0 THEN 0 WHEN 1.2 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "smallint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "smallint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "integer(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "bigint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "cast(1.2 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "double(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "cast(1.2 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE

    public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "varchar(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "CASE WHEN 1.2 = 0 THEN 0 WHEN 1.2 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "smallint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "smallint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "integer(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "bigint(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "cast(1.2 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "cast(1.2 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "cast(1.2 AS numeric(31,12))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGDECIMAL

    public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "varchar(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "CASE WHEN 1.0 = 0 THEN 0 WHEN 1.0 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "smallint(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "smallint(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "integer(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "bigint(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "cast(1.0 AS numeric(31,0))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "cast(1.0 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "double(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = DATE

    public void testDateToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "varchar({d '2003-11-01'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "timestamp({d '2003-11-01'}, '00:00:00')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "varchar({t '23:59:59'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimeToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "timestamp('1970-01-01', {t '23:59:59'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIMESTAMP

    public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "varchar({ts '2003-11-01 12:05:02.0'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToDate() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "date({ts '2003-11-01 12:05:02.0'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToTime() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "time({ts '2003-11-01 12:05:02.0'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
