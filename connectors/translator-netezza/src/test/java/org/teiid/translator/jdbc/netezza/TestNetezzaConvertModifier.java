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

package org.teiid.translator.jdbc.netezza;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
public class TestNetezzaConvertModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    public TestNetezzaConvertModifier(String name) {
        super(name);
    }

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
    public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS char(1))"); 
    }

    public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "CASE WHEN '5' IN ('false', '0') THEN '0' WHEN '5' IS NOT NULL THEN '1' END"); 
    }

    public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "cast('5' AS byteint)"); 
    }

    public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "cast('5' AS smallint)"); 
    }

    public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "cast('5' AS integer)"); 
    }

    public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "cast('5' AS bigint)"); 
    }

    public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "cast('5' AS numeric(38))");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "cast('5' AS float)");
    }

    public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "cast('5' AS double)"); 
    }

    public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "to_date('2004-06-29', 'YYYY-MM-DD')"); 
    }

    public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "to_timestamp('23:59:59', 'HH24:MI:SS')"); 
    }

    public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "to_timestamp('2004-06-29 23:59:59.987', 'YYYY-MM-DD HH24:MI:SS.MS')"); 
    }

    public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "cast('5' AS numeric(38,18))"); 
    }

    // Source = CHAR
    
    public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'"); 
    }

    // Source = BOOLEAN
    
    public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "CASE WHEN 1 = '0' THEN 'false' WHEN 1 IS NOT NULL THEN 'true' END"); 
    }

    public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "byte", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "short", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "integer", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "long", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "biginteger", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "float", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "double", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }

    public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), "bigdecimal", "(CASE WHEN 0 IN ( '0', 'FALSE') THEN 0 WHEN 0 IS NOT NULL THEN 1 END)"); 
    }
    
    // Source = BYTE
    
    public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))"); 
    }

    public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END"); 
    }

//    public void testByteToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "1"); 
//    }
//
//    public void testByteToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "integer(1)"); 
//    }
//
//    public void testByteToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "bigint(1)"); 
//    }
//
//    public void testByteToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "cast(1 AS numeric(31,0))"); 
//    }
//
//    public void testByteToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "cast(1 AS real)"); 
//    }
//
//    public void testByteToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "double(1)"); 
//    }
//
//    public void testByteToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "cast(1 AS numeric(31,12))"); 
//    }

    // Source = SHORT
    
    public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "cast(1 AS varchar(4000))"); 
    }

    public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END"); 
    }

//    public void testShortToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "1"); 
//    }
//
//    public void testShortToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "integer(1)"); 
//    }
//
//    public void testShortToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "bigint(1)"); 
//    }
//
//    public void testShortToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "cast(1 AS numeric(31,0))"); 
//    }
//
//    public void testShortToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "cast(1 AS real)"); 
//    }
//
//    public void testShortToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "double(1)"); 
//    }
//
//    public void testShortToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "cast(1 AS numeric(31,12))"); 
//    }

    // Source = INTEGER
    
    public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "cast(1 AS varchar(4000))"); 
    }

    public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END"); 
    }

//    public void testIntegerToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "smallint(1)"); 
//    }
//
//    public void testIntegerToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "smallint(1)"); 
//    }
//
//    public void testIntegerToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "bigint(1)"); 
//    }
//
//    public void testIntegerToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "cast(1 AS numeric(31,0))"); 
//    }
//
//    public void testIntegerToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "cast(1 AS real)"); 
//    }
//
//    public void testIntegerToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "double(1)"); 
//    }
//
//    public void testIntegerToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "cast(1 AS numeric(31,12))"); 
//    }

    // Source = LONG
    
    public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "cast(1 AS varchar(4000))"); 
    }

    public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END"); 
    }

//    public void testLongToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "smallint(1)"); 
//    }
//
//    public void testLongToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "smallint(1)"); 
//    }
//
//    public void testLongToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "integer(1)"); 
//    }
//
//    public void testLongToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "cast(1 AS numeric(31,0))"); 
//    }
//
//    public void testLongToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "cast(1 AS real)"); 
//    }
//
//    public void testLongToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "double(1)"); 
//    }
//
//    public void testLongToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "cast(1 AS numeric(31,12))"); 
//    }

    // Source = BIGINTEGER
    
    public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "cast(1 AS varchar(4000))"); 
    }

    public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "CASE WHEN 1 = 0 THEN '0' WHEN 1 IS NOT NULL THEN '1' END"); 
    }

//    public void testBigIntegerToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "smallint(1)"); 
//    }
//
//    public void testBigIntegerToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "smallint(1)"); 
//    }
//
//    public void testBigIntegerToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "integer(1)"); 
//    }
//
//    public void testBigIntegerToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "bigint(1)"); 
//    }
//
//    public void testBigIntegerToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "cast(1 AS real)"); 
//    }
//
//    public void testBigIntegerToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "double(1)"); 
//    }
//
//    public void testBigIntegerToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "cast(1 AS numeric(31,12))"); 
//    }

    // Source = FLOAT
    
    public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "cast(1.2 AS varchar(4000))"); 
    }

    public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "CASE WHEN 1.2 = 0 THEN '0' WHEN 1.2 IS NOT NULL THEN '1' END"); 
    }

//    public void testFloatToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "smallint(1.2)"); 
//    }
//
//    public void testFloatToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "smallint(1.2)"); 
//    }
//
//    public void testFloatToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "integer(1.2)"); 
//    }
//
//    public void testFloatToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "bigint(1.2)"); 
//    }
//
//    public void testFloatToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "cast(1.2 AS numeric(31,0))"); 
//    }
//
//    public void testFloatToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "double(1.2)"); 
//    }
//
//    public void testFloatToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "cast(1.2 AS numeric(31,12))"); 
//    }

    // Source = DOUBLE
    
    public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "cast(1.2 AS varchar(4000))"); 
    }

    public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "CASE WHEN 1.2 = 0 THEN '0' WHEN 1.2 IS NOT NULL THEN '1' END"); 
    }

//    public void testDoubleToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "smallint(1.2)"); 
//    }
//
//    public void testDoubleToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "smallint(1.2)"); 
//    }
//
//    public void testDoubleToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "integer(1.2)"); 
//    }
//
//    public void testDoubleToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "bigint(1.2)"); 
//    }
//
//    public void testDoubleToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "cast(1.2 AS numeric(31,0))"); 
//    }
//
//    public void testDoubleToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "cast(1.2 AS real)"); 
//    }
//
//    public void testDoubleToBigDecimal() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "cast(1.2 AS numeric(31,12))"); 
//    }

    // Source = BIGDECIMAL
    
    public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "cast(1.0 AS varchar(4000))"); 
    }

    public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "CASE WHEN 1.0 = 0 THEN '0' WHEN 1.0 IS NOT NULL THEN '1' END"); 
    }

//    public void testBigDecimalToByte() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "smallint(1.0)"); 
//    }
//
//    public void testBigDecimalToShort() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "smallint(1.0)"); 
//    }
//
//    public void testBigDecimalToInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "integer(1.0)"); 
//    }
//
//    public void testBigDecimalToLong() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "bigint(1.0)"); 
//    }
//
//    public void testBigDecimalToBigInteger() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "cast(1.0 AS numeric(31,0))"); 
//    }
//
//    public void testBigDecimalToFloat() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "cast(1.0 AS real)"); 
//    }
//
//    public void testBigDecimalToDouble() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "double(1.0)"); 
//    }

//    // Source = DATE
//
//    public void testDateToString() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "char({d '2003-11-01'})"); 
//    }
//
//    public void testDateToTimestamp() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "timestamp({d '2003-11-01'}, '00:00:00')"); 
//    }
//
//    // Source = TIME
//
//    public void testTimeToString() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "char({t '23:59:59'})"); 
//    }
//
//    public void testTimeToTimestamp() throws Exception {
//        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "timestamp('1970-01-01', {t '23:59:59'})"); 
//    }
//
//    // Source = TIMESTAMP
//    
//    public void testTimestampToString() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "char({ts '2003-11-01 12:05:02.0'})"); 
//    }
//
//    public void testTimestampToDate() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "date({ts '2003-11-01 12:05:02.0'})"); 
//    }
//
//    public void testTimestampToTime() throws Exception {
//        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
//        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "time({ts '2003-11-01 12:05:02.0'})"); 
//    }
    
}
