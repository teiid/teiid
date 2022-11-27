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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.util.Version;


/**
 */
public class TestSybaseConvertModifier {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
    private static SybaseExecutionFactory trans = new SybaseExecutionFactory();

    @BeforeClass
    public static void setup() throws TranslatorException {
        trans.setDatabaseVersion(Version.DEFAULT_VERSION);
        trans.start();
    }

    public String helpGetString(Expression expr) throws Exception {
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();
        sqlVisitor.append(expr);
        return sqlVisitor.toString();
    }

    private void helpGetString1(Function func, String expectedStr) throws Exception {
        assertEquals(expectedStr, helpGetString(func));
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)},
            DataTypeManager.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + DataTypeManager.getDataTypeName(srcExpression.getType()) + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$
            expectedExpression, helpGetString(func));
    }

    // original test -- this is not a drop one anymore
    @Test public void testModDrop() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("5", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer", String.class)     //$NON-NLS-1$
            },
            Integer.class);

        assertEquals("cast('5' AS int)", helpGetString(func)); //$NON-NLS-1$
    }

    /********************Beginning of cast(date AS INPUT) ******************/
    @Test public void testStringToDate() throws Exception {
        String dateStr = "2003-12-31"; //$NON-NLS-1$
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(dateStr, String.class),
                LANG_FACTORY.createLiteral("date", String.class)}, //$NON-NLS-1$
            java.sql.Date.class);

        assertEquals("cast('2003-12-31' AS datetime)", helpGetString(func)); //$NON-NLS-1$
    }

    @Test public void testTimestampToDate() throws Exception {
        Literal c = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(89, 2, 3, 7, 8, 12, 99999), Timestamp.class);
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                c,
                LANG_FACTORY.createLiteral("date", String.class)}, //$NON-NLS-1$
            java.sql.Date.class);

        helpGetString1(func,  "cast(stuff(stuff(convert(varchar, CAST('1989-03-03 07:08:12.0' AS DATETIME), 102), 5, 1, '-'), 8, 1, '-') AS datetime)");  //$NON-NLS-1$
    }

    /********************END of cast(date AS INPUT) ******************/
    /********************Beginning of cast(time AS INPUT) ******************/
    @Test public void testStringToTime() throws Exception {
        String timeStr = "12:08:07"; //$NON-NLS-1$
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(timeStr, String.class),
                LANG_FACTORY.createLiteral("time", String.class)}, //$NON-NLS-1$
            java.sql.Time.class);

        helpGetString1(func,  "cast('12:08:07' AS datetime)");  //$NON-NLS-1$
    }

    @Test public void testTimestampToTime() throws Exception {
        Literal c = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(89, 2, 3, 7, 8, 12, 99999), Timestamp.class);
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                c,
                LANG_FACTORY.createLiteral("time", String.class)}, //$NON-NLS-1$
            java.sql.Time.class);

        helpGetString1(func,  "cast('1970-01-01 ' + convert(varchar, CAST('1989-03-03 07:08:12.0' AS DATETIME), 8) AS datetime)");  //$NON-NLS-1$
    }
    /********************END of cast(time AS INPUT) ******************/

    /********************Beginning of cast(timestamp AS INPUT) ******************/
    @Test public void testStringToTimestamp() throws Exception {
        String timestampStr = "1989-07-09 12:08:07"; //$NON-NLS-1$
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(timestampStr, String.class),
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);

        helpGetString1(func,  "cast('1989-07-09 12:08:07' AS datetime)");  //$NON-NLS-1$
    }

    @Test public void testTimeToTimestamp() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(TimestampUtil.createTime(12, 2, 3), java.sql.Time.class),
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);

        helpGetString1(func,  "CAST('1970-01-01 12:02:03.0' AS DATETIME)");  //$NON-NLS-1$
    }

    @Test public void testDateToTimestamp() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(TimestampUtil.createDate(89, 2, 3), java.sql.Date.class),
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);

        helpGetString1(func,  "CAST('1989-03-03' AS DATE)");  //$NON-NLS-1$
    }
    /********************END of cast(timestamp AS INPUT) ******************/

    /*****************Beginning of cast(string AS input)******************/
    @Test public void testBooleanToStringa() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "CASE WHEN 1 = 0 THEN 'false' ELSE 'true' END");  //$NON-NLS-1$
    }

    @Test public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(ts, Timestamp.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "stuff(stuff(convert(varchar, CAST('2003-11-01 12:05:02.0' AS DATETIME), 102), 5, 1, '-'), 8, 1, '-')+' '+convert(varchar, CAST('2003-11-01 12:05:02.0' AS DATETIME), 8)");  //$NON-NLS-1$

        func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
                new Expression[] {
                    LANG_FACTORY.createLiteral(ts, Timestamp.class),
                    LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
                String.class);

        SybaseExecutionFactory old = trans;
        trans = new SybaseExecutionFactory();
        trans.setDatabaseVersion(SybaseExecutionFactory.FIFTEEN_5);
        trans.start();
        try {
            helpGetString1(func,  "stuff(convert(varchar, CAST('2003-11-01 12:05:02.0' AS DATETIME), 23), 11, 1, ' ')");  //$NON-NLS-1$
        } finally {
            trans = old;
        }
    }

    @Test public void testDateToString() throws Exception {
        java.sql.Date d = TimestampUtil.createDate(103, 10, 1);
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(d, java.sql.Date.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "stuff(stuff(convert(varchar, CAST('2003-11-01' AS DATE), 102), 5, 1, '-'), 8, 1, '-')");  //$NON-NLS-1$
    }

    @Test public void testTimeToString() throws Exception {
        java.sql.Time t = TimestampUtil.createTime(3, 10, 1);
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(t, java.sql.Time.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "convert(varchar, CAST('1970-01-01 03:10:01.0' AS DATETIME), 8)");  //$NON-NLS-1$
    }

    @Test public void testBigDecimalToString() throws Exception {
        java.math.BigDecimal m = new java.math.BigDecimal("-123124534.3"); //$NON-NLS-1$
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(m, java.math.BigDecimal.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "cast(-123124534.3 AS varchar(4000))");  //$NON-NLS-1$
    }
    /***************** End of cast(string AS input)******************/

    /***************** Beginning of cast(char AS input) ************/
    @Test public void testStringToChar() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("12", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("char", Character.class)}, //$NON-NLS-1$
        Character.class);

        helpGetString1(func,  "cast('12' AS char(1))");  //$NON-NLS-1$
    }
    /***************** End of cast(char AS input)******************/

    /***************** Beginning of cast(boolean AS input) ************/
    @Test public void testStringToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("true", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 'true' IN ('false', '0') THEN 0 WHEN 'true' IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testByteToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testShortToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Short((short) 0), Short.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 0 = 0 THEN 0 WHEN 0 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testIntegerToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Integer(1), Integer.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testLongToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Long(1), Long.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testBigIntegerToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigInteger("1"), java.math.BigInteger.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testFloatToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Float((float)1.0), Float.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1.0 = 0 THEN 0 WHEN 1.0 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testDoubleToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1.0 = 0 THEN 0 WHEN 1.0 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    @Test public void testBigDecimalToBoolean() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("1.0"), java.math.BigDecimal.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);

        helpGetString1(func,  "CASE WHEN 1.0 = 0 THEN 0 WHEN 1.0 IS NOT NULL THEN 1 END");  //$NON-NLS-1$
    }

    /***************** End of cast(boolean AS input)******************/


    /***************** Beginning of cast(byte AS input) ************/
    @Test public void testStringToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("12", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte", Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast('12' AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToBytea() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("byte", Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "1");  //$NON-NLS-1$
    }

    @Test public void testShortToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Short((short) 123), Short.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "123");  //$NON-NLS-1$
    }

    @Test public void testIntegerToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Integer(1232321), Integer.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(1232321 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testLongToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(1231232341 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBigIntegerToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(123 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testFloatToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(123.0 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testDoubleToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(1.0 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBigDecimalToByte() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);

        helpGetString1(func,  "cast(12.3 AS smallint)");  //$NON-NLS-1$
    }

    /***************** End of cast(byte AS input)******************/

    /*****************Beginning of cast(short AS input)************/
    @Test public void testStringToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short", Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast('123' AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToShorta() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("short", Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "1");  //$NON-NLS-1$
    }

    @Test public void testByteToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Byte((byte) 12), Byte.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "12");  //$NON-NLS-1$
    }

    @Test public void testIntegerToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Integer(1232321), Integer.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(1232321 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testLongToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(1231232341 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBigIntegerToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(123 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testFloatToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(123.0 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testDoubleToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(1.0 AS smallint)");  //$NON-NLS-1$
    }

    @Test public void testBigDecimalToShort() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);

        helpGetString1(func,  "cast(12.3 AS smallint)");  //$NON-NLS-1$
    }
    /***************** End of cast(short AS input)******************/

    /***************** Beginning of cast(integer AS input) ************/
    @Test public void testStringToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("12332", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast('12332' AS int)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToIntegera() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(1 AS int)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToIntegerb() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(0 AS int)");  //$NON-NLS-1$
    }

    @Test public void testByteToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Byte((byte)12), Byte.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(12 AS int)");  //$NON-NLS-1$
    }

    @Test public void testShortToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Short((short)1243 ), Short.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(1243 AS int)");  //$NON-NLS-1$
    }

    @Test public void testLongToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(1231232341 AS int)");  //$NON-NLS-1$
    }

    @Test public void testBigIntegerToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(123 AS int)");  //$NON-NLS-1$
    }

    @Test public void testFloatToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(123.0 AS int)");  //$NON-NLS-1$
    }

    @Test public void testDoubleToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(1.0 AS int)");  //$NON-NLS-1$
    }

    @Test public void testBigDecimalToInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func,  "cast(12.3 AS int)");  //$NON-NLS-1$
    }

    /***************** End of cast(integer AS input)******************/

    /***************** Beginning of cast(long AS input) ************/
    @Test public void testStringToLong() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("12332131413", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("long", Long.class)}, //$NON-NLS-1$
            Long.class);

        helpGetString1(func,  "cast('12332131413' AS numeric(19,0))");  //$NON-NLS-1$
    }

    @Test public void testBooleanToLonga() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), LANG_FACTORY.createLiteral("long", Long.class)}, //$NON-NLS-1$
            Long.class);

        helpGetString1(func, "cast(1 AS numeric(19,0))"); //$NON-NLS-1$
    }

    /***************** End of cast(long AS input)******************/

    /***************** Beginning of cast(biginteger AS input) ************/
    @Test public void testStringToBigInteger() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("12323143241414", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("biginteger", java.math.BigInteger.class)}, //$NON-NLS-1$
            java.math.BigInteger.class);

        helpGetString1(func,  "cast('12323143241414' AS numeric(38, 0))");  //$NON-NLS-1$
    }

    @Test public void testBooleanToBigIntegera() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), LANG_FACTORY.createLiteral("biginteger", java.math.BigInteger.class)}, //$NON-NLS-1$
            BigInteger.class);

        helpGetString1(func, "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$
    }

    /***************** End of cast(biginteger AS input)******************/

    /***************** Beginning of cast(float AS input) ************/
    @Test public void testStringToFloat() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);

        helpGetString1(func,  "cast('123' AS real)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToFloata() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);

        helpGetString1(func, "cast(1 AS real)"); //$NON-NLS-1$
    }

    @Test public void testBooleanToFloatb() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);

        helpGetString1(func, "cast(0 AS real)"); //$NON-NLS-1$
    }

    /***************** End of cast(float AS input)******************/

    /***************** Beginning of cast(double AS input) ************/
    @Test public void testStringToDouble() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);

        helpGetString1(func,  "cast('123' AS double precision)");  //$NON-NLS-1$
    }

    @Test public void testBooleanToDoublea() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);

        helpGetString1(func, "cast(1 AS double precision)"); //$NON-NLS-1$
    }

    @Test public void testBooleanToDoubleb() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);

        helpGetString1(func, "cast(0 AS double precision)"); //$NON-NLS-1$
    }

    /***************** End of cast(double AS input)******************/

    /***************** Beginning of cast(bigdecimal AS input) ************/
    @Test public void testStringToBigDecimal() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new Expression[] {
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("bigdecimal", java.math.BigDecimal.class)}, //$NON-NLS-1$
            java.math.BigDecimal.class);

        helpGetString1(func,  "cast('123' AS numeric(38, 19))");  //$NON-NLS-1$
    }

    @Test public void testBooleanToBigDecimala() throws Exception {
        Function func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new Expression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("bigdecimal", java.math.BigDecimal.class)}, //$NON-NLS-1$
            java.math.BigDecimal.class);

        helpGetString1(func, "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$
    }

    // Source = CHAR

    @Test public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BOOLEAN

    @Test public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "CASE WHEN 1 = 0 THEN 'false' ELSE 'true' END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "integer", "cast(1 AS int)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "long", "cast(1 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "biginteger", "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BYTE

    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "cast(1 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = SHORT

    @Test public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "cast(1 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testShortToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = INTEGER

    @Test public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "cast(1 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = LONG

    @Test public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "cast(1 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLongToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGINTEGER

    @Test public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "cast(1 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "cast(1 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "cast(1 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "cast(1 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT

    @Test public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "cast(1.2 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "cast(1.2 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "cast(1.2 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "cast(1.2 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFloatToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "cast(1.2 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE

    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "cast(1.2 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "cast(1.2 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "cast(1.2 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "cast(1.2 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDoubleToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "cast(1.2 AS numeric(38, 19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBigDecimalToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "cast(1.0 AS numeric(19,0))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "cast(1.0 AS numeric(38, 0))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "cast(1.0 AS real)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBigDecimalToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "cast(1.0 AS double precision)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
