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

package org.teiid.core.types.basic;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.Test;
import org.teiid.core.CorePlugin;
import org.teiid.core.types.*;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.query.unittest.TimestampUtil;



/**
 * @since 4.2
 */
public class TestTransforms {

    private static void helpTestTransform(Object value, Object expectedValue) throws TransformationException {
        Transform transform = DataTypeManager.getTransform(value.getClass(), expectedValue.getClass());
        Object result = transform.transform(value, expectedValue.getClass());
        assertEquals(expectedValue, result);
    }

    private static void validateTransform(String src, Object value, String target, Object expectedValue) throws TransformationException {
        try {
            Transform transform = DataTypeManager.getTransform(DataTypeManager.getDataTypeClass(src), expectedValue.getClass());
            Object result = transform.transform(value, expectedValue.getClass());
            assertTrue(expectedValue.getClass().isAssignableFrom(result.getClass()));
            assertFalse("Expected exception for " +src+ " to " + target, //$NON-NLS-1$ //$NON-NLS-2$
                    isException(DataTypeManager.getDataTypeName(value.getClass()), target,value));
        } catch (TransformationException e) {
            if (!isException(DataTypeManager.getDataTypeName(value.getClass()), target,value)) {
                throw e;
            }
        }
    }

    private static void helpTransformException(Object value, Class<?> target, String msg) {
        try {
            Transform transform = DataTypeManager.getTransform(value.getClass(), target);
            transform.transform(value, target);
            fail("Expected to get an exception during the transformation"); //$NON-NLS-1$
        } catch (TransformationException e) {
            if (msg != null) {
                assertEquals(msg, e.getMessage());
            }
        }
    }

    @Test public void testBigDecimalToBigInteger_Defect16875() throws TransformationException {
        helpTestTransform(new BigDecimal("0.5867"), new BigInteger("0")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testString2Boolean() throws TransformationException {
        helpTestTransform(new String("1"), Boolean.TRUE); //$NON-NLS-1$
        helpTestTransform(new String("0"), Boolean.FALSE); //$NON-NLS-1$
        helpTestTransform(new String("true"), Boolean.TRUE); //$NON-NLS-1$
        helpTestTransform(new String("false"), Boolean.FALSE); //$NON-NLS-1$
        helpTestTransform(new String("foo"), Boolean.TRUE); //$NON-NLS-1$
    }

    @Test public void testByte2Boolean() throws TransformationException {
        helpTestTransform(new Byte((byte)1), Boolean.TRUE);
        helpTestTransform(new Byte((byte)0), Boolean.FALSE);
        helpTestTransform(new Byte((byte)12), Boolean.TRUE);
    }

    @Test public void testShort2Boolean() throws TransformationException {
        helpTestTransform(new Short((short)1), Boolean.TRUE);
        helpTestTransform(new Short((short)0), Boolean.FALSE);
        helpTestTransform(new Short((short)12), Boolean.TRUE);
    }

    @Test public void testInteger2Boolean() throws TransformationException {
        helpTestTransform(new Integer(1), Boolean.TRUE);
        helpTestTransform(new Integer(0), Boolean.FALSE);
        helpTestTransform(new Integer(12), Boolean.TRUE);
    }

    @Test public void testLong2Boolean() throws TransformationException {
        helpTestTransform(new Long(1), Boolean.TRUE);
        helpTestTransform(new Long(0), Boolean.FALSE);
        helpTestTransform(new Long(12), Boolean.TRUE);
    }

    @Test public void testBigInteger2Boolean() throws TransformationException {
        helpTestTransform(new BigInteger("1"), Boolean.TRUE); //$NON-NLS-1$
        helpTestTransform(new BigInteger("0"), Boolean.FALSE); //$NON-NLS-1$
        helpTestTransform(new BigInteger("12"), Boolean.TRUE); //$NON-NLS-1$
    }

    @Test public void testBigDecimal2Boolean() throws TransformationException {
        helpTestTransform(new BigDecimal("1"), Boolean.TRUE); //$NON-NLS-1$
        helpTestTransform(new BigDecimal("0"), Boolean.FALSE); //$NON-NLS-1$
        helpTestTransform(new BigDecimal("0.00"), Boolean.FALSE); //$NON-NLS-1$
    }

    static Object[][] testData = {
        /*string-0*/  {"1", "0", "123"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        /*char-1*/    {new Character('1'), new Character('0'), new Character('1')},
        /*boolean-2*/ {Boolean.TRUE, Boolean.FALSE, Boolean.FALSE},
        /*byte-3*/    {new Byte((byte)1), new Byte((byte)0), new Byte((byte)123)},
        /*short-4*/   {new Short((short)1), new Short((short)0), new Short((short)123)},
        /*integer-5*/ {new Integer(1), new Integer(0), new Integer(123)},
        /*long-6*/    {new Long(1), new Long(0), new Long(123)},
        /*biginteger-7*/ {new BigInteger("1"), new BigInteger("0"), new BigInteger("123")}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        /*float-8*/   {new Float(1.0f), new Float(0.0f), new Float(123.0f)},
        /*double-9*/  {new Double(1.0d), new Double(0.0d), new Double(123.0d)},
        /*bigdecimal-10*/{new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("123")}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        /*date-11*/    {new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis())},
        /*time-12*/    {new Time(System.currentTimeMillis()), new Time(System.currentTimeMillis()), new Time(System.currentTimeMillis())},
        /*timestamp-13*/{new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis())},
        /*object-14*/  {null, null, null},
        /*blob-15*/    {null, null, null},
        /*clob-16*/    {new ClobType(ClobImpl.createClob("ClobData".toCharArray())), new ClobType(ClobImpl.createClob("0".toCharArray())), new ClobType(ClobImpl.createClob("123".toCharArray()))}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        /*xml-17*/     {new XMLType(new SQLXMLImpl("<foo>bar</foo>")), new XMLType(new SQLXMLImpl("<foo>bar</foo>")), new XMLType(new SQLXMLImpl("<foo>bar</foo>"))}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
     };

    private String[] dataTypes = TestDataTypeManager.dataTypes;
    private char[][] conversions = TestDataTypeManager.conversions;
    private static boolean isException(String src, String tgt, Object source) {
        return (src.equals(DataTypeManager.DefaultDataTypes.STRING) && tgt.equals(DataTypeManager.DefaultDataTypes.XML))
            || (src.equals(DataTypeManager.DefaultDataTypes.STRING) && tgt.equals(DataTypeManager.DefaultDataTypes.TIME))
            || (src.equals(DataTypeManager.DefaultDataTypes.STRING) && tgt.equals(DataTypeManager.DefaultDataTypes.TIMESTAMP))
            || (src.equals(DataTypeManager.DefaultDataTypes.STRING) && tgt.equals(DataTypeManager.DefaultDataTypes.DATE))
            || (src.equals(DataTypeManager.DefaultDataTypes.CLOB) && tgt.equals(DataTypeManager.DefaultDataTypes.XML));
    }

    @Test public void testAllConversions() throws TransformationException {
        for (int src = 0; src < dataTypes.length; src++) {
            for (int tgt =0; tgt < dataTypes.length; tgt++) {
                char c = conversions[src][tgt];

                if (c == 'I' || c == 'C') {
                    Object[] srcdata = testData[src];
                    Object[] tgtdata = testData[tgt];
                    for (int i=0; i<tgtdata.length; i++) {
                        if (tgtdata[i] != null && srcdata[i] != null) {
                            validateTransform(dataTypes[src], srcdata[i], dataTypes[tgt],tgtdata[i]);
                        }
                    }
                }
            }
        }
    }

    @Test public void testAllConversionsAsObject() throws TransformationException {
        for (int src = 0; src < dataTypes.length; src++) {
            for (int tgt =0; tgt < dataTypes.length; tgt++) {
                char c = conversions[src][tgt];

                if (c == 'I' || c == 'C') {
                    Object[] srcdata = testData[src];
                    Object[] tgtdata = testData[tgt];
                    for (int i=0; i<tgtdata.length; i++) {
                        if (tgtdata[i] != null && srcdata[i] != null) {
                            validateTransform(DefaultDataTypes.OBJECT, srcdata[i], dataTypes[tgt],tgtdata[i]);
                        }
                    }
                }
            }
        }
    }

    @Test public void testObjectToAnyTransformFailure() {
        Transform transform = DataTypeManager.getTransform(DefaultDataClasses.OBJECT, DefaultDataClasses.TIME);
        try {
            transform.transform("1", DefaultDataClasses.TIME); //$NON-NLS-1$
            fail("expected exception"); //$NON-NLS-1$
        } catch (TransformationException e) {
            assertEquals("TEIID10076 Invalid conversion from type class java.lang.Object with value '1' to type class java.sql.Time", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testSQLXMLToStringTransform() throws Exception {
        StringBuffer xml = new StringBuffer();
        int iters = DataTypeManager.MAX_STRING_LENGTH/10;
        for (int i = 0; i < iters; i++) {
            if (i < iters/2) {
                xml.append("<opentag>1"); //$NON-NLS-1$
            } else {
                xml.append("</opentag>"); //$NON-NLS-1$
            }
        }

        String expected = ""; //$NON-NLS-1$
        expected += xml.substring(0, DataTypeManager.MAX_STRING_LENGTH - expected.length());

        helpTestTransform(new StringToSQLXMLTransform().transformDirect(xml.toString()), expected);
    }

    @Test public void testStringToTimestampOutOfRange() throws Exception {
        helpTransformException("2005-13-01 11:13:01", DefaultDataClasses.TIMESTAMP, null); //$NON-NLS-1$
    }

    @Test public void testStringToTimeTimestampWithWS() throws Exception {
        helpTestTransform(" 2005-12-01 11:13:01 ", TimestampUtil.createTimestamp(105, 11, 1, 11, 13, 1, 0)); //$NON-NLS-1$
    }

    @Test public void testStringToTimestampFails() throws Exception {
        helpTransformException("2005-12-01 11:88:60", Timestamp.class, "TEIID10060 The string representation '2005-12-01 11:88:60' of a Timestamp value is not valid."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testStringToTimestampDSTTransition() throws Exception {
        //use a DST time zone
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("America/New_York")); //$NON-NLS-1$
        try {
            helpTestTransform("2016-03-13 02:00:00", TimestampUtil.createTimestamp(116, 2, 13, 3, 0, 0, 0)); //$NON-NLS-1$
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testStringToLongWithWS() throws Exception {
        helpTestTransform(" 1 ", Long.valueOf(1)); //$NON-NLS-1$
    }

    @Test public void testEngineeringNotationFloatToBigInteger() throws Exception {
        helpTestTransform(Float.MIN_VALUE, new BigDecimal(Float.MIN_VALUE).toBigInteger());
    }

    @Test public void testRangeCheck() throws Exception {
        helpTransformException(300, DataTypeManager.DefaultDataClasses.BYTE, "TEIID10058 The Integer value '300' is outside the of range for Byte"); //$NON-NLS-1$
    }

    @Test public void testRangeCheck1() throws Exception {
        Double value = new Double("1E11");//$NON-NLS-1$
        helpTransformException(value, DataTypeManager.DefaultDataClasses.INTEGER, CorePlugin.Util.gs(CorePlugin.Event.TEIID10058, value, Double.class.getSimpleName(), Integer.class.getSimpleName())); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPrimitiveArrayConversion() throws Exception {
        Object val = DataTypeManager.transformValue(new long[] {1}, DataTypeManager.DefaultDataClasses.OBJECT, Long[].class);
        assertEquals(new ArrayImpl(new Long[]{Long.valueOf(1)}), val);
    }

}
