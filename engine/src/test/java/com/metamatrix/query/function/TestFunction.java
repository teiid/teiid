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

package com.metamatrix.query.function;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.unittest.TimestampUtil;
import com.metamatrix.query.util.CommandContext;

public class TestFunction extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestFunction(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    private void helpConcat(String s1, String s2, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.concat(s1, s2);
        assertEquals("concat(" + s1 + ", " + s2 + ") failed.", expected, actual);	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTrim(String str, boolean left, Object expected) {
        Object actual = null;
        if (left) {
            actual = FunctionMethods.leftTrim(str);
            assertEquals("ltrim(" + str + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            actual = FunctionMethods.rightTrim(str);
            assertEquals("rtrim(" + str + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public static void helpLeft(String str, int count, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.left(str, new Integer(count));
        assertEquals("left(" + str + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void helpRight(String str, int count, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.right(str, new Integer(count));
        assertEquals("right(" + str + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void helpReplace(String str, String sub, String replace, Object expected) {
        Object actual = FunctionMethods.replace(str, sub, replace);
        assertEquals("replace(" + str + "," + sub + "," + replace + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public static void helpSubstring(String str, Integer start, Integer length, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.substring(str, start, length);
        assertEquals("substring(" + str + "," + start + "," + length + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public static void helpSubstring(String str, Integer start, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.substring(str, start);
        assertEquals("substring(" + str + "," + start + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpConvert(Object src, String tgtType, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.convert(src, tgtType);
        assertEquals("convert(" + src + "," + tgtType + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpConvertFail(Object src, String tgtType) {
        try {
            FunctionMethods.convert(src, tgtType);
            fail("Expected convert(" + src + "," + tgtType + ") to throw FunctionExecutionException, but it did not."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } catch (FunctionExecutionException e) {
        } 
    }

    public static void helpTestInitCap(String input, String expected) {
        String actual = (String) FunctionMethods.initCap(input);
        assertEquals("Didn't get expected result from initCap", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestLpad(String input, int length, String expected) throws FunctionExecutionException {
        String actual = (String) FunctionMethods.lpad(input, new Integer(length));
        assertEquals("Didn't get expected result from lpad", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestLpad(String input, int length, String pad, String expected) throws FunctionExecutionException {
        String actual = (String) FunctionMethods.lpad(input, new Integer(length), pad);
        assertEquals("Didn't get expected result from lpad", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestRpad(String input, int length, String expected) throws FunctionExecutionException {
        String actual = (String) FunctionMethods.rpad(input, new Integer(length));
        assertEquals("Didn't get expected result from rpad", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestRpad(String input, int length, String c, String expected) throws FunctionExecutionException {
        String actual = (String) FunctionMethods.rpad(input, new Integer(length), c);
        assertEquals("Didn't get expected result from rpad", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestTranslate(String input, String src, String dest, String expected) throws FunctionExecutionException {
        String actual = (String) FunctionMethods.translate(input, src, dest);
        assertEquals("Didn't get expected result from translate", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestLocate(String locateString, String input, int expectedLocation) throws FunctionExecutionException {
        Integer location = (Integer) FunctionMethods.locate(locateString, input);
        int actualLocation = location.intValue();
        assertEquals("Didn't get expected result from locate", expectedLocation, actualLocation); //$NON-NLS-1$
    }

    public static void helpTestLocate(String locateString, String input, Integer start, int expectedLocation) {
        Integer location = (Integer) FunctionMethods.locate(locateString, input, start);
        int actualLocation = location.intValue();
        assertEquals("Didn't get expected result from locate", expectedLocation, actualLocation); //$NON-NLS-1$
    }

    public static void helpTestRound(Integer number, Integer places, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public static void helpTestRound(Float number, Integer places, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestRound(Double number, Integer places, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestRound(BigDecimal number, Integer places, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestHour1(String timeStr, int expected) throws FunctionExecutionException {
        Time t = Time.valueOf(timeStr);
        Object actual = FunctionMethods.hour(t);
        assertEquals("hour(" + t + ") failed", new Integer(expected), actual); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void helpTestHour2(String timestampStr, int expected) throws FunctionExecutionException {
        Timestamp ts = Timestamp.valueOf(timestampStr);
        Object actual = FunctionMethods.hour(ts);
        assertEquals("hour(" + ts + ") failed", new Integer(expected), actual); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void helpTestTimestampCreate(java.sql.Date date, Time time, String expected) {
        Object actual = FunctionMethods.timestampCreate(date, time);
        assertEquals("timestampCreate(" + date + ", " + time + ") failed", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
                     expected, actual.toString()); 
    }

    public static void helpTestTimestampDiff(String intervalType, Timestamp timeStamp1, Timestamp timeStamp2, Long expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.timestampDiff(intervalType, timeStamp1, timeStamp2);
        assertEquals("timestampDiff(" + intervalType + ", " + timeStamp1 + ", " + timeStamp2 + ") failed", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                     expected, actual); 

        // test reverse - should be 
        Long expected2 = new Long(0 - expected.longValue());
        Object actual2 = FunctionMethods.timestampDiff(intervalType, timeStamp2, timeStamp1);
        assertEquals("timestampDiff(" + intervalType + ", " + timeStamp2 + ", " + timeStamp1 + ") failed", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                     expected2, actual2); 
    }
    
    public static void helpTestTimestampDiff(String intervalType, Time timeStamp1, Time timeStamp2, Long expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.timestampDiff(intervalType, timeStamp1, timeStamp2);
        assertEquals("timestampDiff(" + intervalType + ", " + timeStamp1 + ", " + timeStamp2 + ") failed", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                     expected, actual); 

        // test reverse - should be 
        Long expected2 = new Long(0 - expected.longValue());
        Object actual2 = FunctionMethods.timestampDiff(intervalType, timeStamp2, timeStamp1);
        assertEquals("timestampDiff(" + intervalType + ", " + timeStamp2 + ", " + timeStamp1 + ") failed", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                     expected2, actual2); 
    }

    public static void helpTestParseTimestamp(String tsStr, String format, String expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.parseTimestamp(tsStr, format);
        assertEquals("parseTimestamp(" + tsStr + ", " + format + ") failed", expected.toString(), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                     new Constant(actual).toString()); 
    }

    // ################################## ACTUAL TESTS ################################

    // ------------------------------ CONCAT ------------------------------

    public void testConcat1() throws FunctionExecutionException {
        helpConcat("x", "y", "xy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testConcat5() throws FunctionExecutionException {
        helpConcat("", "", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // ------------------------------ TRIM ------------------------------

    public void testTrim3() throws FunctionExecutionException {
        helpTrim("", true, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim4() throws FunctionExecutionException {
        helpTrim("", false, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim5() throws FunctionExecutionException {
        helpTrim("x", true, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim6() throws FunctionExecutionException {
        helpTrim("x", false, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim7() throws FunctionExecutionException {
        helpTrim("  x", true, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim8() throws FunctionExecutionException {
        helpTrim(" x ", true, "x "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim9() throws FunctionExecutionException {
        helpTrim("x  ", false, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim10() throws FunctionExecutionException {
        helpTrim(" x x ", false, " x x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim11() throws FunctionExecutionException {
        helpTrim("  ", true, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTrim12() throws FunctionExecutionException {
        helpTrim("  ", false, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ LEFT ------------------------------

    public void testLeft1() throws FunctionExecutionException {
        helpLeft("abcd", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLeft2() throws FunctionExecutionException {
        helpLeft("abcd", 3, "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLeft4() throws FunctionExecutionException {
        helpLeft("", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLeft5() throws FunctionExecutionException {
        helpLeft("", 2, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLeft6() throws FunctionExecutionException {
        helpLeft("abcd", 5, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLeft7() throws FunctionExecutionException {
        helpLeft("abcd", 4, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ RIGHT ------------------------------

    public void testRight1() throws FunctionExecutionException {
        helpRight("abcd", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRight2() throws FunctionExecutionException {
        helpRight("abcd", 3, "bcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRight4() throws FunctionExecutionException {
        helpRight("", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRight5() throws FunctionExecutionException {
        helpRight("", 2, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRight6() throws FunctionExecutionException {
        helpRight("abcd", 5, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRight7() throws FunctionExecutionException {
        helpRight("abcd", 4, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ SUBSTRING ------------------------------

    public void testSubstring1() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(1), new Integer(1), "a"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring2() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(2), new Integer(2), "bc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring3() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(3), new Integer(3), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring4() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(3), new Integer(0), ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring6() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(3), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring7() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(1), "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSubstring8() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(-1), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSubstring9() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(-3), "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubstring10() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(-4), null); //$NON-NLS-1$ 
    }
    
    public void testSubstring11() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(-1), new Integer(2), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSubstring12() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(-3), new Integer(2), "ab"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSubstring13() throws FunctionExecutionException {
        helpSubstring("abc", new Integer(0), new Integer(2), "ab"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    
    // ------------------------------ REPLACE ------------------------------

    public void testReplace1() throws FunctionExecutionException {
        helpReplace("", "x", "y", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace2() throws FunctionExecutionException {
        helpReplace("", "", "z", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace3() throws FunctionExecutionException {
        helpReplace("x", "x", "y", "y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace4() throws FunctionExecutionException {
        helpReplace("xx", "x", "y", "yy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace5() throws FunctionExecutionException {
        helpReplace("x x", "x", "y", "y y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace6() throws FunctionExecutionException {
        helpReplace("x x", "x", "", " "); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace7() throws FunctionExecutionException {
        helpReplace("x x", "x", "yz", "yz yz"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testReplace8() throws FunctionExecutionException {
        helpReplace("xx xx", "xx", "y", "y y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    // ------------------------------ CONVERT ------------------------------

    public void testConvertStringBoolean1() throws FunctionExecutionException {
        helpConvert("true", "boolean", Boolean.TRUE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBoolean2() throws FunctionExecutionException {
        helpConvert("false", "boolean", Boolean.FALSE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBoolean3() throws FunctionExecutionException {
        helpConvert("x", "boolean", Boolean.FALSE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBoolean4() throws FunctionExecutionException {
        helpConvert("TrUe", "boolean", Boolean.TRUE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBoolean5() throws FunctionExecutionException {
        helpConvert("FAlsE", "boolean", Boolean.FALSE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringChar1() throws FunctionExecutionException {
        helpConvert("a", "char", new Character('a')); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringChar2() {
        helpConvertFail("xx", "char"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringByte1() throws FunctionExecutionException {
        helpConvert("5", "byte", new Byte((byte) 5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringShort1() throws FunctionExecutionException {
        helpConvert("5", "short", new Short((short) 5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringInteger1() throws FunctionExecutionException {
        helpConvert("5", "integer", new Integer(5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Integer > Integer.MAX_VALUE - should fail
    public void testConvertStringInteger2() {
        helpConvertFail("" + Integer.MAX_VALUE + "1", "integer"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testConvertStringInteger3() {
        helpConvertFail("5.99", "integer"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringLong1() throws FunctionExecutionException {
        helpConvert("5", "long", new Long(5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBigInteger1() throws FunctionExecutionException {
        helpConvert("5", "biginteger", new BigInteger("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testConvertStringBigInteger2() throws FunctionExecutionException {
        String bigInt = "" + Integer.MAX_VALUE + "111"; //$NON-NLS-1$ //$NON-NLS-2$
        helpConvert(bigInt, "biginteger", new BigInteger(bigInt)); //$NON-NLS-1$
    }

    public void testConvertStringFloat1() throws FunctionExecutionException {
        helpConvert("5.2", "float", new Float(5.2f)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringDouble1() throws FunctionExecutionException {
        helpConvert("5.2", "double", new Double(5.2d)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertStringBigDecimal1() throws FunctionExecutionException {
        helpConvert("5.2", "bigdecimal", new BigDecimal("5.2")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testConvertDoubleBigInteger() throws FunctionExecutionException {
        helpConvert(new Double(1.0d), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertFloatBigInteger() throws FunctionExecutionException {
        helpConvert(new Float(1.0), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertBigDecimalBigInteger() throws FunctionExecutionException {
        helpConvert(new BigDecimal("1.0"), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testConvertDoubleLong() throws FunctionExecutionException {
        helpConvert(new Double(1.0d), "long", new Long("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConvertTimestampString() throws FunctionExecutionException {
        Timestamp ts = TimestampUtil.createTimestamp(103, 7, 22, 22, 43, 53, 3333333);
        helpConvert(ts, "string", "2003-08-22 22:43:53.003333333"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testAscii1() throws FunctionExecutionException {
        Integer code = (Integer) FunctionMethods.ascii(new Character(' '));
        assertEquals("Didn't get expected code", 32, code.intValue()); //$NON-NLS-1$
    }

    public void testAscii2() throws FunctionExecutionException {
        Integer code = (Integer) FunctionMethods.ascii(" "); //$NON-NLS-1$
        assertEquals("Didn't get expected code", 32, code.intValue()); //$NON-NLS-1$
    }

    public void testAscii4() {
        try {
            FunctionMethods.ascii(""); //$NON-NLS-1$
            fail("Expected function exception"); //$NON-NLS-1$
        } catch (FunctionExecutionException e) {
        } 
    }

    public void testAscii5() throws FunctionExecutionException {
        Integer code = (Integer) FunctionMethods.ascii("abc"); //$NON-NLS-1$
        assertEquals("Didn't get expected code", 97, code.intValue()); //$NON-NLS-1$
    }

    public void testChr1() {
        Character chr = (Character) FunctionMethods.chr(new Integer(32));
        assertEquals("Didn't get expected character", ' ', chr.charValue()); //$NON-NLS-1$
    }

    public void testNvl1() {
        String ret = (String) FunctionMethods.ifnull("x", "y"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Didn't get expected value", "x", ret); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testNvl2() {
        String ret = (String) FunctionMethods.ifnull(null, "y"); //$NON-NLS-1$
        assertEquals("Didn't get expected value", "y", ret); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testNvl3() {
        String ret = (String) FunctionMethods.ifnull(null, null);
        assertEquals("Didn't get expected value", null, ret); //$NON-NLS-1$
    }

    public void testInitCap2() throws Exception {
        helpTestInitCap("abc", "Abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInitCap3() throws FunctionExecutionException {
        helpTestInitCap(" test    some\tweird\rspaces\nhere", " Test    Some\tWeird\rSpaces\nHere"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInitCap4() throws FunctionExecutionException {
        helpTestInitCap("x y ", "X Y "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInitCap5() throws FunctionExecutionException {
        helpTestInitCap("cows are FUN", "Cows Are Fun"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLpad1() throws FunctionExecutionException {
        helpTestLpad("x", 4, "   x");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLpad3() throws FunctionExecutionException {
        helpTestLpad("x", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLpad4() throws FunctionExecutionException {
        helpTestLpad("xx", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLpad5() throws FunctionExecutionException {
        helpTestLpad("", 4, "x", "xxxx");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testLpad6() throws FunctionExecutionException {
        helpTestLpad("10", 6, "0", "000010"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testLpad7() throws FunctionExecutionException {
    	helpTestLpad("x", 4, "yq", "qyqx" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    }

    public void testRpad1() throws FunctionExecutionException {
        helpTestRpad("x", 4, "x   "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRpad3() throws FunctionExecutionException {
        helpTestRpad("x", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRpad4() throws FunctionExecutionException {
        helpTestRpad("xx", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRpad5() throws FunctionExecutionException {
        helpTestRpad("", 4, "x", "xxxx"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testRpad6() throws FunctionExecutionException {
        helpTestRpad("10", 6, "0", "100000"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testTranslate1() throws FunctionExecutionException {
        helpTestTranslate("This is my test", "ty", "yt", "This is mt yesy");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testTranslate2() throws FunctionExecutionException {
        helpTestTranslate("", "ty", "yt", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testTranslate3() {
        try {
            FunctionMethods.translate("test", "x", "yz"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            fail("Did not get expected exception on differing src and dest lengths"); //$NON-NLS-1$
        } catch (FunctionExecutionException e) {
        }
    }

    public void testTranslate4() throws FunctionExecutionException {
        helpTestTranslate("test", "xy", "ab", "test"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testLocate1() throws FunctionExecutionException {
        helpTestLocate(",", "Metamatrix, John Quincy", 11); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLocate2() throws FunctionExecutionException {
        helpTestLocate(" ", "Metamatrix, John Quincy", 12); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLocate3() throws FunctionExecutionException {
        helpTestLocate("x", "xx", 1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLocate4() throws FunctionExecutionException {
        helpTestLocate("y", "xx", 0); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLocate5() throws Exception {
        helpTestLocate("b", "abab", 3, 4); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLocate6() throws Exception {
        helpTestLocate("z", "abab", 0, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLocate7() throws Exception {
        helpTestLocate("z", "abab", null, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testLocate8() throws Exception {
        helpTestLocate("z", "abab", -1, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBitand() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitand(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0x0F0", 0x0F0, result.intValue()); //$NON-NLS-1$
    }

    public void testBitor() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitor(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xFFF", 0xFFF, result.intValue()); //$NON-NLS-1$
    }

    public void testBitxor() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitxor(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xF0F", 0xF0F, result.intValue()); //$NON-NLS-1$
    }

    public void testBitnot() {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitnot(new Integer(0xF0F));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xFFFFF0F0", 0xFFFFF0F0, result.intValue()); //$NON-NLS-1$
    }

    public void testRoundInteger1() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(0), new Integer(1928));
    }

    public void testRoundInteger2() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(-1), new Integer(1930));
    }

    public void testRoundInteger3() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(-2), new Integer(1900));
    }

    public void testRoundInteger4() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(-3), new Integer(2000));
    }

    public void testRoundInteger5() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(-4), new Integer(0));
    }

    public void testRoundInteger6() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(-5), new Integer(0));
    }

    public void testRoundInteger7() throws FunctionExecutionException {
        helpTestRound(new Integer(1928), new Integer(1), new Integer(1928));
    }

    public void testRoundFloat1() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(4), new Float(123.456F));
    }

    public void testRoundFloat2() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(3), new Float(123.456F));
    }

    public void testRoundFloat3() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(2), new Float(123.46F));
    }

    public void testRoundFloat4() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(1), new Float(123.5F));
    }

    public void testRoundFloat5() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(0), new Float(123F));
    }

    public void testRoundFloat6() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(-1), new Float(120F));
    }

    public void testRoundFloat7() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(-2), new Float(100F));
    }

    public void testRoundFloat8() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(-3), new Float(0F));
    }

    public void testRoundFloat9() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(-4), new Float(0F));
    }
    
    public void testRoundFloat10() throws FunctionExecutionException {
        helpTestRound(new Float(123.456F), new Integer(4000), new Float(123.456F));
    }

    public void testRoundDouble1() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(4), new Double(123.456));
    }

    public void testRoundDouble2() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(3), new Double(123.456));
    }

    public void testRoundDouble3() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(2), new Double(123.46));
    }

    public void testRoundDouble4() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(1), new Double(123.5));
    }

    public void testRoundDouble5() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(0), new Double(123));
    }

    public void testRoundDouble6() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(-1), new Double(120));
    }

    public void testRoundDouble7() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(-2), new Double(100));
    }

    public void testRoundDouble8() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(-3), new Double(0));
    }

    public void testRoundDouble9() throws FunctionExecutionException {
        helpTestRound(new Double(123.456), new Integer(-4), new Double(0));
    }
    
    public void testRoundDouble10() throws FunctionExecutionException {
        helpTestRound(new Double(-3.5), new Integer(0), new Double(-4));
    }
    
    public void testRoundBigDecimal1() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(4), new BigDecimal("123.456")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal2() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(3), new BigDecimal("123.456")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal3() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(2), new BigDecimal("123.460")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal4() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(1), new BigDecimal("123.500")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal5() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(0), new BigDecimal("123.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal6() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(-1), new BigDecimal("120.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal7() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(-2), new BigDecimal("100.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal8() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(-3), new BigDecimal("0.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRoundBigDecimal9() throws FunctionExecutionException {
        helpTestRound(new BigDecimal("123.456"), new Integer(-4), new BigDecimal("0.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testHour1() throws FunctionExecutionException {
        helpTestHour1("00:00:00", 0); //$NON-NLS-1$
    }

    public void testHour2() throws FunctionExecutionException {
        helpTestHour1("11:00:00", 11); //$NON-NLS-1$
    }

    public void testHour3() throws FunctionExecutionException {
        helpTestHour1("12:00:00", 12); //$NON-NLS-1$
    }

    public void testHour4() throws FunctionExecutionException {
        helpTestHour1("13:00:00", 13); //$NON-NLS-1$
    }

    public void testHour5() throws FunctionExecutionException {
        helpTestHour1("23:59:59", 23); //$NON-NLS-1$
    }

    public void testHour6() throws FunctionExecutionException {
        helpTestHour2("2002-01-01 00:00:00", 0); //$NON-NLS-1$
    }

    public void testHour7() throws FunctionExecutionException {
        helpTestHour2("2002-01-01 11:00:00", 11); //$NON-NLS-1$
    }

    public void testHour8() throws FunctionExecutionException {
        helpTestHour2("2002-01-01 12:00:00", 12); //$NON-NLS-1$
    }

    public void testHour9() throws FunctionExecutionException {
        helpTestHour2("2002-01-01 13:00:00", 13); //$NON-NLS-1$
    }

    public void testHour10() throws FunctionExecutionException {
        helpTestHour2("2002-01-01 23:59:59", 23); //$NON-NLS-1$
    }

    public void testTimestampCreate1() {
        helpTestTimestampCreate(TimestampUtil.createDate(103, 11, 1), TimestampUtil.createTime(23, 59, 59), "2003-12-01 23:59:59.0"); //$NON-NLS-1$
    }

    public void testTimestampAdd1() throws Exception {
        assertEquals(TimestampUtil.createDate(103, 11, 4), FunctionMethods.timestampAdd(ReservedWords.SQL_TSI_DAY, 3, TimestampUtil.createDate(103, 11, 1))); 
    }

    public void testTimestampAdd2() throws Exception {
    	assertEquals(TimestampUtil.createTimestamp(103, 11, 1, 18, 20, 30, 0), FunctionMethods.timestampAdd(ReservedWords.SQL_TSI_HOUR, 3, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 0)));
    }

    public void testTimestampAdd3() throws Exception {
    	assertEquals(TimestampUtil.createTime(11, 50, 30), FunctionMethods.timestampAdd(ReservedWords.SQL_TSI_MINUTE, 90, TimestampUtil.createTime(10, 20, 30)));
    }

    public void testTimestampDiffTimeStamp_FracSec_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND, 
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 1),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 100000000),
                              new Long(99999999));
    }

    public void testTimestampDiffTimeStamp_FracSec_2() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              // 1 day (8.64 x 10^10 nanos) and 1 nano
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 3, 9, 35, 3),
                              new Long(86400000000001L));
    }

    public void testTimestampDiffTimeStamp_FracSec_3() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              // 1 day (8.64 x 10^10 nanos) less 1 nano
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 3),
                              new Long(-86399999999999L));
    }

    public void testTimestampDiffTimeStamp_FracSec_4() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 0, 0, 0, 3),
                              new Long(00000002));
    }

    public void testTimestampDiffTimeStamp_FracSec_5() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 0, 0, 0, 10),
                              new Long(9));
    }

    public void testTimestampDiffTimeStamp_FracSec_6() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 0, 0, 0, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 0, 0, 0, 3),
                              new Long(1));
    }

    public void testTimestampDiffTimeStamp_FracSec_7() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              // 1 nano diff
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 3),
                              new Long(1));
    }

    public void testTimestampDiffTimeStamp_FracSec_8() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_FRAC_SECOND,
                              // 1 nano diff
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 3),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 2),
                              new Long(-1));
    }

    public void testTimestampDiffTimeStamp_Min_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp(0, 0, 0, 2, 34, 12, 0),
                              TimestampUtil.createTimestamp(0, 0, 0, 12, 0, 0, 0),
                              new Long(565));
    }

    public void testTimestampDiffTimeStamp_Min_2() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 0, 0, 2, 0, 0, 0),
                              TimestampUtil.createTimestamp((2001-1900), 0, 0, 0, 33, 12, 0),
                              new Long(-86));
    }

    public void testTimestampDiffTimeStamp_Min_3() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 07, 58, 65497),
                              TimestampUtil.createTimestamp((2001-1900), 8, 29, 11, 25, 42, 483219),
                              new Long(4277));
    }

    public void testTimestampDiffTimeStamp_Min_4() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 07, 58, 0),
                              TimestampUtil.createTimestamp((2001-1900), 8, 29, 11, 25, 42, 0),
                              new Long(4277));
    }

    public void testTimestampDiffTimeStamp_Min_5() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 0, 0, 1),
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 0, 0, 0),
                              new Long(0));
    }

    public void testTimestampDiffTimeStamp_Hour_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_HOUR,
                              TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 59, 59, 999999999),
                              new Long(0));
    }

    public void testTimestampDiffTimeStamp_Week_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_WEEK,
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 100),
                              TimestampUtil.createTimestamp((2001-1900), 4, 2, 5, 19, 35, 500),
                              new Long(-7));
    }

    public void testTimestampDiffTimeStamp_Month_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 19, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 20, 12, 0, 0, 0),
                              new Long(7));
    }

    public void testTimestampDiffTimeStamp_Month_2() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 5, 1, 0, 0, 0, 1000000),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 12, 0, 0, 1),
                              new Long(6));
    }

    public void testTimestampDiffTimeStamp_Month_3() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 19, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2004-1900), 11, 18, 12, 0, 0, 1000000),
                              new Long(7));
    }

    public void testTimestampDiffTimeStamp_Month_4() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 0, 1000000),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 0, 0),
                              new Long(7));
    }

    public void testTimestampDiffTimeStamp_Month_5() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 1, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 0, 0),
                              new Long(7));
    }

    public void testTimestampDiffTimeStamp_Month_6() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 1, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 2, 0),
                              new Long(7));
    }

    public void testTimestampDiffTimeStamp_Day_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_DAY,
                              TimestampUtil.createTimestamp((2004-1900), 2, 1, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 3, 1, 0, 0, 0, 0),
                              new Long(31));
    }

    public void testTimestampDiffTimeStamp_Day_2() throws FunctionExecutionException {
        // Leap year
        helpTestTimestampDiff(ReservedWords.SQL_TSI_DAY,
                              TimestampUtil.createTimestamp((2004-1900), 1, 1, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 2, 1, 0, 0, 0, 0),
                              new Long(29));
    }

    public void testTimestampDiffTime_Hour_1() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_HOUR, 
                              TimestampUtil.createTime(3, 4, 45),
                              TimestampUtil.createTime(5, 5, 36),
                              new Long(2));
    }

    public void testTimestampDiffTime_Hour_2() throws FunctionExecutionException {
        helpTestTimestampDiff(ReservedWords.SQL_TSI_HOUR, 
                              TimestampUtil.createTime(5, 0, 30),
                              TimestampUtil.createTime(3, 0, 31),
                              new Long(-1));
    }

    public void testParseTimestamp1() throws FunctionExecutionException {
        helpTestParseTimestamp("1993-04-24 3:59:59 PM", "yyyy-MM-dd hh:mm:ss aa", "{ts'1993-04-24 15:59:59.0'}"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    }
    
    public void helpTestModifyTimeZone(String tsStr, String tzStart, String tzEnd, String expectedStr) throws Exception {
        Timestamp ts = tsStr != null ? Timestamp.valueOf(tsStr) : null;
        Timestamp actual = null;
        
        if(tzStart == null) {
            actual = (Timestamp) FunctionMethods.modifyTimeZone(new CommandContext(), ts, tzEnd);
        } else {
            actual = (Timestamp) FunctionMethods.modifyTimeZone(ts, tzStart, tzEnd);
        }
        
        String actualStr = null;
        if(actual != null) {
            actualStr = actual.toString();
        }
        assertEquals("Did not get expected output timestamp", expectedStr, actualStr); //$NON-NLS-1$
    }
    
    /*
     * The following test results may look odd, but it is due to the parsing of the initial date, which is done
     * against the system default timezone (not the startTz shown below).  The fianl date value is also being read
     * against the default timezone and not the endTz shown. 
     */
    public void testModifyTimeZoneGMT() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "GMT+00:00", "GMT-01:00", "2004-10-03 16:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testModifyTimeZoneGMTPartialHour() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:30:59.123456789", "GMT+00:00", "GMT-01:45", "2004-10-03 17:15:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testModifyTimeZoneNamedTZ() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/New_York", "America/Chicago", "2004-10-03 16:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    public void testModifyTimeZoneNamedTZ2() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/Chicago", "America/New_York", "2004-10-03 14:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    public void testModifyTimeZoneStartLocal() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/Chicago", "America/Los_Angeles", "2004-10-03 17:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
    }
    
    // case 2458
    public void testCurrentDate() {

        Date curDate = (Date)FunctionMethods.currentDate();
     
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(curDate);
        
        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), 0);
        assertEquals(cal1.get(Calendar.MINUTE), 0);
        assertEquals(cal1.get(Calendar.SECOND), 0);
        assertEquals(cal1.get(Calendar.MILLISECOND), 0);
             
    }

    // case 2458
    public void testCurrentTime() {
        
        Time curDate = (Time)FunctionMethods.currentTime();

        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(curDate);
        
        // can not test the current time without making a copy of current "time"
        // so check the normalization of the date to unix epoch
        assertEquals(cal1.get(Calendar.YEAR), 1970);
        assertEquals(cal1.get(Calendar.MONTH), Calendar.JANUARY);
        assertEquals(cal1.get(Calendar.DATE), 1);
              
    }    
    
    public void testRand() throws FunctionExecutionException {
        Double d = (Double)FunctionMethods.rand(new CommandContext(), new Integer(100));
        assertEquals(new Double(0.7220096548596434), d);
        
        try {
            FunctionMethods.rand(new CommandContext(), new Double(34.5));
            fail("should have failed to take a double"); //$NON-NLS-1$
        } catch (FunctionExecutionException e) {            
        }   

        FunctionMethods.rand(new CommandContext());            
    }
    
    public void testEnv() throws FunctionExecutionException {
        Properties p = new Properties();
        String envProperty = "EnvProperty"; //$NON-NLS-1$
        String systemProperty = "SystemProperty"; //$NON-NLS-1$        
        p.setProperty(envProperty.toLowerCase(), envProperty);

        // set an environment property
        CommandContext context = new CommandContext();
        context.setEnvironmentProperties(p);

        // set the system property
        System.setProperty(systemProperty, systemProperty);
        System.setProperty(systemProperty.toLowerCase(), systemProperty+"_lowercase"); //$NON-NLS-1$
        
        assertEquals(envProperty, FunctionMethods.env(context, envProperty));
        assertEquals(systemProperty, FunctionMethods.env(context, systemProperty));
        assertEquals(systemProperty+"_lowercase", FunctionMethods.env(context, systemProperty.toUpperCase())); //$NON-NLS-1$
    }
    
}
