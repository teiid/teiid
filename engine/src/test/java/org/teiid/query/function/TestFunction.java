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

package org.teiid.query.function;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;


public class TestFunction {

    @Before
    public void setUp() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-5")); //$NON-NLS-1$
    }

    @After
    public void tearDown() {
        TimestampWithTimezone.resetCalendar(null);
    }

    private void helpConcat(String s1, String s2, Object expected) {
        Object actual = FunctionMethods.concat(s1, s2);
        assertEquals("concat(" + s1 + ", " + s2 + ") failed.", expected, actual);     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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

    public static void helpSubstring(String str, Integer start, Integer length, Object expected) {
        Object actual = FunctionMethods.substring(str, start, length);
        assertEquals("substring(" + str + "," + start + "," + length + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public static void helpSubstring(String str, Integer start, Object expected) {
        Object actual = FunctionMethods.substring(str, start);
        assertEquals("substring(" + str + "," + start + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpConvert(Object src, String tgtType, Object expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.convert(null, src, tgtType);
        assertEquals("convert(" + src + "," + tgtType + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpConvertFail(Object src, String tgtType) {
        try {
            FunctionMethods.convert(null, src, tgtType);
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

    public static void helpTestLocate(String locateString, String input, int expectedLocation) {
        Integer location = (Integer) FunctionMethods.locate(locateString, input);
        int actualLocation = location.intValue();
        assertEquals("Didn't get expected result from locate", expectedLocation, actualLocation); //$NON-NLS-1$
    }

    public static void helpTestEndssWith(String locateString, String input, Boolean expected) {
        Boolean actual = (Boolean) FunctionMethods.endsWith(locateString, input);
        assertEquals("Didn't get expected result from startsWith", expected, actual); //$NON-NLS-1$
    }

    public static void helpTestLocate(String locateString, String input, Integer start, int expectedLocation) {
        Integer location = (Integer) FunctionMethods.locate(locateString, input, start);
        int actualLocation = location.intValue();
        assertEquals("Didn't get expected result from locate", expectedLocation, actualLocation); //$NON-NLS-1$
    }

    public static void helpTestRound(Integer number, Integer places, Object expected) {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestRound(Float number, Integer places, Object expected) {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestRound(Double number, Integer places, Object expected) {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestRound(BigDecimal number, Integer places, Object expected) {
        Object actual = FunctionMethods.round(number, places);
        assertEquals("round(" + number + "," + places + ") failed.", expected, actual); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public static void helpTestHour1(String timeStr, int expected) {
        Time t = Time.valueOf(timeStr);
        Object actual = FunctionMethods.hour(t);
        assertEquals("hour(" + t + ") failed", new Integer(expected), actual); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void helpTestHour2(String timestampStr, int expected) {
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

    public static void helpTestParseTimestamp(String tsStr, String format, String expected) throws FunctionExecutionException {
        Object actual = FunctionMethods.parseTimestamp(new CommandContext(), tsStr, format);
        assertEquals("parseTimestamp(" + tsStr + ", " + format + ") failed", expected.toString(), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                     new Constant(actual).toString());
    }

    // ################################## ACTUAL TESTS ################################

    // ------------------------------ CONCAT ------------------------------

    @Test public void testConcat1() throws Exception {
        helpConcat("x", "y", "xy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testConcat5() throws Exception {
        helpConcat("", "", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // ------------------------------ TRIM ------------------------------

    @Test public void testTrim3() throws Exception {
        helpTrim("", true, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim4() throws Exception {
        helpTrim("", false, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim5() throws Exception {
        helpTrim("x", true, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim6() throws Exception {
        helpTrim("x", false, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim7() throws Exception {
        helpTrim("  x", true, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim8() throws Exception {
        helpTrim(" x ", true, "x "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim9() throws Exception {
        helpTrim("x  ", false, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim10() throws Exception {
        helpTrim(" x x ", false, " x x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim11() throws Exception {
        helpTrim("  ", true, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTrim12() throws Exception {
        helpTrim("  ", false, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ LEFT ------------------------------

    @Test public void testLeft1() throws Exception {
        helpLeft("abcd", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeft2() throws Exception {
        helpLeft("abcd", 3, "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeft4() throws Exception {
        helpLeft("", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeft5() throws Exception {
        helpLeft("", 2, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeft6() throws Exception {
        helpLeft("abcd", 5, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeft7() throws Exception {
        helpLeft("abcd", 4, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ RIGHT ------------------------------

    @Test public void testRight1() throws Exception {
        helpRight("abcd", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRight2() throws Exception {
        helpRight("abcd", 3, "bcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRight4() throws Exception {
        helpRight("", 0, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRight5() throws Exception {
        helpRight("", 2, ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRight6() throws Exception {
        helpRight("abcd", 5, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRight7() throws Exception {
        helpRight("abcd", 4, "abcd"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ------------------------------ SUBSTRING ------------------------------

    @Test public void testSubstring1() throws Exception {
        helpSubstring("abc", new Integer(1), new Integer(1), "a"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring2() throws Exception {
        helpSubstring("abc", new Integer(2), new Integer(2), "bc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring3() throws Exception {
        helpSubstring("abc", new Integer(3), new Integer(3), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring4() throws Exception {
        helpSubstring("abc", new Integer(3), new Integer(0), ""); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring6() throws Exception {
        helpSubstring("abc", new Integer(3), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring7() throws Exception {
        helpSubstring("abc", new Integer(1), "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring8() throws Exception {
        helpSubstring("abc", new Integer(-1), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring9() throws Exception {
        helpSubstring("abc", new Integer(-3), "abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring10() throws Exception {
        helpSubstring("abc", new Integer(-4), null); //$NON-NLS-1$
    }

    @Test public void testSubstring11() throws Exception {
        helpSubstring("abc", new Integer(-1), new Integer(2), "c"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring12() throws Exception {
        helpSubstring("abc", new Integer(-3), new Integer(2), "ab"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubstring13() throws Exception {
        helpSubstring("abc", new Integer(0), new Integer(2), "ab"); //$NON-NLS-1$ //$NON-NLS-2$
    }


    // ------------------------------ REPLACE ------------------------------

    @Test public void testReplace1() throws Exception {
        helpReplace("", "x", "y", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace2() throws Exception {
        helpReplace("", "", "z", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace3() throws Exception {
        helpReplace("x", "x", "y", "y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace4() throws Exception {
        helpReplace("xx", "x", "y", "yy"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace5() throws Exception {
        helpReplace("x x", "x", "y", "y y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace6() throws Exception {
        helpReplace("x x", "x", "", " "); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace7() throws Exception {
        helpReplace("x x", "x", "yz", "yz yz"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplace8() throws Exception {
        helpReplace("xx xx", "xx", "y", "y y"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    // ------------------------------ CONVERT ------------------------------

    @Test public void testConvertStringBoolean1() throws Exception {
        helpConvert("true", "boolean", Boolean.TRUE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBoolean2() throws Exception {
        helpConvert("false", "boolean", Boolean.FALSE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBoolean3() throws Exception {
        helpConvert("x", "boolean", Boolean.TRUE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBoolean4() throws Exception {
        helpConvert("TrUe", "boolean", Boolean.TRUE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBoolean5() throws Exception {
        helpConvert("FAlsE", "boolean", Boolean.FALSE); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringChar1() throws Exception {
        helpConvert("a", "char", new Character('a')); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringChar2() throws Exception {
        helpConvert("xx", "char", new Character('x')); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringByte1() throws Exception {
        helpConvert("5", "byte", new Byte((byte) 5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringShort1() throws Exception {
        helpConvert("5", "short", new Short((short) 5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringInteger1() throws Exception {
        helpConvert("5", "integer", new Integer(5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Integer > Integer.MAX_VALUE - should fail
    @Test public void testConvertStringInteger2() throws Exception {
        helpConvertFail("" + Integer.MAX_VALUE + "1", "integer"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testConvertStringInteger3() throws Exception {
        helpConvertFail("5.99", "integer"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringLong1() throws Exception {
        helpConvert("5", "long", new Long(5)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBigInteger1() throws Exception {
        helpConvert("5", "biginteger", new BigInteger("5")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testConvertStringBigInteger2() throws Exception {
        String bigInt = "" + Integer.MAX_VALUE + "111"; //$NON-NLS-1$ //$NON-NLS-2$
        helpConvert(bigInt, "biginteger", new BigInteger(bigInt)); //$NON-NLS-1$
    }

    @Test public void testConvertStringFloat1() throws Exception {
        helpConvert("5.2", "float", new Float(5.2f)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringDouble1() throws Exception {
        helpConvert("5.2", "double", new Double(5.2d)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertStringBigDecimal1() throws Exception {
        helpConvert("5.2", "bigdecimal", new BigDecimal("5.2")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testConvertDoubleBigInteger() throws Exception {
        helpConvert(new Double(1.0d), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertFloatBigInteger() throws Exception {
        helpConvert(new Float(1.0), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertBigDecimalBigInteger() throws Exception {
        helpConvert(new BigDecimal("1.0"), "biginteger", new BigInteger("1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testConvertDoubleLong() throws Exception {
        helpConvert(new Double(1.0d), "long", new Long("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testConvertTimestampString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 7, 22, 22, 43, 53, 3333333);
        helpConvert(ts, "string", "2003-08-22 22:43:53.003333333"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testAscii2() throws Exception {
        Integer code = FunctionMethods.ascii(" "); //$NON-NLS-1$
        assertEquals("Didn't get expected code", 32, code.intValue()); //$NON-NLS-1$
    }

    @Test public void testAscii4() throws Exception {
        assertNull(FunctionMethods.ascii("")); //$NON-NLS-1$
    }

    @Test public void testAscii5() throws Exception {
        Integer code = FunctionMethods.ascii("abc"); //$NON-NLS-1$
        assertEquals("Didn't get expected code", 97, code.intValue()); //$NON-NLS-1$
    }

    @Test public void testChr1() throws Exception {
        Character chr = (Character) FunctionMethods.chr(new Integer(32));
        assertEquals("Didn't get expected character", ' ', chr.charValue()); //$NON-NLS-1$
    }

    @Test public void testNvl1() throws Exception {
        String ret = (String) FunctionMethods.ifnull("x", "y"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Didn't get expected value", "x", ret); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNvl2() throws Exception {
        String ret = (String) FunctionMethods.ifnull(null, "y"); //$NON-NLS-1$
        assertEquals("Didn't get expected value", "y", ret); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNvl3() throws Exception {
        String ret = (String) FunctionMethods.ifnull(null, null);
        assertEquals("Didn't get expected value", null, ret); //$NON-NLS-1$
    }

    @Test public void testInitCap2() throws Exception {
        helpTestInitCap("abc", "Abc"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInitCap3() throws Exception {
        helpTestInitCap(" test    some\tweird\rspaces\nhere", " Test    Some\tWeird\rSpaces\nHere"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInitCap4() throws Exception {
        helpTestInitCap("x y ", "X Y "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInitCap5() throws Exception {
        helpTestInitCap("cows are FUN", "Cows Are Fun"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInitCap6() throws Exception {
        helpTestInitCap("𐐩ome chars are fun 𐐨!", "𐐁ome Chars Are Fun 𐐀!"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLpad1() throws Exception {
        helpTestLpad("x", 4, "   x");     //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLpad3() throws Exception {
        helpTestLpad("x", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLpad4() throws Exception {
        helpTestLpad("xx", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLpad5() throws Exception {
        helpTestLpad("", 4, "x", "xxxx");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testLpad6() throws Exception {
        helpTestLpad("10", 6, "0", "000010"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testLpad7() throws Exception {
        helpTestLpad("x", 4, "yq", "qyqx" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testRpad1() throws Exception {
        helpTestRpad("x", 4, "x   "); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRpad3() throws Exception {
        helpTestRpad("x", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRpad4() throws Exception {
        helpTestRpad("xx", 1, "x"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRpad5() throws Exception {
        helpTestRpad("", 4, "x", "xxxx"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testRpad6() throws Exception {
        helpTestRpad("10", 6, "0", "100000"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testTranslate1() throws Exception {
        helpTestTranslate("This is my test", "ty", "yt", "This is mt yesy");     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testTranslate2() throws Exception {
        helpTestTranslate("", "ty", "yt", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testTranslate3() throws Exception {
        try {
            FunctionMethods.translate("test", "x", "yz"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            fail("Did not get expected exception on differing src and dest lengths"); //$NON-NLS-1$
        } catch (FunctionExecutionException e) {
        }
    }

    @Test public void testTranslate4() throws Exception {
        helpTestTranslate("test", "xy", "ab", "test"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testLocate1() throws Exception {
        helpTestLocate(",", "Metamatrix, John Quincy", 11); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTranslate5() throws Exception {
        helpTestTranslate("𐀀 a", "𐀀", "b", "b a"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testLocate2() throws Exception {
        helpTestLocate(" ", "Metamatrix, John Quincy", 12); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate3() throws Exception {
        helpTestLocate("x", "xx", 1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate4() throws Exception {
        helpTestLocate("y", "xx", 0); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate5() throws Exception {
        helpTestLocate("b", "abab", 3, 4); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate6() throws Exception {
        helpTestLocate("z", "abab", 0, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate7() throws Exception {
        helpTestLocate("z", "abab", null, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLocate8() throws Exception {
        helpTestLocate("z", "abab", -1, 0); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEndsWith1() throws Exception {
        helpTestEndssWith("z", "abab", false); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEndsWith2() throws Exception {
        helpTestEndssWith("b", "abab", true); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBitand() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitand(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0x0F0", 0x0F0, result.intValue()); //$NON-NLS-1$
    }

    @Test public void testBitor() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitor(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xFFF", 0xFFF, result.intValue()); //$NON-NLS-1$
    }

    @Test public void testBitxor() throws Exception {
        // Both values are integers
        Integer result = (Integer) FunctionMethods.bitxor(new Integer(0xFFF), new Integer(0x0F0));
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xF0F", 0xF0F, result.intValue()); //$NON-NLS-1$
    }

    @Test public void testBitnot() throws Exception {
        // Both values are integers
        Integer result = FunctionMethods.bitnot(0xF0F);
        assertNotNull("Result should not be null", result); //$NON-NLS-1$
        assertEquals("result should be 0xFFFFF0F0", 0xFFFFF0F0, result.intValue()); //$NON-NLS-1$
    }

    @Test public void testRoundInteger1() throws Exception {
        helpTestRound(new Integer(1928), new Integer(0), new Integer(1928));
    }

    @Test public void testRoundInteger2() throws Exception {
        helpTestRound(new Integer(1928), new Integer(-1), new Integer(1930));
    }

    @Test public void testRoundInteger3() throws Exception {
        helpTestRound(new Integer(1928), new Integer(-2), new Integer(1900));
    }

    @Test public void testRoundInteger4() throws Exception {
        helpTestRound(new Integer(1928), new Integer(-3), new Integer(2000));
    }

    @Test public void testRoundInteger5() throws Exception {
        helpTestRound(new Integer(1928), new Integer(-4), new Integer(0));
    }

    @Test public void testRoundInteger6() throws Exception {
        helpTestRound(new Integer(1928), new Integer(-5), new Integer(0));
    }

    @Test public void testRoundInteger7() throws Exception {
        helpTestRound(new Integer(1928), new Integer(1), new Integer(1928));
    }

    @Test public void testRoundFloat1() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(4), new Float(123.456F));
    }

    @Test public void testRoundFloat2() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(3), new Float(123.456F));
    }

    @Test public void testRoundFloat3() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(2), new Float(123.46F));
    }

    @Test public void testRoundFloat4() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(1), new Float(123.5F));
    }

    @Test public void testRoundFloat5() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(0), new Float(123F));
    }

    @Test public void testRoundFloat6() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(-1), new Float(120F));
    }

    @Test public void testRoundFloat7() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(-2), new Float(100F));
    }

    @Test public void testRoundFloat8() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(-3), new Float(0F));
    }

    @Test public void testRoundFloat9() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(-4), new Float(0F));
    }

    @Test public void testRoundFloat10() throws Exception {
        helpTestRound(new Float(123.456F), new Integer(4000), new Float(123.456F));
    }

    @Test public void testRoundDouble1() throws Exception {
        helpTestRound(new Double(123.456), new Integer(4), new Double(123.456));
    }

    @Test public void testRoundDouble2() throws Exception {
        helpTestRound(new Double(123.456), new Integer(3), new Double(123.456));
    }

    @Test public void testRoundDouble3() throws Exception {
        helpTestRound(new Double(123.456), new Integer(2), new Double(123.46));
    }

    @Test public void testRoundDouble4() throws Exception {
        helpTestRound(new Double(123.456), new Integer(1), new Double(123.5));
    }

    @Test public void testRoundDouble5() throws Exception {
        helpTestRound(new Double(123.456), new Integer(0), new Double(123));
    }

    @Test public void testRoundDouble6() throws Exception {
        helpTestRound(new Double(123.456), new Integer(-1), new Double(120));
    }

    @Test public void testRoundDouble7() throws Exception {
        helpTestRound(new Double(123.456), new Integer(-2), new Double(100));
    }

    @Test public void testRoundDouble8() throws Exception {
        helpTestRound(new Double(123.456), new Integer(-3), new Double(0));
    }

    @Test public void testRoundDouble9() throws Exception {
        helpTestRound(new Double(123.456), new Integer(-4), new Double(0));
    }

    @Test public void testRoundDouble10() throws Exception {
        helpTestRound(new Double(-3.5), new Integer(0), new Double(-4));
    }

    @Test public void testRoundBigDecimal1() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(4), new BigDecimal("123.456")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal2() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(3), new BigDecimal("123.456")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal3() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(2), new BigDecimal("123.460")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal4() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(1), new BigDecimal("123.500")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal5() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(0), new BigDecimal("123.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal6() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(-1), new BigDecimal("120.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal7() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(-2), new BigDecimal("100.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal8() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(-3), new BigDecimal("0.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRoundBigDecimal9() throws Exception {
        helpTestRound(new BigDecimal("123.456"), new Integer(-4), new BigDecimal("0.000")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testHour1() throws Exception {
        helpTestHour1("00:00:00", 0); //$NON-NLS-1$
    }

    @Test public void testHour2() throws Exception {
        helpTestHour1("11:00:00", 11); //$NON-NLS-1$
    }

    @Test public void testHour3() throws Exception {
        helpTestHour1("12:00:00", 12); //$NON-NLS-1$
    }

    @Test public void testHour4() throws Exception {
        helpTestHour1("13:00:00", 13); //$NON-NLS-1$
    }

    @Test public void testHour5() throws Exception {
        helpTestHour1("23:59:59", 23); //$NON-NLS-1$
    }

    @Test public void testHour6() throws Exception {
        helpTestHour2("2002-01-01 00:00:00", 0); //$NON-NLS-1$
    }

    @Test public void testHour7() throws Exception {
        helpTestHour2("2002-01-01 11:00:00", 11); //$NON-NLS-1$
    }

    @Test public void testHour8() throws Exception {
        helpTestHour2("2002-01-01 12:00:00", 12); //$NON-NLS-1$
    }

    @Test public void testHour9() throws Exception {
        helpTestHour2("2002-01-01 13:00:00", 13); //$NON-NLS-1$
    }

    @Test public void testHour10() throws Exception {
        helpTestHour2("2002-01-01 23:59:59", 23); //$NON-NLS-1$
    }

    @Test public void testTimestampCreate1() throws Exception {
        helpTestTimestampCreate(TimestampUtil.createDate(103, 11, 1), TimestampUtil.createTime(23, 59, 59), "2003-12-01 23:59:59.0"); //$NON-NLS-1$
    }

    @Test public void testTimestampAddLeapYear() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(117, 1, 28, 15, 20, 30, 0), FunctionMethods.timestampAdd(NonReserved.SQL_TSI_YEAR, 1, TimestampUtil.createTimestamp(116, 1, 29, 15, 20, 30, 0)));
    }

    @Test public void testTimestampAdd2() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(103, 11, 1, 18, 20, 30, 0), FunctionMethods.timestampAdd(NonReserved.SQL_TSI_HOUR, 3, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 0)));
    }

    @Test public void testTimestampAdd3() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 29, 999999999), FunctionMethods.timestampAdd(NonReserved.SQL_TSI_FRAC_SECOND, -1, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 0)));
    }

    @Test public void testTimestampAdd4() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 31, 2), FunctionMethods.timestampAdd(NonReserved.SQL_TSI_FRAC_SECOND, 3, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 999999999)));
    }

    @Test(expected=FunctionExecutionException.class) public void testTimestampAdd5() throws Exception {
        assertEquals(null, FunctionMethods.timestampAdd(NonReserved.SQL_TSI_FRAC_SECOND, Long.MAX_VALUE, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 999999999)));
    }

    @Test(expected=FunctionExecutionException.class) public void testTimestampAdd6() throws Exception {
        assertEquals(null, FunctionMethods.timestampAdd(NonReserved.SQL_TSI_SECOND, (long)Integer.MAX_VALUE + 1, TimestampUtil.createTimestamp(103, 11, 1, 15, 20, 30, 0)));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 1),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 100000000),
                              new Long(99999999));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_2() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              // 1 day (8.64 x 10^10 nanos) and 1 nano
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 3, 9, 35, 3),
                              new Long(86400000000001L));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_3() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              // 1 day (8.64 x 10^10 nanos) less 1 nano
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 3),
                              new Long(-86399999999999L));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_4() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 0, 0, 0, 3),
                              new Long(00000002));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_5() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 0, 0, 0, 10),
                              new Long(9));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_6() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 0, 0, 0, 2),
                              TimestampUtil.createTimestamp((2001-1900), 5, 22, 0, 0, 0, 3),
                              new Long(1));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_7() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              // 1 nano diff
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 2),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 3),
                              new Long(1));
    }

    @Test public void testTimestampDiffTimeStamp_FracSec_8() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_FRAC_SECOND,
                              // 1 nano diff
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 3),
                              TimestampUtil.createTimestamp((2004-1900), 5, 22, 3, 9, 35, 2),
                              new Long(-1));
    }

    @Test public void testTimestampDiffTimeStamp_Min_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp(0, 0, 0, 2, 34, 12, 0),
                              TimestampUtil.createTimestamp(0, 0, 0, 12, 0, 0, 0),
                              new Long(565));
    }

    @Test public void testTimestampDiffTimeStamp_Min_2() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 0, 0, 2, 0, 0, 0),
                              TimestampUtil.createTimestamp((2001-1900), 0, 0, 0, 33, 12, 0),
                              new Long(-87));
    }

    @Test public void testTimestampDiffTimeStamp_Min_3() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 07, 58, 65497),
                              TimestampUtil.createTimestamp((2001-1900), 8, 29, 11, 25, 42, 483219),
                              new Long(4278));
    }

    @Test public void testTimestampDiffTimeStamp_Min_4() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 07, 58, 0),
                              TimestampUtil.createTimestamp((2001-1900), 8, 29, 11, 25, 42, 0),
                              new Long(4278));
    }

    @Test public void testTimestampDiffTimeStamp_Min_5() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MINUTE,
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 0, 0, 1),
                              TimestampUtil.createTimestamp((2001-1900), 8, 26, 12, 0, 0, 0),
                              new Long(0));
    }

    @Test public void testTimestampDiffTimeStamp_Hour_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_HOUR,
                TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 0, 0, 0),
                TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 59, 59, 999999999),
                new Long(0));
        //ensure that we get the same answer in a tz with an non-hour aligned offset and no dst
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("Pacific/Marquesas")); //$NON-NLS-1$
        try {
            helpTestTimestampDiff(NonReserved.SQL_TSI_HOUR,
                              TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 8, 26, 12, 59, 59, 999999999),
                              new Long(0));
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testTimestampDiffTimeStamp_Week_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_WEEK,
                              TimestampUtil.createTimestamp((2001-1900), 5, 21, 3, 9, 35, 100),
                              TimestampUtil.createTimestamp((2001-1900), 4, 2, 5, 19, 35, 500),
                              new Long(-7));
    }

    @Test public void testTimestampDiffTimeStamp_Month_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 19, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 20, 12, 0, 0, 0),
                              new Long(7));
    }

    @Test public void testTimestampDiffTimeStamp_Month_2() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 5, 1, 0, 0, 0, 1000000),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 12, 0, 0, 1),
                              new Long(6));
    }

    @Test public void testTimestampDiffTimeStamp_Month_3() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 19, 0, 0, 0, 1),
                              TimestampUtil.createTimestamp((2004-1900), 11, 18, 12, 0, 0, 1000000),
                              new Long(7));
    }

    @Test public void testTimestampDiffTimeStamp_Month_4() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 0, 1000000),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 0, 0),
                              new Long(7));
    }

    @Test public void testTimestampDiffTimeStamp_Month_5() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 1, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 0, 0),
                              new Long(7));
    }

    @Test public void testTimestampDiffTimeStamp_Month_6() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_MONTH,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 1, 0),
                              TimestampUtil.createTimestamp((2004-1900), 11, 1, 0, 0, 2, 0),
                              new Long(7));
    }

    @Test public void testTimestampDiffTimeStamp_Day_1() throws Exception {
        // Moving to June, March fails because of DST
        helpTestTimestampDiff(NonReserved.SQL_TSI_DAY,
                              TimestampUtil.createTimestamp((2004-1900), 4, 1, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 5, 1, 0, 0, 0, 0),
                              new Long(31));
    }

    @Test public void testTimestampDiffTimeStamp_Day_2() throws Exception {
        // Leap year
        helpTestTimestampDiff(NonReserved.SQL_TSI_DAY,
                              TimestampUtil.createTimestamp((2004-1900), 1, 1, 0, 0, 0, 0),
                              TimestampUtil.createTimestamp((2004-1900), 2, 1, 0, 0, 0, 0),
                              new Long(29));
    }

    @Test public void testTimestampDiffTime_Hour_1() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_HOUR, new Timestamp(
                TimestampUtil.createTime(3, 4, 45).getTime()), new Timestamp(
                TimestampUtil.createTime(5, 5, 36).getTime()), new Long(2));
    }

    @Test public void testTimestampDiffTime_Hour_2() throws Exception {
        helpTestTimestampDiff(NonReserved.SQL_TSI_HOUR, new Timestamp(
                TimestampUtil.createTime(5, 0, 30).getTime()), new Timestamp(
                TimestampUtil.createTime(3, 0, 31).getTime()), new Long(-2));
    }

    @Test public void testParseTimestamp1() throws Exception {
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
    @Test public void testModifyTimeZoneGMT() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "GMT+00:00", "GMT-01:00", "2004-10-03 16:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testModifyTimeZoneGMTPartialHour() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:30:59.123456789", "GMT+00:00", "GMT-01:45", "2004-10-03 17:15:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testModifyTimeZoneNamedTZ() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/New_York", "America/Chicago", "2004-10-03 16:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testModifyTimeZoneNamedTZ2() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/Chicago", "America/New_York", "2004-10-03 14:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testModifyTimeZoneStartLocal() throws Exception {
        helpTestModifyTimeZone("2004-10-03 15:19:59.123456789", "America/Chicago", "America/Los_Angeles", "2004-10-03 17:19:59.123456789"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    // case 2458
    @Test public void testCurrentDate() throws Exception {

        Date curDate = (Date)FunctionMethods.currentDate(new CommandContext());

        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(curDate);

        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), 0);
        assertEquals(cal1.get(Calendar.MINUTE), 0);
        assertEquals(cal1.get(Calendar.SECOND), 0);
        assertEquals(cal1.get(Calendar.MILLISECOND), 0);

    }

    // case 2458
    @Test public void testCurrentTime() throws Exception {

        Time curDate = (Time)FunctionMethods.currentTime(new CommandContext());

        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(curDate);

        // can not test the current time without making a copy of current "time"
        // so check the normalization of the date to unix epoch
        assertEquals(cal1.get(Calendar.YEAR), 1970);
        assertEquals(cal1.get(Calendar.MONTH), Calendar.JANUARY);
        assertEquals(cal1.get(Calendar.DATE), 1);

    }

    @Test public void testCurrentTimestamp() throws Exception {
        CommandContext context = new CommandContext();
        context.setCurrentTimestamp(1290123456789L);
        Timestamp current = FunctionMethods.current_timestamp(context, 0);
        assertEquals(0, current.getNanos());
        assertNotEquals(context.currentTimestamp(), current);
        assertEquals(context.currentTimestamp().getTime()/1000, current.getTime()/1000);

        current = FunctionMethods.current_timestamp(context, 3);
        assertEquals(789000000, current.getNanos());
    }

    @Test public void testRand() throws Exception {
        Double d = (Double)FunctionMethods.rand(new CommandContext(), new Integer(100));
        assertEquals(new Double(0.7220096548596434), d);

        FunctionMethods.rand(new CommandContext());
    }

    @Test public void testEnv() throws Exception {
        String systemProperty = "SystemProperty"; //$NON-NLS-1$

        // set the system property
        System.setProperty(systemProperty, systemProperty);
        System.setProperty(systemProperty.toLowerCase(), systemProperty+"_lowercase"); //$NON-NLS-1$

        assertEquals(systemProperty, FunctionMethods.env(systemProperty));
        assertEquals(systemProperty+"_lowercase", FunctionMethods.env(systemProperty.toUpperCase())); //$NON-NLS-1$
    }

    @Test(expected=FunctionExecutionException.class) public void testParseIntStrictness() throws Exception {
        FunctionMethods.parseBigDecimal(new CommandContext(), "a 1 a", "#"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testParseDateStrictness() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(108, 0, 1, 0, 0, 0, 0), FunctionMethods.parseTimestamp(new CommandContext(), " 2007-13-01", "yyyy-MM")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testParseTimeWhitespace() throws Exception {
        assertEquals(TimestampUtil.createTime(15, 0, 0), FunctionMethods.parseTimestamp(new CommandContext(), " 15:00:00 ", "HH:mm:ss")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testMod() {
        assertEquals(new BigDecimal("-1.1"), FunctionMethods.mod(new BigDecimal("-3.1"), new BigDecimal("2")));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testMod1() {
        assertEquals(new BigDecimal("-1.1"), FunctionMethods.mod(new BigDecimal("-3.1"), new BigDecimal("-2")));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testMod2() {
        assertEquals(-40, FunctionMethods.mod(-340, 60), 0);
    }

    @Test public void testMod3() {
        assertEquals(-40, FunctionMethods.mod(-340, -60), 0);
    }

    @Test public void testMod4() {
        assertEquals(new BigInteger("-1"), FunctionMethods.mod(new BigInteger("-3"), new BigInteger("2")));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testMod5() {
        assertEquals(new BigInteger("-1"), FunctionMethods.mod(new BigInteger("-3"), new BigInteger("-2")));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
