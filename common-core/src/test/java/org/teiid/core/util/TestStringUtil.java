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

package org.teiid.core.util;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * @version     1.0
 * @author
 */
public class TestStringUtil {

    public void helpTestJoin(List input, String delimiter, String expectedResult){
        String result = StringUtil.join(input, delimiter);
        assertEquals("Unexpected Join result", expectedResult, result ); //$NON-NLS-1$
    }

    public void helpTestReplace(String source, String search, String replace, String expectedResult){
        String result = StringUtil.replace(source, search, replace);
        assertEquals("Unexpected Replace result", expectedResult, result ); //$NON-NLS-1$
    }

    public void helpTestReplaceAll(String source, String search, String replace, String expectedResult){
        String result = StringUtil.replaceAll(source, search, replace);
        assertEquals("Unexpected ReplaceAll result", expectedResult, result ); //$NON-NLS-1$
    }

    @Test public void testJoin1() {
        List<String> input = new ArrayList<String>();
        input.add("One"); //$NON-NLS-1$
        input.add("Two"); //$NON-NLS-1$
        helpTestJoin(input, null, null);
    }

    @Test public void testJoin2() {
        helpTestJoin(null, "/", null); //$NON-NLS-1$
    }

    @Test public void testJoin3() {
        List<String> input = new ArrayList<String>();
        input.add("One"); //$NON-NLS-1$
        input.add("Two"); //$NON-NLS-1$
        helpTestJoin(input, "/", "One/Two"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testReplace1() {
        helpTestReplace("12225", null, "234", "12225"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testReplace2() {
        helpTestReplace("12225", "222", null, "12225"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testReplace3() {
        helpTestReplace("12225", "222", "234", "12345"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testReplaceAll() {
        helpTestReplaceAll("1121121112", "2", "1", "1111111111"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testGetStackTrace() {
        final String expectedStackTrace = "java.lang.RuntimeException: Test"; //$NON-NLS-1$
        final Throwable t = new RuntimeException("Test"); //$NON-NLS-1$
        final String trace = StringUtil.getStackTrace(t);
        if ( !trace.startsWith(expectedStackTrace) ) {
            fail("Stack trace: \n" + trace + "\n did not match expected stack trace: \n" + expectedStackTrace); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    @Test public void testToString() {
        final String[] input = new String[]{"string1","string2","string3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        final String output = StringUtil.toString(input);
        assertEquals("[string1,string2,string3]", output); //$NON-NLS-1$
    }

    @Test public void testGetTokens() {
        final String input = "string with; tokens ; delimited by ; ; semicolons; there;; are 7 tokens."; //$NON-NLS-1$
        final List<String> tokens = StringUtil.getTokens(input,";"); //$NON-NLS-1$
        assertEquals(7, tokens.size());
        assertEquals("string with", tokens.get(0)); //$NON-NLS-1$
        assertEquals(" tokens ", tokens.get(1)); //$NON-NLS-1$
        assertEquals(" delimited by ", tokens.get(2)); //$NON-NLS-1$
        assertEquals(" ", tokens.get(3)); //$NON-NLS-1$
        assertEquals(" semicolons", tokens.get(4)); //$NON-NLS-1$
        assertEquals(" there", tokens.get(5)); //$NON-NLS-1$
        assertEquals(" are 7 tokens.", tokens.get(6)); //$NON-NLS-1$
    }

    @Test public void testIndexOfIgnoreCase() {
        String text = "test"; //$NON-NLS-1$
        assertEquals(-1,StringUtil.indexOfIgnoreCase(null,text));
        assertEquals(-1,StringUtil.indexOfIgnoreCase("",text)); //$NON-NLS-1$
        assertEquals(-1,StringUtil.indexOfIgnoreCase(text,null));
        assertEquals(-1,StringUtil.indexOfIgnoreCase(text,"")); //$NON-NLS-1$
        assertEquals(-1,StringUtil.indexOfIgnoreCase(text,"testing")); //$NON-NLS-1$

        assertEquals(1,StringUtil.indexOfIgnoreCase(text,"es")); //$NON-NLS-1$
        assertEquals(1,StringUtil.indexOfIgnoreCase(text,"Es")); //$NON-NLS-1$
        assertEquals(1,StringUtil.indexOfIgnoreCase(text,"eS")); //$NON-NLS-1$
        assertEquals(2,StringUtil.indexOfIgnoreCase(text,"ST")); //$NON-NLS-1$
    }

    @Test public void testStartsWithIgnoreCase() {
        String text = "test"; //$NON-NLS-1$
        assertEquals(false,StringUtil.startsWithIgnoreCase(null,text));
        assertEquals(false,StringUtil.startsWithIgnoreCase("",text)); //$NON-NLS-1$
        assertEquals(false,StringUtil.startsWithIgnoreCase(text,null));
        assertEquals(true,StringUtil.startsWithIgnoreCase(text,"")); //$NON-NLS-1$
        assertEquals(false,StringUtil.startsWithIgnoreCase(text,"testing")); //$NON-NLS-1$

        assertEquals(false,StringUtil.startsWithIgnoreCase(text,"es")); //$NON-NLS-1$
        assertEquals(true,StringUtil.startsWithIgnoreCase(text,"te")); //$NON-NLS-1$
        assertEquals(true,StringUtil.startsWithIgnoreCase(text,"Te")); //$NON-NLS-1$
        assertEquals(true,StringUtil.startsWithIgnoreCase(text,"tE")); //$NON-NLS-1$
        assertEquals(true,StringUtil.startsWithIgnoreCase(text,"TE")); //$NON-NLS-1$
    }

    @Test public void testEndsWithIgnoreCase() {
        String text = "test"; //$NON-NLS-1$
        assertEquals(false,StringUtil.endsWithIgnoreCase(null,text));
        assertEquals(false,StringUtil.endsWithIgnoreCase("",text)); //$NON-NLS-1$
        assertEquals(false,StringUtil.endsWithIgnoreCase(text,null));
        assertEquals(true,StringUtil.endsWithIgnoreCase(text,"")); //$NON-NLS-1$
        assertEquals(false,StringUtil.endsWithIgnoreCase(text,"testing")); //$NON-NLS-1$

        assertEquals(false,StringUtil.endsWithIgnoreCase(text,"es")); //$NON-NLS-1$
        assertEquals(true,StringUtil.endsWithIgnoreCase(text,"st")); //$NON-NLS-1$
        assertEquals(true,StringUtil.endsWithIgnoreCase(text,"St")); //$NON-NLS-1$
        assertEquals(true,StringUtil.endsWithIgnoreCase(text,"sT")); //$NON-NLS-1$
        assertEquals(true,StringUtil.endsWithIgnoreCase(text,"ST")); //$NON-NLS-1$
    }

    @Test public void testIsLetter() {
        assertTrue(StringUtil.isLetter('a'));
        assertTrue(StringUtil.isLetter('A'));
        assertFalse(StringUtil.isLetter('5'));
        assertFalse(StringUtil.isLetter('_'));
        assertTrue(StringUtil.isLetter('\u00cf')); // Latin-1 letter
        assertFalse(StringUtil.isLetter('\u0967')); // Devanagiri number
        assertTrue(StringUtil.isLetter('\u0905')); // Devanagiri letter
    }

    @Test public void testIsDigit() {
        assertFalse(StringUtil.isDigit('a'));
        assertFalse(StringUtil.isDigit('A'));
        assertTrue(StringUtil.isDigit('5'));
        assertFalse(StringUtil.isDigit('_'));
        assertFalse(StringUtil.isDigit('\u00cf')); // Latin-1 letter
        assertTrue(StringUtil.isDigit('\u0967')); // Devanagiri number
        assertFalse(StringUtil.isDigit('\u0905')); // Devanagiri letter
    }

    @Test public void testIsLetterOrDigit() {
        assertTrue(StringUtil.isLetterOrDigit('a'));
        assertTrue(StringUtil.isLetterOrDigit('A'));
        assertTrue(StringUtil.isLetterOrDigit('5'));
        assertFalse(StringUtil.isLetterOrDigit('_'));
        assertTrue(StringUtil.isLetterOrDigit('\u00cf')); // Latin-1 letter
        assertTrue(StringUtil.isLetterOrDigit('\u0967')); // Devanagiri number
        assertTrue(StringUtil.isLetterOrDigit('\u0905')); // Devanagiri letter
    }

    @Test public void testGetFirstToken(){
        assertEquals("/foo/bar", StringUtil.getFirstToken("/foo/bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("", StringUtil.getFirstToken("/foo/bar.vdb", "/"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("/foo", StringUtil.getFirstToken("/foo./bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("bar", StringUtil.getFirstToken(StringUtil.getLastToken("/foo/bar.vdb", "/"), "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("vdb", StringUtil.getLastToken("/foo/bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public enum EnumTest {
        HELLO,
        WORLD
    }

    @Test public void testValueOf() throws Exception {
        assertEquals(Integer.valueOf(21), StringUtil.valueOf("21", Integer.class)); //$NON-NLS-1$
        assertEquals(Boolean.valueOf(true), StringUtil.valueOf("true", Boolean.class)); //$NON-NLS-1$
        assertEquals("Foo", StringUtil.valueOf("Foo", String.class)); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(Float.valueOf(10.12f), StringUtil.valueOf("10.12", Float.class)); //$NON-NLS-1$
        assertEquals(Double.valueOf(121.123), StringUtil.valueOf("121.123", Double.class)); //$NON-NLS-1$
        assertEquals(Long.valueOf(12334567L), StringUtil.valueOf("12334567", Long.class)); //$NON-NLS-1$
        assertEquals(Short.valueOf((short)21), StringUtil.valueOf("21", Short.class)); //$NON-NLS-1$

        List list = StringUtil.valueOf("foo,bar,x,y,z", List.class); //$NON-NLS-1$
        assertEquals(5, list.size());
        assertTrue(list.contains("foo")); //$NON-NLS-1$
        assertTrue(list.contains("x")); //$NON-NLS-1$

        int[] values = StringUtil.valueOf("1,2,3,4,5", new int[0].getClass()); //$NON-NLS-1$
        assertEquals(5, values.length);
        assertEquals(5, values[4]);

        Map m = StringUtil.valueOf("foo=bar,x=,y=z", Map.class); //$NON-NLS-1$
        assertEquals(3, m.size());
        assertEquals(m.get("foo"), "bar"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(m.get("x"), ""); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(EnumTest.HELLO, StringUtil.valueOf("HELLO", EnumTest.class)); //$NON-NLS-1$

        assertEquals(new URL("http://teiid.org"), StringUtil.valueOf("http://teiid.org", URL.class)); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
