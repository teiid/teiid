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

package org.teiid.core.util;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

/**
 * @version 	1.0
 * @author
 */
public class TestStringUtil extends TestCase {

    /**
     * Constructor for TestStringUtil.
     * @param name
     */
    public TestStringUtil(String name) {
        super(name);
    }

	//  ********* H E L P E R   M E T H O D S  *********
	public void helpTestEncloseInSingleQuotes(String input, String expectedResult){
	    String result = StringUtil.enclosedInSingleQuotes(input);
	    assertEquals("Unexpected encloseInSignleQuotes result", expectedResult, result ); //$NON-NLS-1$
	}

	public void helpTestComputeDisplayableForm(String input, String expectedResult){
	    String result = StringUtil.computeDisplayableForm(input, input);
	    assertEquals("Unexpected ComputeDisplayableForm result", expectedResult, result ); //$NON-NLS-1$
	}

	public void helpTestComputePluralForm(String input, String expectedResult){
	    String result = StringUtil.computePluralForm(input);
	    assertEquals("Unexpected ComputePluralForm result", expectedResult, result ); //$NON-NLS-1$
	}

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

	public void helpTestTruncString(String input, int length, String expectedResult){
	    String result = StringUtil.truncString(input, length);
	    assertEquals("Unexpected TruncString result", expectedResult, result ); //$NON-NLS-1$
	}

	//  ********* T E S T   S U I T E   M E T H O D S  *********
	public void testEncloseInSingleQuotes() {
	    helpTestEncloseInSingleQuotes("testString", "\'testString\'"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputeDisplayableForm1() {
	    helpTestComputeDisplayableForm("testString", "Test String"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputeDisplayableForm2() {
	    helpTestComputeDisplayableForm("TEST STRING", "TEST STRING"); //$NON-NLS-1$ //$NON-NLS-2$
	}

    public void testComputeDisplayableForm3() {
        helpTestComputeDisplayableForm("TestSTRING", "Test STRING"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm4() {
        helpTestComputeDisplayableForm("MetaMatrix", "Meta Matrix"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm5() {
        helpTestComputeDisplayableForm("metaMatrix", "Meta Matrix"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm6() {
        helpTestComputeDisplayableForm("Metamatrix", "Metamatrix"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm7() {
        helpTestComputeDisplayableForm("SomeMetaMatrixEmbedded", "Some Meta Matrix Embedded"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm8() {
        helpTestComputeDisplayableForm("SomeMetaMetaMatrixMetaEmbedded", "Some Meta Meta Matrix Meta Embedded"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputeDisplayableForm9() {
        helpTestComputeDisplayableForm("SomemetaMatrixMetaMatrixMetaEmbedded", "Somemeta Matrix Meta Matrix Meta Embedded"); //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void testComputePluralForm1() {
	    helpTestComputePluralForm("Test", "Tests"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm2() {
	    helpTestComputePluralForm("ss", "sses"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm3() {
	    helpTestComputePluralForm("x", "xes"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm4() {
	    helpTestComputePluralForm("ch", "ches"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm5() {
	    helpTestComputePluralForm("zy", "zies"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm6() {
	    helpTestComputePluralForm("ay", "ays"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm7() {
	    helpTestComputePluralForm("ey", "eys"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm8() {
	    helpTestComputePluralForm("iy", "iys"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testComputePluralForm9() {
	    helpTestComputePluralForm("oy", "oys"); //$NON-NLS-1$ //$NON-NLS-2$
	}

    public void testComputePluralForm10() {
        helpTestComputePluralForm("uy", "uys"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputePluralForm11() {
        helpTestComputePluralForm("any", "anys"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testComputePluralForm12() {
        helpTestComputePluralForm("classes", "classes"); //$NON-NLS-1$ //$NON-NLS-2$
    }

	public void testJoin1() {
	    List input = new ArrayList();
	    input.add("One"); //$NON-NLS-1$
	    input.add("Two"); //$NON-NLS-1$
	    helpTestJoin(input, null, null);
	}

	public void testJoin2() {
	    helpTestJoin(null, "/", null); //$NON-NLS-1$
	}

	public void testJoin3() {
	    List input = new ArrayList();
	    input.add("One"); //$NON-NLS-1$
	    input.add("Two"); //$NON-NLS-1$
	    helpTestJoin(input, "/", "One/Two"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testReplace1() {
	    helpTestReplace("12225", null, "234", "12225"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testReplace2() {
	    helpTestReplace("12225", "222", null, "12225"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testReplace3() {
	    helpTestReplace("12225", "222", "234", "12345"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public void testReplaceAll() {
	    helpTestReplaceAll("1121121112", "2", "1", "1111111111"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

    public void testTruncString() {
        helpTestTruncString("123456", 5, "12345"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetStackTrace() {
        final String expectedStackTrace = "java.lang.RuntimeException: Test"; //$NON-NLS-1$
        final Throwable t = new RuntimeException("Test"); //$NON-NLS-1$
        final String trace = StringUtil.getStackTrace(t);
        if ( !trace.startsWith(expectedStackTrace) ) {
            fail("Stack trace: \n" + trace + "\n did not match expected stack trace: \n" + expectedStackTrace); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testToString() {
        final String[] input = new String[]{"string1","string2","string3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        final String output = StringUtil.toString(input);
        assertEquals("[string1,string2,string3]", output); //$NON-NLS-1$
    }

    public void testGetTokens() {
        final String input = "string with; tokens ; delimited by ; ; semicolons; there;; are 7 tokens."; //$NON-NLS-1$
        final List tokens = StringUtil.getTokens(input,";"); //$NON-NLS-1$
        assertEquals(7, tokens.size());
		assertEquals("string with", tokens.get(0)); //$NON-NLS-1$
		assertEquals(" tokens ", tokens.get(1)); //$NON-NLS-1$
		assertEquals(" delimited by ", tokens.get(2)); //$NON-NLS-1$
		assertEquals(" ", tokens.get(3)); //$NON-NLS-1$
		assertEquals(" semicolons", tokens.get(4)); //$NON-NLS-1$
		assertEquals(" there", tokens.get(5)); //$NON-NLS-1$
		assertEquals(" are 7 tokens.", tokens.get(6)); //$NON-NLS-1$
    }

    public void testSplitOnEntireString() {
        List result = StringUtil.splitOnEntireString("thisNEXTcanNEXTbe", "NEXT"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(3, result.size());
        assertEquals("this", result.get(0)); //$NON-NLS-1$
        assertEquals("can", result.get(1)); //$NON-NLS-1$
        assertEquals("be", result.get(2)); //$NON-NLS-1$

    }

    public void testSplitOnEntireStringEmptyString() {
        List result = StringUtil.splitOnEntireString("", "NEXT"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(1, result.size());
        assertEquals("", result.get(0)); //$NON-NLS-1$
    }

    public void testSplitOnEntireStringEntireStringIsDelimiter() {
        List result = StringUtil.splitOnEntireString("NEXT", "NEXT"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(2, result.size());
        assertEquals("", result.get(0)); //$NON-NLS-1$
        assertEquals("", result.get(1)); //$NON-NLS-1$
    }

    public void testSplitOnEntireStringEmptyDelimiter() {
        List result = StringUtil.splitOnEntireString("test", ""); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(1, result.size());
        assertEquals("test", result.get(0)); //$NON-NLS-1$
    }

    public void testSplitOnEntireStringEndsWithDelimiter() {
        List result = StringUtil.splitOnEntireString("testNEXT", "NEXT"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(2, result.size());
        assertEquals("test", result.get(0)); //$NON-NLS-1$
        assertEquals("", result.get(1)); //$NON-NLS-1$
    }

    public void testIndexOfIgnoreCase() {
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

    public void testStartsWithIgnoreCase() {
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

    public void testEndsWithIgnoreCase() {
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

    public void testIsDigits() {
        assertTrue(StringUtil.isDigits("012872")); //$NON-NLS-1$
        assertTrue(StringUtil.isDigits("634644")); //$NON-NLS-1$
        assertFalse(StringUtil.isDigits("A634644")); //$NON-NLS-1$
        assertFalse(StringUtil.isDigits("634A644")); //$NON-NLS-1$
    }

    public void testToFixedLengthNull() {
        assertEquals("    ", StringUtil.toFixedLength(null, 4)); //$NON-NLS-1$
    }

    public void testToFixedLengthPad() {
        assertEquals("a   ", StringUtil.toFixedLength("a", 4)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testToFixedLengthNoChange() {
        assertEquals("abcd", StringUtil.toFixedLength("abcd", 4)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testToFixedLengthChop() {
        assertEquals("abcd", StringUtil.toFixedLength("abcdefgh", 4)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIsLetter() {
        assertTrue(StringUtil.isLetter('a'));
        assertTrue(StringUtil.isLetter('A'));
        assertFalse(StringUtil.isLetter('5'));
        assertFalse(StringUtil.isLetter('_'));
        assertTrue(StringUtil.isLetter('\u00cf')); // Latin-1 letter
        assertFalse(StringUtil.isLetter('\u0967')); // Devanagiri number
        assertTrue(StringUtil.isLetter('\u0905')); // Devanagiri letter
    }

    public void testIsDigit() {
        assertFalse(StringUtil.isDigit('a'));
        assertFalse(StringUtil.isDigit('A'));
        assertTrue(StringUtil.isDigit('5'));
        assertFalse(StringUtil.isDigit('_'));
        assertFalse(StringUtil.isDigit('\u00cf')); // Latin-1 letter
        assertTrue(StringUtil.isDigit('\u0967')); // Devanagiri number
        assertFalse(StringUtil.isDigit('\u0905')); // Devanagiri letter
    }

    public void testIsLetterOrDigit() {
        assertTrue(StringUtil.isLetterOrDigit('a'));
        assertTrue(StringUtil.isLetterOrDigit('A'));
        assertTrue(StringUtil.isLetterOrDigit('5'));
        assertFalse(StringUtil.isLetterOrDigit('_'));
        assertTrue(StringUtil.isLetterOrDigit('\u00cf')); // Latin-1 letter
        assertTrue(StringUtil.isLetterOrDigit('\u0967')); // Devanagiri number
        assertTrue(StringUtil.isLetterOrDigit('\u0905')); // Devanagiri letter
    }

    public void testToUpperCase() {
        assertEquals("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", StringUtil.toUpperCase("abcdefghijklmnopqrstuvwxyz1234567890")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("LATIN1_\u00c0", StringUtil.toUpperCase("Latin1_\u00e0")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    public void testToLowerCase() {
        assertEquals("abcdefghijklmnopqrstuvwxyz1234567890", StringUtil.toLowerCase("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("latin1_\u00e0", StringUtil.toLowerCase("Latin1_\u00c0")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCreateFileName() {
        assertEquals("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", StringUtil.createFileName("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("http:__www.metamatrix.com_parm1=test;parm2=testy2", StringUtil.createFileName("http://www.metamatrix.com?parm1=test;parm2=testy2")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetFirstToken(){
    	assertEquals("/foo/bar", StringUtil.getFirstToken("/foo/bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	assertEquals("", StringUtil.getFirstToken("/foo/bar.vdb", "/"));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	assertEquals("/foo", StringUtil.getFirstToken("/foo./bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	assertEquals("bar", StringUtil.getFirstToken(StringUtil.getLastToken("/foo/bar.vdb", "/"), "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    	assertEquals("vdb", StringUtil.getLastToken("/foo/bar.vdb", "."));//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public enum Test {
    	HELLO,
    	WORLD
    }
    
    public void testValueOf() throws Exception {
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
    	assertEquals(Test.HELLO, StringUtil.valueOf("HELLO", Test.class)); //$NON-NLS-1$ 
    	
    	assertEquals(new URL("http://teiid.org"), StringUtil.valueOf("http://teiid.org", URL.class)); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
