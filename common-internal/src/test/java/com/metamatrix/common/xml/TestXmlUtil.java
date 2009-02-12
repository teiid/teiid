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

package com.metamatrix.common.xml;

import junit.framework.TestCase;

/**
 */
public class TestXmlUtil extends TestCase {
    
    public static String TEST_STRING_1 = "this is a > test &amp"; //$NON-NLS-1$
    public static String RESULT_STRING_1 = "this is a &gt; test &amp;amp"; //$NON-NLS-1$
    public static String TEST_STRING_2 = "this \" is a > < test &amp"; //$NON-NLS-1$
    public static String RESULT_STRING_2 = "this &quot; is a &gt; &lt; test &amp;amp"; //$NON-NLS-1$


    /**
     * Constructor for TestXmlUtil.
     * @param name
     */
    public TestXmlUtil(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================
    
    public void helpCheckValidCharacters( final String str, final boolean shouldBeValid ) {
        final String reason = XmlUtil.containsValidCharacters(str);
        if ( shouldBeValid ) {
            assertNull("Expected the string \"" + str + "\" to contain all valid characters; actual reason: " + reason, reason); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            assertNotNull("Expected the string \"" + str + "\" to contain some invalid characters; but no invalid characters found"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
    
    protected boolean isCharacterValid( final char c ) {
        if (c == '\n') return true;
        if (c == '\r') return true;
        if (c == '\t') return true;
        
        if (c < 0x20) return false;  if (c <= 0xD7FF) return true;
        if (c < 0xE000) return false;  if (c <= 0xFFFD) return true;
        if (c < 0x10000) return false;  if (c <= 0x10FFFF) return true;
        
        return false;
    }

    /**
     * This method loops over the range of allowable characters, builds String objects
     * of the supplied length, and checks whether the 
     * {@link XmlUtil#containsValidCharacters(String)} method returns the expected value.
     * @param maxLength the maximum length of the string objects to pass to 
     * the {@link XmlUtil#containsValidCharacters(String)} method.
     * @param validXmlCharsOnly true if only valid characters should be included,
     * or false if invalid characters should be included.
     */
    public void helpTestValidCharacters( final long maxLength, final boolean validXmlCharsOnly ) {
        long maxLengthEachString = Math.max(2, maxLength );
        if ( !validXmlCharsOnly ) {
            --maxLengthEachString;
        }
        StringBuffer sb = new StringBuffer();
        // Note that Java can only handle 2-byte characters, but the spec (and the Verifier method)
        // is written with 4-byte characters in mind.  So, rather than loop from
        //    0x000 <= c <= 0x10FFFF
        // we only can loop from
        //    0x000 <= c < 0xFFFF
        for ( char c = 0x000; c<0xFFFF; ++c ) {
            if ( sb.length() == maxLengthEachString ) {
                if ( !validXmlCharsOnly ) {
                    sb.append(0x20);        // ensure every string has invalid char
                }
                helpCheckValidCharacters(sb.toString(),validXmlCharsOnly);
                sb = new StringBuffer();
            }
            if ( !validXmlCharsOnly || isCharacterValid(c) ) {
                sb.append(c);
            }
        }
        if ( !validXmlCharsOnly ) {
            sb.append(0x20);        // ensure every string has invalid char
        }
        helpCheckValidCharacters(sb.toString(),validXmlCharsOnly);
    }
    

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    public void testContainsValidCharactersInStrings10CharsLong() {
        helpTestValidCharacters(10,true);
    }

    public void testContainsValidCharactersInStrings100CharsLong() {
        helpTestValidCharacters(100,true);
    }

    public void testContainsValidCharactersInStrings1000CharsLong() {
        helpTestValidCharacters(1000,true);
    }

    public void testContainsInvalidCharactersInStrings10CharsLong() {
        helpTestValidCharacters(10,false);
    }

    public void testContainsInvalidCharactersInStrings100CharsLong() {
        helpTestValidCharacters(100,false);
    }

    public void testContainsInvalidCharactersInStrings1000CharsLong() {
        helpTestValidCharacters(1000,false);
    }
    
    public void testEscapeCharacterData1() {
        String result = XmlUtil.escapeCharacterData(TEST_STRING_1);
        assertEquals(RESULT_STRING_1, result);
    }
    
    public void testEscapeCharacterData2() {
        String result = XmlUtil.escapeCharacterData(TEST_STRING_2);
        assertEquals(RESULT_STRING_2, result);
    }
    
    public void testEscapeCharacterDataEmptyString() {
        String result = XmlUtil.escapeCharacterData(""); //$NON-NLS-1$
        assertEquals("", result); //$NON-NLS-1$
    }
    
    public void testEscapeCharacterDataNullParam() {
        String result = XmlUtil.escapeCharacterData(null);
        assertEquals(null, result);
    }

}
