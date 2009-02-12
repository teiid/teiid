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

package com.metamatrix.core.id;

import junit.framework.TestCase;

/**
 * TestParsedObjectID
 */
public class TestParsedObjectID extends TestCase {

    private static final String EMPTY_STRING = ""; //$NON-NLS-1$
    private static final String PROTOCOL = "SomeProtocol"; //$NON-NLS-1$
    private static final String REMAINDER = "Some remainder"; //$NON-NLS-1$
    private static final String NON_DELIM_CHARS = " !@#$%^&*()01234567890-=_+`~" + //$NON-NLS-1$
                                                  "abcdefghijklmnopqrstuvwxyz" + //$NON-NLS-1$
                                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + //$NON-NLS-1$
                                                  "{}|[]]\\;'\"<>?,./\t\n\r"; //$NON-NLS-1$

    private static final String[] PARSABLE_IDS = new String[]{"p123:abcdef", //$NON-NLS-1$
                                                              "p123: abcdef", //$NON-NLS-1$
                                                              " p123 :abcdef", //$NON-NLS-1$
                                                              " p123 : abcdef ", //$NON-NLS-1$
                                                              " p123 : abc def ", //$NON-NLS-1$
                                                              " p123 : a : b", //$NON-NLS-1$
                                                              " p123:a:b", //$NON-NLS-1$
                                                              NON_DELIM_CHARS + ":" + NON_DELIM_CHARS, //$NON-NLS-1$
                                                              ":"}; //$NON-NLS-1$

    private static final String[] NOT_PARSABLE_IDS  = new String[]{"no delimiter", //$NON-NLS-1$
                                                                   "", //$NON-NLS-1$
                                                                   null};

    private ParsedObjectID parsedID;
    private ParsedObjectID parsedIDWithNullProtocol;
    private ParsedObjectID parsedIDWithNullRemainder;
    private ParsedObjectID parsedIDWithZeroLengthProtocol;
    private ParsedObjectID parsedIDWithZeroLengthRemainder;

    /**
     * Constructor for TestParsedObjectID.
     * @param name
     */
    public TestParsedObjectID(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        parsedID                        = new ParsedObjectID(PROTOCOL,REMAINDER);
        parsedIDWithNullProtocol        = new ParsedObjectID(null,REMAINDER);
        parsedIDWithNullRemainder       = new ParsedObjectID(PROTOCOL,null);
        parsedIDWithZeroLengthProtocol  = new ParsedObjectID(EMPTY_STRING,REMAINDER);
        parsedIDWithZeroLengthRemainder = new ParsedObjectID(PROTOCOL,EMPTY_STRING);
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        parsedID                        = null;
        parsedIDWithNullProtocol        = null;
        parsedIDWithNullRemainder       = null;
        parsedIDWithZeroLengthProtocol  = null;
        parsedIDWithZeroLengthRemainder = null;
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public void helpTestParsable(final String str, final String expectedProtocol, boolean shouldSucceed ) {
        ParsedObjectID p = null;
        try {
            if ( expectedProtocol == null ) {
                p = ParsedObjectID.parsedStringifiedObjectID(str);
            } else {
                p = ParsedObjectID.parsedStringifiedObjectID(str,expectedProtocol);
            }
            if ( !shouldSucceed ) {
                fail("Unexpectedly succeeded in parsing string " + str); //$NON-NLS-1$
            }
        } catch (InvalidIDException e) {
            if ( shouldSucceed ) {
            	throw new RuntimeException(e);
            }
        }

        // If shouldSucceed, then test for expected protocol ...
        if ( shouldSucceed && expectedProtocol != null ) {
            if ( !p.getProtocol().equals(expectedProtocol)) {
                fail("Result of getProtocol() on parsed string didn't match expected protocol " + expectedProtocol); //$NON-NLS-1$
            }
        }
    }

    public void helpTestGetProtocol( final ParsedObjectID id, final String protocol ) {
        assertEquals(protocol,id.getProtocol());
    }

    public void helpTestGetRemainder( final ParsedObjectID id, final String remainder ) {
        assertEquals(remainder,id.getRemainder());
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    public void testGetProtocol() {
        helpTestGetProtocol(parsedID,PROTOCOL);
    }
    public void testGetRemainder() {
        helpTestGetRemainder(parsedID,REMAINDER);
    }

    public void testGetProtocolWhenConstructedWithNullProtocol() {
        helpTestGetProtocol(parsedIDWithNullProtocol,null);
    }
    public void testGetRemainderWhenConstructedWithNullProtocol() {
        helpTestGetRemainder(parsedIDWithNullProtocol,REMAINDER);
    }

    public void testGetProtocolWhenConstructedWithNullRemainder() {
        helpTestGetProtocol(parsedIDWithNullRemainder,PROTOCOL);
    }
    public void testGetRemainderWhenConstructedWithNullRemainder() {
        helpTestGetRemainder(parsedIDWithNullRemainder,null);
    }

    public void testGetProtocolWhenConstructedWithZeroLengthProtocol() {
        helpTestGetProtocol(parsedIDWithZeroLengthProtocol,EMPTY_STRING);
    }
    public void testGetRemainderWhenConstructedWithZeroLengthProtocol() {
        helpTestGetRemainder(parsedIDWithZeroLengthProtocol,REMAINDER);
    }

    public void testGetProtocolWhenConstructedWithZeroLengthRemainder() {
        helpTestGetProtocol(parsedIDWithZeroLengthRemainder,PROTOCOL);
    }
    public void testGetRemainderWhenConstructedWithZeroLengthRemainder() {
        helpTestGetRemainder(parsedIDWithZeroLengthRemainder,EMPTY_STRING);
    }

    public void testParsingParsables() {
        for (int i = 0; i < PARSABLE_IDS.length; i++) {
            final String parsableString = PARSABLE_IDS[i];
            helpTestParsable(parsableString,null,true);
        }
    }
    public void testParsingNotParsables() {
        for (int i = 0; i < NOT_PARSABLE_IDS.length; i++) {
            final String parsableString = NOT_PARSABLE_IDS[i];
            helpTestParsable(parsableString,null,false);
        }
    }
}
