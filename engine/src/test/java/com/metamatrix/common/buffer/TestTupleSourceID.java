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

package com.metamatrix.common.buffer;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestTupleSourceID extends TestCase {

    private static final String ID_VALUE = "idValue"; //$NON-NLS-1$
    private static final String LOCATION_VALUE = "location"; //$NON-NLS-1$
    private TupleSourceID objWithIdAndLocation;
    private TupleSourceID objWithId;
    private TupleSourceID objWithLocation;

    /**
     * Constructor for TestTupleSourceID.
     * @param name
     */
    public TestTupleSourceID(String name) {
        super(name);
    }

    /**
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        this.objWithId = new TupleSourceID(ID_VALUE,null);
        this.objWithIdAndLocation = new TupleSourceID(ID_VALUE,LOCATION_VALUE);
        this.objWithLocation = new TupleSourceID("",LOCATION_VALUE); //$NON-NLS-1$
    }

    /**
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public TupleSourceID helpTestConstructor( final String location, final String id,
                                          final boolean shouldSucceed ) {
        try {
            final TupleSourceID channelId = new TupleSourceID(location,id);
            // Make sure that we were supposed to have succeeded
            if ( !shouldSucceed ) {
                fail("Did not expect to construct successfully"); //$NON-NLS-1$
            }
            return channelId;
        } catch ( Throwable t ) {
            if ( shouldSucceed ) {
            	throw new RuntimeException(t);
            }
        }
        return null;
    }

    public TupleSourceID helpTestConstructor( final String idValue,
                                          final boolean shouldSucceed ) {
        try {
            final TupleSourceID channelId = new TupleSourceID(idValue);
            // Make sure that we were supposed to have succeeded
            if ( !shouldSucceed ) {
                fail("Did not expect to construct successfully"); //$NON-NLS-1$
            }
            return channelId;
        } catch ( Throwable t ) {
            if ( shouldSucceed ) {
            	throw new RuntimeException(t);
            }
        }
        return null;
    }

    public void helpCheckStringId( final TupleSourceID id, final String expected ) {
        final String actual = id.getStringID();
        assertEquals(expected, actual);
    }

    public void helpCheckId( final TupleSourceID id, final String expected ) {
        final String actual = id.getIDValue();
        assertEquals(expected, actual);
    }

    public void helpCheckLocation( final TupleSourceID id, final String expected ) {
        final String actual = id.getLocation();
        assertEquals(expected, actual);
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /*
     * Test for void TupleSourceID(String)
     */
    public void testTupleSourceIDStringWithIdAndNoLocation() {
        final String idValue = "SingleIdWithoutDelimiter"; //$NON-NLS-1$
        final TupleSourceID id = helpTestConstructor(idValue, true);
        helpCheckId(id,idValue);
        helpCheckStringId(id, idValue);
        helpCheckLocation(id, null);
    }

    /*
     * Test for void TupleSourceID(String, String)
     */
    public void testTupleSourceIDStringWithIdAndLocation() {
        final String idValue = "SingleId"; //$NON-NLS-1$
        final String location = "Location"; //$NON-NLS-1$
        final String stringId = location + ":" + idValue; //$NON-NLS-1$
        final TupleSourceID id = helpTestConstructor(stringId, true);
        helpCheckId(id,idValue);
        helpCheckStringId(id, stringId);
        helpCheckLocation(id, location);
    }

    /*
     * Test for void TupleSourceID(String, String)
     */
    public void testTupleSourceIDStringWithNullIdAndLocation() {
        final String idValue = ""; //$NON-NLS-1$
        final String location = "Location"; //$NON-NLS-1$
        final String stringId = location + ":" + idValue; //$NON-NLS-1$
        final TupleSourceID id = helpTestConstructor(stringId, true);
        helpCheckId(id,idValue);
        helpCheckStringId(id, stringId);
        helpCheckLocation(id, location);
    }

    /*
     * Test for void TupleSourceID(String, String)
     */
    public void testTupleSourceIDStringWithIdAndNullLocation() {
        final String idValue = "SingleId"; //$NON-NLS-1$
        final String location = ""; //$NON-NLS-1$
        final String stringId = location + ":" + idValue; //$NON-NLS-1$
        final TupleSourceID id = helpTestConstructor(stringId, true);
        helpCheckId(id,idValue);
        helpCheckStringId(id, stringId);
        helpCheckLocation(id, location);
    }

    public void testIllegalTupleSourceIDString() {
        final String stringId = null;
        helpTestConstructor(stringId, false);
    }

    public void testHashCode1() {
        assertEquals(objWithId.hashCode(), ID_VALUE.hashCode());
    }

    public void testHashCode2() {
        assertEquals(objWithLocation.hashCode(), "".hashCode()); //$NON-NLS-1$
    }

    public void testHashCode3() {
        assertEquals(objWithIdAndLocation.hashCode(), ID_VALUE.hashCode());
    }

    public void testGetLocation1() {
        assertEquals(objWithId.getLocation(), null);
    }

    public void testGetLocation2() {
        assertEquals(objWithLocation.getLocation(), LOCATION_VALUE);
    }

    public void testGetLocation3() {
        assertEquals(objWithIdAndLocation.getLocation(), LOCATION_VALUE);
    }

    public void testGetIDValue1() {
        assertEquals(objWithId.getIDValue(), ID_VALUE);
    }

    public void testGetIDValue2() {
        assertEquals(objWithLocation.getIDValue(), ""); //$NON-NLS-1$
    }

    public void testGetIDValue3() {
        assertEquals(objWithIdAndLocation.getIDValue(), ID_VALUE);
    }

    public void testGetStringID1() {
        assertEquals(objWithId.getStringID(), ID_VALUE);
    }

    public void testGetStringID2() {
        assertEquals(objWithLocation.getStringID(), LOCATION_VALUE + ":"); //$NON-NLS-1$
    }

    public void testGetStringID3() {
        assertEquals(objWithIdAndLocation.getStringID(), LOCATION_VALUE + ":" + ID_VALUE); //$NON-NLS-1$
    }

    /*
     * Test for boolean equals(Object)
     */
    public void testEqualsSameObject1() {
        assertTrue(objWithId.equals(objWithId));
    }

    public void testEqualsSameObject2() {
        assertTrue(objWithLocation.equals(objWithLocation));
    }

    public void testEqualsSameObject3() {
        assertTrue(objWithIdAndLocation.equals(objWithIdAndLocation));
    }

    public void testEqualsSimilarObject1() {
        assertTrue(objWithId.equals(new TupleSourceID(ID_VALUE, null)));
    }

    public void testEqualsSimilarObject2() {
        assertTrue(objWithLocation.equals(new TupleSourceID("", LOCATION_VALUE))); //$NON-NLS-1$
    }

    public void testEqualsSimilarObject3() {
        assertTrue(objWithIdAndLocation.equals(new TupleSourceID(ID_VALUE, LOCATION_VALUE)));
    }

    /*
     * Test for String toString()
     */
    public void testToString1() {
        assertEquals(objWithId.toString(), ID_VALUE);
    }

    public void testToString2() {
        assertEquals(objWithLocation.toString(), LOCATION_VALUE + ":"); //$NON-NLS-1$
    }

    public void testToString3() {
        assertEquals(objWithIdAndLocation.toString(), LOCATION_VALUE + ":" + ID_VALUE); //$NON-NLS-1$
    }

    public void testRoundtrip1() {
        TupleSourceID expectedID = new TupleSourceID("1", "mymachine,100"); //$NON-NLS-1$ //$NON-NLS-2$
        String stringID = expectedID.getStringID();
        TupleSourceID actualID = new TupleSourceID(stringID);

        assertEquals("Different ID after roundtrip: ", expectedID.getIDValue(), actualID.getIDValue()); //$NON-NLS-1$
        assertEquals("Different location after roundtrip: ", expectedID.getIDValue(), actualID.getIDValue()); //$NON-NLS-1$
    }

    public void testRoundtrip2() {
        TupleSourceID expectedID = new TupleSourceID("1"); //$NON-NLS-1$
        String stringID = expectedID.getStringID();
        TupleSourceID actualID = new TupleSourceID(stringID);

        assertEquals("Different ID after roundtrip: ", expectedID.getIDValue(), actualID.getIDValue()); //$NON-NLS-1$
        assertEquals("Different location after roundtrip: ", expectedID.getIDValue(), actualID.getIDValue()); //$NON-NLS-1$
    }

    public void testStandard1() {
        UnitTestUtil.helpTestEquivalence(0, new TupleSourceID("1"), new TupleSourceID("1"));     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testStandard2() {
        UnitTestUtil.helpTestEquivalence(0, new TupleSourceID("1"), new TupleSourceID("1", "mymachine,100"));    //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testNotEquals() {
        TupleSourceID id1 = new TupleSourceID("1"); //$NON-NLS-1$
        TupleSourceID id2 = new TupleSourceID("2"); //$NON-NLS-1$
        assertTrue("Differing IDs compare as equal", ! id1.equals(id2)); //$NON-NLS-1$
    }
}

