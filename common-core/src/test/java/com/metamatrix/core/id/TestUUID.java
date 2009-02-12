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

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

/**
 * <p>Test cases for {@link UUID} class. </p>
 */
public class TestUUID extends TestCase {

    // Number of IDs to generate
    private static final int NGEN_POWERS  = 63;
    private static final int NGEN_RANDOM  = 10000;

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestUUID( String name ) {
        super( name );
    }

    // =========================================================================
    //                      H E L P E R    M E T H O D S
    // =========================================================================

    /**
     * Test UUID generation for IDs generated from powers of 2.
     */
    private static void helpTestGenPowers( int nGen ) {
        UUID uuid = null;

        // First test some fairly "regular" numbers...
        for ( int j = 0; j < Math.min(nGen,63); j++ ) {
            long v1 = 1L << j;
            long v2 = (1L << j)/2;
            uuid = new UUID(v1,v2);
            checkStringToObject(uuid);  // Test case may fail here
        }
    }
    
    /**
     * Test UUID generation for IDs generated randomly.
     */
    private static void helpTestGenRandom( int nGen ) {
        UUID uuid = null;

        // Then test some random numbers...
        java.util.Random rng = new java.util.Random();
        for ( int k = 0; k < nGen; k++ ) {
            long v1 = rng.nextLong();
            long v2 = rng.nextLong();
            uuid = new UUID(v1,v2);
            checkStringToObject(uuid);  // Test case may fail here
        }
    }
    
    /**
     * Test for duplicates when generating a bunch of IDs.
     */
    private static void helpTestDuplicates( int nGen ) {
        UUID uuid = null;
        Set uuids = new HashSet();
//        Set duplicates = new HashSet();

        // First test some fairly "regular" numbers...
        for ( int j = 0; j < Math.min(nGen,63); j++ ) {
            long v1 = 1L << j;
            long v2 = (1L << j)/2;
            uuid = new UUID(v1,v2);
            assertTrue( "UUID '" + uuid + "' is a duplicate!", !uuids.contains(uuid) ); //$NON-NLS-1$ //$NON-NLS-2$
            uuids.add(uuid);
        }

        // Then test some random numbers...
        java.util.Random rng = new java.util.Random();
        for ( int k = 0; k < nGen; k++ ) {
            long v1 = rng.nextLong();
            long v2 = rng.nextLong();
            uuid = new UUID(v1,v2);
            if ( uuids.contains(uuid) ) {
                fail( "UUID '" + uuid + "' is a duplicate!" ); //$NON-NLS-1$ //$NON-NLS-2$
            }
            uuids.add(uuid);
        }
    }
    
    /**
     * Helper method for testing UUID-string conversions.  This method takes a
     * UUID, converts it to "exportable" (String) format, then converts it back
     * to a UUID, and compares that UUID with the original.
     *
     * @param id1 The ID to test
     */
    public static void checkStringToObject( UUID id1 ) {
        String uuidString = id1.exportableForm();

        UUID id2 = null;
        try {
            id2 = (UUID)UUID.stringToObject( uuidString );
        } catch ( InvalidIDException e ) {
            fail( "Could not convert UUID exportable form '" + uuidString + "' to UUID: " //$NON-NLS-1$ //$NON-NLS-2$
                + e.getMessage() );
        }

        // Check the two parts...
        assertTrue( "UUID '" + id1 + "' conversion to string and back failed: " //$NON-NLS-1$ //$NON-NLS-2$
                    + "most significant part did not match.", //$NON-NLS-1$
                    (UUID.getPart1(id1) == UUID.getPart1(id2)) );
        assertTrue( "UUID '" + id1 + "' conversion to string and back failed: " //$NON-NLS-1$ //$NON-NLS-2$
                    + "least significant part did not match.", //$NON-NLS-1$
                    (UUID.getPart2(id1) == UUID.getPart2(id2)) );
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /**
     * This testcase generates IDs based on powers of 2, converts to exportable
     * (string) format and back to UUID, and compares to original.
     */
    public void testGenPowers() {
        helpTestGenPowers( NGEN_POWERS );
    }

    /**
     * This testcase generates IDs randomly, converts to exportable
     * (string) format and back to UUID, and compares to original.
     */
    public void testGenRandom() {
        helpTestGenRandom( NGEN_RANDOM );
    }

    /**
     * Test whether any UUIDs generated in a set are duplciates.  All generated
     * UUIDs should be unique.
     */
    public void testDuplicates() {
        helpTestDuplicates( NGEN_RANDOM );
    }
    
} // END CLASS

