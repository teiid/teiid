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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

/**
 * <p>Test cases for {@link UUIDFactory} class. </p>
 */
public class TestUUIDFactory extends TestCase {

    // Number of IDs to generate
    private static final int NGEN  = 10000;

    /** Generator of UUIDs. */
    static UUIDFactory generator = new UUIDFactory();

    /**
     * Test UUID generation for IDs generated from powers of 2.
     */
    private void helpTestGen( int nGen ) {
        UUID uuid = null;

        // First test some fairly "regular" numbers...
        for ( int j = 0; j < Math.min(nGen,63); j++ ) {
            uuid = (UUID)generator.create();
            checkStringToObject(uuid);  // Test case may fail here
        }
    }
    
    /**
     * Test for duplicates when generating a bunch of IDs.
     */
    private void helpTestDuplicates( int nGen ) {
        UUID uuid = null;
        Set uuids = new HashSet();
//        Set duplicates = new HashSet();

        // Then test some random numbers...
//        java.util.Random rng = new java.util.Random();
        for ( int k = 0; k < nGen; k++ ) {
            uuid = (UUID)generator.create();
            assertTrue( "UUID '" + uuid + "' is a duplicate!", !uuids.contains(uuid) ); //$NON-NLS-1$ //$NON-NLS-2$
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
    
    public void helpCheckVariant( Collection objectIDs, int expectedVariant ) {
        Iterator iter = objectIDs.iterator();
        while (iter.hasNext()) {
            ObjectID uuid = (ObjectID) iter.next();
            helpCheckVariant(uuid,expectedVariant);
        }
    }

    public void helpCheckVersion( Collection objectIDs, int expectedVersion ) {
        Iterator iter = objectIDs.iterator();
        while (iter.hasNext()) {
            ObjectID uuid = (ObjectID) iter.next();
            helpCheckVersion(uuid,expectedVersion);
        }
    }

    /**
     * Ensure that the variant matches the expected variant.
     * @param uuid the ObjectID
     * @param expectedVariant one of the {@link com.metamatrix.common.id.UUID.Variant UUID.Variant} constants.
     */
    public static void helpCheckVariant( ObjectID uuid, int expectedVariant ) {
        if ( UUID.getVariant(uuid) != expectedVariant ) {
            fail( "UUID '" + uuid + "' variant ('" + UUID.getVariant(uuid) + "') does not match the expected variant " + expectedVariant); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    /**
     * Ensure that the version matches the expected version.
     * @param uuid the ObjectID
     * @param expectedVersion one of the {@link com.metamatrix.common.id.UUID.Version UUID.Version} constants.
     */
    public static void helpCheckVersion( ObjectID uuid, int expectedVersion ) {
        if ( UUID.getVersion(uuid) != expectedVersion ) {
            fail( "UUID '" + uuid + "' version ('" + UUID.getVersion(uuid) + "') does not match the expected version " + expectedVersion); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /**
     * This testcase generates IDs based on powers of 2, converts to exportable
     * (string) format and back to UUID, and compares to original.
     */
    public void testGen() {
        helpTestGen( NGEN );
    }

    /**
     * Test whether any UUIDs generated in a set are duplciates.  All generated
     * UUIDs should be unique.
     */
    public void testDuplicates() {
        helpTestDuplicates( NGEN );
    }
    
    public void testVariant() {
        ObjectID uuid = generator.create();
        int variant = UUID.getVariant(uuid);
        if ( variant != UUID.Variant.STANDARD ) {
            fail("The variant " + variant + " doesn't match expected value of " + UUID.Variant.STANDARD );     //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testVersion() {
        ObjectID uuid = generator.create();
        int version = UUID.getVersion(uuid);
        if ( version != UUID.Version.PSEUDO_RANDOM ) {
            fail("The version " + version + " doesn't match expected value of " + UUID.Version.PSEUDO_RANDOM );     //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
        
} // END CLASS

