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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * TestParsingObjectIDs
 */
public class TestParsingObjectIDs extends TestCase {

    private static final boolean DEBUG_PRINT = false;
    private static final String COMMENT_PREFIX = "#"; //$NON-NLS-1$
    private static final String UUIDS_FILENAME2 = "UUIDs.txt"; //$NON-NLS-1$

    /**
     * Constructor for TestParsingUUID.
     * @param name
     */
    public TestParsingObjectIDs(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    /**
     * Load the stringified UUIDs from a file and test 'stringToObject'.
     */
    public static Collection helpTestStringToObject( String filename, boolean checkForDuplicates ) throws Exception {
        String stringifiedID = null;
        final Set objectIDs = new HashSet();
        try {
            Collection stringifiedIDs = helpLoad(filename);
            if ( DEBUG_PRINT ) {
                System.out.println("# of ids = " + stringifiedIDs.size() ); //$NON-NLS-1$
            }
            final Iterator iter = stringifiedIDs.iterator();
            while (iter.hasNext()) {
                stringifiedID = (String) iter.next();
                final ObjectID objectId = IDGenerator.getInstance().stringToObject(stringifiedID);
                if ( checkForDuplicates && objectIDs.contains(objectId) ) {
                    fail( "ObjectID '" + objectId + "' is a duplicate!" ); //$NON-NLS-1$ //$NON-NLS-2$
                }
                objectIDs.add(objectId);
            }

        } catch ( InvalidIDException e ) {
            fail("Unable to convert stringified ID \"" + stringifiedID + "\" to an ObjectID"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return objectIDs;
    }

    /**
     * Load the test data file from the input stream and return the collection of Strings
     * loaded from the stream that each contain a stringified UUID.
     */
    public static Collection helpLoad( String filename ) throws Exception {
        // Create the input stream ...
        InputStream stream = new FileInputStream(UnitTestUtil.getTestDataFile(filename));
        BufferedReader reader = new BufferedReader( new InputStreamReader(stream) );

        // Process the stream ...
        Collection result = new ArrayList();
        try {
            String line = null;
            while ( (line = reader.readLine()) != null ) {
                if ( line.trim().length() != 0 && !line.startsWith(COMMENT_PREFIX) ) {
                    result.add( line );
                }
            }
        } catch ( IOException e ) {    // may get if EOF reached while reading a line
        } finally {
            try {
                reader.close();
            } catch ( IOException e ) {
                fail("Unable to close the stream to the test file \"" + filename + "\"");     //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        return result;
    }

    public void helpCheckVariant( Collection objectIDs, int expectedVariant ) {
        Iterator iter = objectIDs.iterator();
        while (iter.hasNext()) {
            ObjectID objectId = (ObjectID) iter.next();
            if ( objectId instanceof UUID ) {
                helpCheckVariant((UUID)objectId,expectedVariant);
            }
        }
    }

    /**
     * Ensure that the variant matches the expected variant.
     * @param uuid the ObjectID
     * @param expectedVariant one of the {@link com.metamatrix.common.id.UUID.Variant UUID.Variant} constants.
     */
    public static void helpCheckVariant( UUID uuid, int expectedVariant ) {
//System.out.println("variant == UUID.Variant.STANDARD        ? " + (UUID.getVariant(uuid)==UUID.Variant.STANDARD) );
//System.out.println("variant == UUID.Variant.RESERVED_FUTURE ? " + (UUID.getVariant(uuid)==UUID.Variant.RESERVED_FUTURE) );
//System.out.println("variant == UUID.Variant.NSC_COMPATIBLE  ? " + (UUID.getVariant(uuid)==UUID.Variant.NSC_COMPATIBLE) );
//System.out.println("variant == UUID.Variant.MICROSOFT       ? " + (UUID.getVariant(uuid)==UUID.Variant.MICROSOFT) );
        if ( UUID.getVariant(uuid) != expectedVariant ) {
            fail( "UUID '" + uuid + "' variant ('" + UUID.getVariant(uuid) + "') does not match the expected variant " + expectedVariant); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    /**
     * Reads UUIDs from a file and constructs instances from the Strings.
     */
    public void testStringToObjectFromUUIDFile() throws Exception {
        Collection ids = helpTestStringToObject(UUIDS_FILENAME2, false);
        helpCheckVariant(ids,UUID.Variant.STANDARD);
        //helpCheckVersion(ids,UUID.Version.TIME_BASED);    // mixture of versions!
    }
}
