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
 * TestUUIDFactoryWithoutCreation
 */
public class TestUUIDFactoryWithoutCreation extends TestCase {

    private static final String STRINGIFIED_ID_1 = "mmuuid:fdb70a40-f02f-1e59-972f-9c0cb9386e57"; //$NON-NLS-1$
    private static final String STRINGIFIED_ID_2 = "mmuuid:fdb70a41-f02f-1e59-972f-9c0cb9386e57"; //$NON-NLS-1$
    private static final String STRINGIFIED_ID_3 = "mmuuid:060f4540-f030-1e59-972f-9c0cb9386e57"; //$NON-NLS-1$
    private static final String STRINGIFIED_ID_4 = "mmuuid:060f4541-f030-1e59-972f-9c0cb9386e57"; //$NON-NLS-1$

    /**
     * Constructor for TestUUIDFactoryWithoutCreation.
     * @param name
     */
    public TestUUIDFactoryWithoutCreation(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public void helpTestStringToObject( final UUIDFactory factory, final String id ) {
        try {
            factory.stringToObject(id);
        } catch (InvalidIDException e) {
            throw new RuntimeException(e);
        }
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    public void testFactoryCreationTimeWithMultipleParses() {
    	long start = System.currentTimeMillis();
        // Create the factory ...
        final UUIDFactory myFactory = new UUIDFactory();

        // and use it immediately to parse (not to create an ID!) ...
        helpTestStringToObject(myFactory,STRINGIFIED_ID_1);
        helpTestStringToObject(myFactory,STRINGIFIED_ID_2);
        helpTestStringToObject(myFactory,STRINGIFIED_ID_3);
        helpTestStringToObject(myFactory,STRINGIFIED_ID_4);

        assertTrue(System.currentTimeMillis() - start < 500 );
    }

}
