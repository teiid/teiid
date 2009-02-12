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

package com.metamatrix.common.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import junit.framework.TestCase;

public class TestByteArrayHelper extends TestCase {

    private static final byte[] TEST_ARRAY = (new String("TEST 1 2 3 4 5 TEST")).getBytes(); //$NON-NLS-1$

	// ################################## FRAMEWORK ################################

    public TestByteArrayHelper(String name) {
        super(name);
    }
    
    //===================================================================
    //ACTUAL TESTS
    //===================================================================

    public void testToByteArrayExactChunkSize(){
        int chunkSize = TEST_ARRAY.length+1;
        InputStream is = new ByteArrayInputStream(TEST_ARRAY);
        checkByteArrayWithChunkSize(TEST_ARRAY,chunkSize, is);
    }

    public void testToByteArrayBiggerChunkSize(){
        int chunkSize = 2*TEST_ARRAY.length;
        InputStream is = new ByteArrayInputStream(TEST_ARRAY);
        checkByteArrayWithChunkSize(TEST_ARRAY,chunkSize, is);
    }

    public void testToByteArraySmallerChunkSize(){
        int chunkSize = TEST_ARRAY.length/7;
        InputStream is = new ByteArrayInputStream(TEST_ARRAY);
        checkByteArrayWithChunkSize(TEST_ARRAY,chunkSize, is);
    }

    public void testToByteArrayBufferedInputStream(){
        int chunkSize = TEST_ARRAY.length+1;
        InputStream is = new ByteArrayInputStream(TEST_ARRAY);
        is = new BufferedInputStream(is);
        checkByteArrayWithChunkSize(TEST_ARRAY,chunkSize, is);
    }

    public void testToByteArrayFakeInputStream(){
        int chunkSize = TEST_ARRAY.length+5;
        InputStream is = new FakeInputStream(TEST_ARRAY);
        checkByteArrayWithChunkSize(TEST_ARRAY,chunkSize, is);
    }


    //===================================================================
    //TESTS HELPERS
    //===================================================================

    private void checkByteArrayWithChunkSize(byte[] controlData, int chunkSize, InputStream is){
        byte[] testData = null;
        try{
            testData = ByteArrayHelper.toByteArray(is, chunkSize);
            assertNotNull(testData);
        } catch (IOException e){
            fail(e.getMessage());
        }

        assertTrue("byte arrays are NOT same size", controlData.length == testData.length); //$NON-NLS-1$
        assertTrue("byte arrays are NOT identical", Arrays.equals(controlData, testData)); //$NON-NLS-1$
    }
}
