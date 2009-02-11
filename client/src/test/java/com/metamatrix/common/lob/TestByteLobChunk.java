/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.lob;

import java.util.Arrays;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;


public class TestByteLobChunk extends TestCase {

    /*
     * Test method for 'com.metamatrix.common.lob.ByteLobChunk.getBytes()'
     */
    public void testGetBytes() {
        String testString = "This is test string for testing ByteLobChunk"; //$NON-NLS-1$
        LobChunk chunk = new LobChunk(testString.getBytes(), false);        
        assertEquals(testString, new String(chunk.getBytes()));
        assertFalse(chunk.isLast());
    }
    
    public void testSerialization() throws Exception {
    	String testString = "This is test string for testing ByteLobChunk"; //$NON-NLS-1$
        LobChunk chunk = new LobChunk(testString.getBytes(), true);        
        
        LobChunk result = UnitTestUtil.helpSerialize(chunk);
        assertTrue(Arrays.equals(chunk.getBytes(), result.getBytes()));
        assertTrue(result.isLast());
    }

}
