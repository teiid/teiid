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

package com.metamatrix.common.types;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;

import com.metamatrix.common.types.BlobImpl;

import junit.framework.TestCase;

public class TestBlobImpl extends TestCase{
	private Blob blob;
	
    public TestBlobImpl(String name) {
        super(name);
    }
    
    public void setUp() throws Exception{
    	ByteArrayInputStream in = new ByteArrayInputStream("test blob".getBytes()); //$NON-NLS-1$
    	blob = new BlobImpl(in, 9);
    }
    
    public void testGetBinaryStream() throws Exception{
    	InputStream in = blob.getBinaryStream();
    	BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    	assertEquals("test blob", reader.readLine()); //$NON-NLS-1$
    	assertNull(reader.readLine());
    }
    
    public void testGetBytes() throws Exception{
    	byte[] bytes = blob.getBytes(2, 5);
    	assertEquals("est b", new String(bytes)); //$NON-NLS-1$
    }
    
    public void testLength() throws Exception{
    	assertEquals(9, blob.length()); 
    }
    
    public void testPosition() throws Exception{
    	assertEquals(3, blob.position("st".getBytes(), 2)); //$NON-NLS-1$
    }
    
    public void testEquals() throws Exception{
    	assertEquals(blob, new BlobImpl(new ByteArrayInputStream("test blob".getBytes()), 9)); //$NON-NLS-1$
    }
}
