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

package com.metamatrix.connector.xml.base;

import junit.framework.TestCase;

import com.metamatrix.connector.exception.ConnectorException;

public class TestStringBackedValueReference extends TestCase {

	public TestStringBackedValueReference(String arg0) {
		super(arg0);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.StringBackedValueReference.StringBackedValueReference(String)'
	 */
	public void testStringBackedValueReference() {
		String testString = new String("testString");
		StringBackedValueReference ref = new StringBackedValueReference(testString);
		assertNotNull(ref);			
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.StringBackedValueReference.getValue()'
	 */
	public void testGetValue() {
		String testString = new String("testString");
		StringBackedValueReference ref = new StringBackedValueReference(testString);
		assertNotNull(ref);	
		Object val = ref.getValue();
		assertTrue(val instanceof String);
		assertEquals(testString, ((String) val));
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.StringBackedValueReference.getSize()'
	 */
	public void testGetSize() {
		String testString = new String("testString");
		StringBackedValueReference ref = new StringBackedValueReference(testString);
		assertNotNull(ref);	
		assertEquals(testString.length(), ref.getSize());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.StringBackedValueReference.createChunk(long, int)'
	 */
	public void testGetContentAsString() {
		String testString = new String("testString");
		StringBackedValueReference ref = new StringBackedValueReference(testString);
		assertNotNull(ref);	
		String thing = null; 
		try {
			thing = ref.getContentAsString();
		} catch (Exception e) { 
			fail(e.getMessage());
		}
		assertEquals(thing,"testString");
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.StringBackedValueReference.isBinary()'
	 */
	public void testIsBinary() {
		String testString = new String("testString");
		StringBackedValueReference ref = new StringBackedValueReference(testString);
		assertNotNull(ref);	
		assertFalse(ref.isBinary());
	}
}
