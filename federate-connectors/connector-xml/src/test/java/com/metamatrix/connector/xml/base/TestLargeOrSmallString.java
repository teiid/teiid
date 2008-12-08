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


public class TestLargeOrSmallString extends TestCase {

	public TestLargeOrSmallString(String arg0) {
		super(arg0);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.LargeOrSmallString.createSmallString(String)'
	 */
	public void testCreateSmallString() {
		LargeOrSmallString loss = LargeOrSmallString.createSmallString("foodle");
		assertNotNull(loss);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.LargeOrSmallString.createLargeString(ValueReference)'
	 */
	public void testCreateLargeString() {
		LargeTextValueReference vr = new StringBackedValueReference(new String("large"));
		assertNotNull(vr);
		LargeOrSmallString large = LargeOrSmallString.createLargeString(vr);
		assertNotNull(large);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.LargeOrSmallString.isSmall()'
	 */
	public void testIsSmall() {
		LargeOrSmallString loss = LargeOrSmallString.createSmallString("foodle");
		assertNotNull(loss);
		LargeTextValueReference vr = new StringBackedValueReference(new String("large"));
		assertNotNull(vr);
		LargeOrSmallString large = LargeOrSmallString.createLargeString(vr);
		assertNotNull(large);
		assertTrue(loss.isSmall());
		assertFalse(large.isSmall());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.LargeOrSmallString.getAsString()'
	 */
	public void testGetAsString() {
		String small = new String("small");
		LargeOrSmallString loss = LargeOrSmallString.createSmallString("small");
		assertNotNull(loss);		
		LargeTextValueReference vr = new StringBackedValueReference(new String("large"));
		assertNotNull(vr);
		LargeOrSmallString large = LargeOrSmallString.createLargeString(vr);
		assertNotNull(large);
		String lossOut = null;
		try { 
			lossOut = loss.getAsString(); 
		} catch (Exception e) {
			fail (e.getMessage()); 
		}
		assertNotNull(lossOut);
		assertEquals(small,lossOut);
		String largeOut = null;
		try { 
			largeOut = large.getAsString(); 
		} catch (Exception e) {
			fail (e.getMessage()); 
		}
		assertEquals(((String) vr.getValue()), largeOut);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.LargeOrSmallString.getAsValueReference()'
	 */
	public void testGetAsValueReference() {
		String small = new String("small");
		LargeOrSmallString loss = LargeOrSmallString.createSmallString("small");
		assertNotNull(loss);		
		LargeTextValueReference vr = new StringBackedValueReference(new String("large"));
		assertNotNull(vr);
		LargeOrSmallString large = LargeOrSmallString.createLargeString(vr);
		assertNotNull(large);
		LargeTextValueReference lossOut = loss.getAsValueReference();
		assertNotNull(lossOut);
		assertEquals(small,lossOut.getValue().toString());
		LargeTextValueReference largeOut = large.getAsValueReference();
		assertEquals(((String) vr.getValue()), largeOut.getValue().toString());
	}

}
