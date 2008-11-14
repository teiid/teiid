/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import junit.framework.TestCase;

import com.metamatrix.data.exception.ConnectorException;

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
