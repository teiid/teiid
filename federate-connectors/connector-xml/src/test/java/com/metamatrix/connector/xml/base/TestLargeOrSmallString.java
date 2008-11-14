/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
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
