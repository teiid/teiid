/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class TestCountingInputStream extends TestCase {

	public TestCountingInputStream(String arg0) {
		super(arg0);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.CountingInputStream.read()'
	 */
	public void testRead() {						
		byte[] singleByte = new byte[]{0x22};
		ByteArrayInputStream bais = new ByteArrayInputStream(singleByte);		
		CountingInputStream cs = new CountingInputStream(bais);		
		try {
			int out = cs.read();			
			assertEquals(singleByte[0], out);
			out = cs.read();
			assertEquals(-1, out);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}

	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.CountingInputStream.read(byte[])'
	 */
	public void testReadByteArray() {
		String theString = new String("foodle");
		byte[] theBytes = theString.getBytes();		
		ByteArrayInputStream bais = new ByteArrayInputStream(theBytes);		
		CountingInputStream cs = new CountingInputStream(bais);
		byte[] read = new byte[theBytes.length];
		try {
			cs.read(read);
			for(int i =0; i < theBytes.length; i++) {
				assertEquals(theBytes[i], read[i]);
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}

	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.CountingInputStream.read(byte[], int, int)'
	 */
	public void testReadByteArrayIntInt() {
		String theString = new String("foodle");
		byte[] theBytes = theString.getBytes();		
		ByteArrayInputStream bais = new ByteArrayInputStream(theBytes);		
		CountingInputStream cs = new CountingInputStream(bais);
		byte[] read = new byte[theBytes.length];
		try {
			int bytesRead = cs.read(read, 0, theBytes.length - 1);
			for(int i =0; i < theBytes.length; i++) {
				assertEquals(theBytes.length - 1, bytesRead);
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}

	}



	/*
	 * Test method for 'com.metamatrix.connector.xml.base.CountingInputStream.getSize()'
	 */
	public void testGetSize() {
		String theString = new String("foodle");
		byte[] theBytes = theString.getBytes();		
		ByteArrayInputStream bais = new ByteArrayInputStream(theBytes);		
		CountingInputStream cs = new CountingInputStream(bais);
		assertEquals(0, cs.getSize());
		byte[] buffer = new byte[theBytes.length];
		try {
			int bytesRead = cs.read(buffer);
			assertEquals(bytesRead, cs.getSize());
			int moreBytes = cs.read(buffer);
			assertEquals(-1, moreBytes);
			assertEquals(bytesRead, cs.getSize());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
		
			
	}

}
