/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import com.metamatrix.data.api.ConnectorEnvironment;

public class TestFileLifeManager extends TestCase {

	public TestFileLifeManager(String arg0) {
		super(arg0);
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileLifeManager.finalize()'
	 */
	public void testFinalize() {
		String filePath = new String(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		File theFile = new File(filePath);
		XMLConnector conn = new XMLConnector();
		ConnectorEnvironment env = ProxyObjectFactory.getDefaultTestConnectorEnvironment();		
		try {
			conn.initialize(env);
			theFile.createNewFile();
			FileLifeManager mgr = new FileLifeManager(theFile, conn.getLogger());
			mgr.createRandomAccessFile();
			mgr.finalize();
			assertFalse(theFile.exists());
		} catch (Exception ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}

/*	public void testFinalizeNoLog() {
		String filePath = new String(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		File theFile = new File(filePath);		
		try {
			theFile.createNewFile();
			FileLifeManager mgr = new FileLifeManager(theFile, null);
			mgr.createRandomAccessFile();
			mgr.finalize();
			assertFalse(theFile.exists());
		} catch (Exception ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}

*/	public void testFinalizeDeletedFile() {
		String filePath = new String(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		File theFile = new File(filePath);
		XMLConnector conn = new XMLConnector();
		ConnectorEnvironment env = ProxyObjectFactory.getDefaultTestConnectorEnvironment();		
		try {
			conn.initialize(env);
			theFile.createNewFile();
			FileLifeManager mgr = new FileLifeManager(theFile, conn.getLogger());
			mgr.createRandomAccessFile();
			theFile.delete();
			mgr.finalize();
			assertFalse(theFile.exists());
		} catch (Exception ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}
	
	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileLifeManager.FileLifeManager(File, ConnectorLogger)'
	 */
	public void testFileLifeManager() {

	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileLifeManager.createRandomAccessFile()'
	 */
	public void testCreateRandomAccessFile() {
		File theFile = new File(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		
		try {
			if(!theFile.exists()) {
				theFile.createNewFile();
			}
			FileLifeManager mgr = new FileLifeManager(theFile, null);
			java.io.RandomAccessFile file = mgr.createRandomAccessFile();
			assertNotNull(file);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileLifeManager.doesMatch(String)'
	 */
	public void testDoesMatch() {
		String filePath = new String(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		File theFile = new File(filePath);
		XMLConnector connector = new XMLConnector();
		ConnectorEnvironment env = ProxyObjectFactory.getDefaultTestConnectorEnvironment();		
		try {
			connector.initialize(env);
			FileLifeManager mgr = new FileLifeManager(theFile, connector.getLogger());
			assertTrue(mgr.doesMatch(filePath));
			assertFalse(mgr.doesMatch("foodle"));
		} catch (Exception ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileLifeManager.getLength()'
	 */
	public void testGetLength() {
		String filePath = new String(ProxyObjectFactory.getDocumentsFolder() + "/finalize.xml");
		File theFile = new File(filePath);
		try {
			FileLifeManager mgr = new FileLifeManager(theFile, null);
			long length = mgr.getLength();
			assertEquals(theFile.length(), mgr.getLength());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fail(ioe.getMessage());
		}			
	}	
}
