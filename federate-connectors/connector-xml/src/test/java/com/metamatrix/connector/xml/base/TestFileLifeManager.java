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
