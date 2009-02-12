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

package com.metamatrix.connector.xml.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

import junit.framework.TestCase;

public class TestFileBackedValueReference extends TestCase {

	public TestFileBackedValueReference(String arg0) {
		super(arg0);
	}

	protected void setUp() throws Exception {
		super.setUp();

	}

	private File createFile(String fileName) throws Exception {
		File file = new File(ProxyObjectFactory.getDocumentsFolder() + "/" + fileName);
		file.createNewFile();
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write("Colorless Green dreams sleep furiously");
		writer.flush();
		writer.close();
		return file;
	}
	
	private File createFile(String fileName, String encoding) throws Exception {
		File file = new File(ProxyObjectFactory.getDocumentsFolder() + "/" + fileName);
		file.createNewFile();
        FileOutputStream stream = new FileOutputStream(file);
        OutputStreamWriter writer = new OutputStreamWriter(stream, encoding);
		writer.write("Colorless Green dreams sleep furiously");
		writer.flush();
		writer.close();
		return file;
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.FileBackedValueReference(FileLifeManager)'
	 */
	public void testFileBackedValueReference() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testFileBackedValueReference.xml");
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.getValue()'
	 */
	public void testGetValue() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testGetValue.xml");
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			Object obj = ref.getValue();
			assertNotNull(obj);		
			assertTrue(obj instanceof java.io.RandomAccessFile);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.getSize()'
	 */
	public void testGetSize() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testGetSize.xml");
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			long size = ref.getSize();
			assertEquals(size, file.length() / 2);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.getContentAsString()'
	 */
	public void testGetContentAsString() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testGetContentAsString.xml", FileBackedValueReference.getEncoding());
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			String thing = ref.getContentAsString();	
			assertEquals("Colorless Green dreams sleep furiously", thing);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.isBinary()'
	 */
	public void testIsBinary() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testIsBinary.xml");
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			assertFalse(ref.isBinary());
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.base.FileBackedValueReference.getEncoding()'
	 */
	public void testGetEncoding() {
		XMLConnector connector = new XMLConnector();
		try {
			File file = createFile("testGetEncoding.xml");
			FileLifeManager mgr = new FileLifeManager(file, connector.getLogger());
			FileBackedValueReference ref = new FileBackedValueReference(mgr);
			assertNotNull(ref);
			assertEquals("UTF-16BE", ref.getEncoding());
			assertEquals("UTF-16BE", FileBackedValueReference.getEncoding());
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
