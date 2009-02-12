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

package com.metamatrix.common.vdb.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.vdb.runtime.BasicVDBDefn;

public class TestDEFReaderWriter extends TestCase {

	public void testReadDEF() throws Exception {
	
		String defFile = UnitTestUtil.getTestDataPath()+"/example.def";
		
		DEFReaderWriter reader = new DEFReaderWriter();
		BasicVDBDefn defn = reader.read(new FileInputStream(defFile));
		
		checkDEF(defn);
	}

	private void checkDEF(BasicVDBDefn defn) {

		Properties header = defn.getHeaderProperties();
		assertEquals("MetaMatrix Console", header.getProperty(DEFReaderWriter.Header.APPLICATION_CREATED_BY));
		assertEquals("5.5:2849", header.getProperty(DEFReaderWriter.Header.APPLICATION_VERSION));
		assertEquals("2007-10-18T11:01:41.622-06:00", header.getProperty(DEFReaderWriter.Header.MODIFICATION_TIME));
		assertEquals("5.5", header.getProperty(DEFReaderWriter.Header.SYSTEM_VERSION));
		assertEquals("metamatrixadmin", header.getProperty(DEFReaderWriter.Header.USER_CREATED_BY));
		assertEquals("4.1", header.getProperty(DEFReaderWriter.Header.VDB_EXPORTER_VERSION));
		
		
		// vdb info
		assertEquals("TransactionsRevisited", defn.getName());
		assertEquals("1", defn.getVersion());
		assertEquals("TR Desc", defn.getDescription());
		assertEquals("TransactionsRevisited.vdb", defn.getFileName());
		assertEquals("mmuuid:9f019542-006a-1087-ad13-8ecae4ac6516", defn.getUUID());

		// model info
		assertEquals(3, defn.getModels().size());
		
		ModelInfo model = defn.getModel("pm2");		
		assertEquals("pm2", model.getName());
		assertEquals(ModelInfo.PRIVATE, model.getVisibility());
		assertEquals(Boolean.TRUE.booleanValue(), model.isMultiSourceBindingEnabled());
		assertEquals(1, model.getConnectorBindingNames().size());
		assertEquals("pm2 Connector", model.getConnectorBindingNames().get(0));
		
		ModelInfo model2 = defn.getModel("vm");		
		assertEquals("vm", model2.getName());
		
		assertEquals(ModelInfo.PUBLIC, model2.getVisibility());
		
		assertFalse(model2.isMultiSourceBindingEnabled());
		assertEquals(0, model2.getConnectorBindingNames().size());
		
		// connector types
		assertEquals(2, defn.getConnectorTypes().size());
		assertNotNull(defn.getConnectorType("Oracle ANSI JDBC XA Connector"));
		assertNull(defn.getConnectorType("dummy"));
	
		// connector bindings
		assertEquals(2, defn.getConnectorBindings().size());
		assertNotNull(defn.getConnectorBindingByName("pm2 Connector"));
		assertNull(defn.getConnectorType("dummy"));
	}
	
	public void testDEFWrite() throws IOException {
		File defFile = new File(UnitTestUtil.getTestDataPath()+"/example.def");
		File exportedFile = new File(UnitTestUtil.getTestDataPath()+"/example.def.exported");
		
		DEFReaderWriter reader = new DEFReaderWriter();
		BasicVDBDefn defn = reader.read(new FileInputStream(defFile));

		Properties header = new Properties();
		header.setProperty(DEFReaderWriter.Header.VDB_EXPORTER_VERSION, "4.1");
		header.setProperty(DEFReaderWriter.Header.APPLICATION_CREATED_BY, "MetaMatrix Console");
		header.setProperty(DEFReaderWriter.Header.APPLICATION_VERSION, "5.5:2849");
		header.setProperty(DEFReaderWriter.Header.USER_CREATED_BY, "metamatrixadmin");
		header.setProperty(DEFReaderWriter.Header.SYSTEM_VERSION, "5.5");
		header.setProperty(DEFReaderWriter.Header.MODIFICATION_TIME, "2007-10-18T11:01:41.622-06:00");
		
		reader.write(new FileOutputStream(exportedFile), defn, header);
		
		checkDEF(reader.read(new FileInputStream(exportedFile)));
		
		exportedFile.delete();
	}
}
