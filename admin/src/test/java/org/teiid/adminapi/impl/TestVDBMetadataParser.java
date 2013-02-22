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
package org.teiid.adminapi.impl;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.Collections;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.xml.sax.SAXException;

@SuppressWarnings("nls")
public class TestVDBMetadataParser {

	@Test
	public void testParseVDB() throws Exception {
		FileInputStream in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/parser-test-vdb.xml");
		VDBMetadataParser.validate(in);
		in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/parser-test-vdb.xml");
		VDBMetaData vdb = VDBMetadataParser.unmarshell(in);
		TestVDBMetaData.validateVDB(vdb);
	}
	
	@Test public void testExcludeImported() throws Exception {
		VDBMetaData metadata = TestVDBMetaData.buildVDB();
		assertNotNull(metadata.getModel("model-one"));
		metadata.setImportedModels(Collections.singleton("model-one"));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		VDBMetadataParser.marshell(metadata, baos);
		baos.close();
		VDBMetaData parsed = VDBMetadataParser.unmarshell(new ByteArrayInputStream(baos.toByteArray()));
		assertNull(parsed.getModel("model-one"));
	}
	
	@Test(expected=SAXException.class) public void testModelNameUniqueness() throws Exception {
		FileInputStream in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/model-not-unique-vdb.xml");
		VDBMetadataParser.validate(in);
	}

}
