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
package org.teiid.adminapi.jboss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.impl.TestVDBUtility;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestVDBMetaData {
	
	@Test
	public void testMarshellUnmarshellDirectParsing() throws Exception {
		
		VDBMetaData vdb = TestVDBUtility.buildVDB();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		VDBMetadataParser.marshell(vdb, out);
		
		//System.out.println(new String(out.toByteArray()));

		// UnMarshell
		vdb = VDBMetadataParser.unmarshell(new ByteArrayInputStream(out.toByteArray()));
		
		TestVDBUtility.validateVDB(vdb);
	}	


	
	@Test
	public void testAdminMOCreation() {
		VDBMetaData vdb = new VDBMetaData();
		
		PropertiesUtils.setBeanProperty(vdb, "name", "x");
		
		assertEquals("x", vdb.getName());
	}
	
	@Test public void testVDBMetaDataMapper() {
		VDBMetaData vdb = TestVDBUtility.buildVDB();
		
		ModelNode node = VDBMetadataMapper.INSTANCE.wrap(vdb, new ModelNode());
		
		vdb = VDBMetadataMapper.INSTANCE.unwrap(node);
		TestVDBUtility.validateVDB(vdb);
	}
	
	@Test
	public void testVDBMetaDataDescribe() throws Exception {
		ModelNode node = VDBMetadataMapper.INSTANCE.describe(new ModelNode());
		String actual = node.toJSONString(false);
		
		assertEquals(ObjectConverterUtil.convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/vdb-describe.txt")), actual);
	}
	
	@Test
	public void testClone() {
		VDBMetaData vdb = TestVDBUtility.buildVDB();
		vdb.setXmlDeployment(true);
		VDBMetaData clone = vdb.clone();
		assertTrue(clone.isXmlDeployment());
		assertEquals(1, vdb.getVDBImports().size());
		assertNotSame(clone.getModelMetaDatas(), vdb.getModelMetaDatas());
		//assertNotSame(clone.getDataPolicyMap(), vdb.getDataPolicyMap());
	}
}
