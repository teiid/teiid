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

import junit.framework.TestCase;

import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.vdb.runtime.BasicVDBDefn;

public class TestVDBArchive extends TestCase {
	static final String vdbPath = UnitTestUtil.getTestDataPath()+"/Test.vdb";
	
	
	/*
	 * Test the setting of the WSDL Defined flag
	 */
	public void testWSDLVDBArchive() throws Exception {
		
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/BooksWebService_VDB.vdb", vdbPath);
		
		File vdbFile = new File(vdbPath);
		
		VDBArchive archive = new VDBArchive(vdbFile);
		
		assertEquals(true, archive.getConfigurationDef().hasWSDLDefined());
		
		archive.close();
		vdbFile.delete();
	}
	
	/*
	 * Updates based on the File VDB
	 */
	public void testFileVDBArchive() throws Exception {
		
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/QT_Ora9DS.vdb", vdbPath);
		
		
		File vdbFile = new File(vdbPath);
		VDBArchive archive = new VDBArchive(vdbFile);
		
		BasicVDBDefn def = archive.getConfigurationDef();
		assertEquals("QT_Ora9DS", def.getName());
		assertEquals(8, def.getModels().size());
		
		assertEquals(ModelType.PHYSICAL, def.getModel("BQT2").getModelType());
		assertEquals("/BQT/XQTNestedDoc.xmi", def.getModel("XQTNestedDoc").getPath());
		
		assertNull(archive.getDataRoles());
		
		// add data file
		String roles = "<data>roles, roles, roles.. any one?</data>";
		archive.updateRoles(roles.toCharArray());
		
		// replace with some other def file
		String anotherDEF = UnitTestUtil.getTestDataPath()+File.separator+"example.def";
		archive.updateConfigurationDef(VDBArchive.readFromDef(new FileInputStream(anotherDEF)));

		// now close the old VDB File 
		archive.close();
		
		// now read the modified file and make sure the updates from the above exist
		archive = new VDBArchive(vdbFile);
		
		def = archive.getConfigurationDef();
		assertEquals("TransactionsRevisited", def.getName());
		assertEquals(3, def.getModels().size());
		
		assertNotNull(archive.getDataRoles());
		assertEquals(roles, new String(archive.getDataRoles()).trim());
		
		archive.close();
		vdbFile.delete();
	}
	
	public void testStreamVDBArchive() throws Exception {
		
		File vdbFile = new File(vdbPath);
		VDBArchive archive = new VDBArchive(new FileInputStream(UnitTestUtil.getTestDataPath()+"/QT_Ora9DS.vdb"));
		
		BasicVDBDefn def = archive.getConfigurationDef();
		assertEquals("QT_Ora9DS", def.getName());
		assertEquals(8, def.getModels().size());
		
		assertNull(archive.getDataRoles());
		
		// add data file
		String roles = "<data>roles, roles, roles.. any one?</data>";
		archive.updateRoles(roles.toCharArray());
		
		// replace with some other def file
		String anotherDEF = UnitTestUtil.getTestDataPath()+File.separator+"example.def";
		archive.updateConfigurationDef(VDBArchive.readFromDef(new FileInputStream(anotherDEF)));

		// now close the old VDB File, save the changes to a new file
		FileOutputStream fos = new FileOutputStream(vdbFile);
		archive.write(fos);
		fos.close();
		archive.close();
		
		// now read the modified file and make sure the updates from the above exist
		archive = new VDBArchive(vdbFile);
		
		def = archive.getConfigurationDef();
		assertEquals("TransactionsRevisited", def.getName());
		assertEquals(3, def.getModels().size());
		
		assertNotNull(archive.getDataRoles());
		assertEquals(roles, new String(archive.getDataRoles()).trim());
		
		archive.close();
		vdbFile.delete();
	}	
}
