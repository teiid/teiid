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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestVDBMetaData {
	
	@Test
	public void testMarshellUnmarshellDirectParsing() throws Exception {
		
		VDBMetaData vdb = buildVDB();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		VDBMetadataParser.marshell(vdb, out);
		
		//System.out.println(new String(out.toByteArray()));

		// UnMarshell
		vdb = VDBMetadataParser.unmarshell(new ByteArrayInputStream(out.toByteArray()));
		
		validateVDB(vdb);
	}	

	static void validateVDB(VDBMetaData vdb) {
		ModelMetaData modelOne;
		ModelMetaData modelTwo;
		assertEquals("myVDB", vdb.getName()); //$NON-NLS-1$
		assertEquals("vdb description", vdb.getDescription()); //$NON-NLS-1$
		assertEquals("connection-type", "NONE", vdb.getConnectionType().name());
		assertEquals(1, vdb.getVersion());
		assertEquals("vdb-value", vdb.getPropertyValue("vdb-property")); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals("vdb-value2", vdb.getPropertyValue("vdb-property2")); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertNotNull(vdb.getModel("model-one")); //$NON-NLS-1$
		assertNotNull(vdb.getModel("model-two")); //$NON-NLS-1$
		assertNull(vdb.getModel("model-unknown")); //$NON-NLS-1$
		
		assertEquals(1, vdb.getVDBImports().size());
		VDBImport vdbImport = vdb.getVDBImports().get(0);
		assertEquals("x", vdbImport.getName());
		assertEquals(2, vdbImport.getVersion());
		
		modelOne = vdb.getModel("model-one"); //$NON-NLS-1$
		assertEquals("model-one", modelOne.getName()); //$NON-NLS-1$
		assertEquals("s1", modelOne.getSourceNames().get(0)); //$NON-NLS-1$
		assertEquals(Model.Type.PHYSICAL, modelOne.getModelType()); 
		assertEquals("model-value-override", modelOne.getPropertyValue("model-prop")); //$NON-NLS-1$ //$NON-NLS-2$
		assertFalse(modelOne.isVisible());
		assertEquals("model description", modelOne.getDescription());
		
		modelTwo = vdb.getModel("model-two"); //$NON-NLS-1$
		assertEquals("model-two", modelTwo.getName()); //$NON-NLS-1$
		assertTrue(modelTwo.getSourceNames().contains("s1")); //$NON-NLS-1$
		assertTrue(modelTwo.getSourceNames().contains("s2")); //$NON-NLS-1$
		assertEquals(Model.Type.VIRTUAL, modelTwo.getModelType()); // this is not persisted in the XML
		assertEquals("model-value", modelTwo.getPropertyValue("model-prop")); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals("DDL", modelTwo.getSchemaSourceType());
		assertEquals("DDL Here", modelTwo.getSchemaText());
		
		
		assertTrue(vdb.getValidityErrors().contains("There is an error in VDB")); //$NON-NLS-1$
		
		List<Translator> translators = vdb.getOverrideTranslators();
		assertTrue(translators.size() == 1);
		
		Translator translator = translators.get(0);
		assertEquals("oracleOverride", translator.getName());
		assertEquals("oracle", translator.getType());
		assertEquals("my-value", translator.getPropertyValue("my-property"));
		assertEquals("hello world", translator.getDescription());
		List<DataPolicy> roles = vdb.getDataPolicies();
		
		assertTrue(roles.size() == 1);
		
		DataPolicyMetadata role = vdb.getDataPolicyMap().get("roleOne"); //$NON-NLS-1$
		assertTrue(role.isAllowCreateTemporaryTables());
		assertEquals("roleOne described", role.getDescription()); //$NON-NLS-1$
		assertNotNull(role.getMappedRoleNames());
		assertTrue(role.getMappedRoleNames().contains("ROLE1")); //$NON-NLS-1$
		assertTrue(role.getMappedRoleNames().contains("ROLE2")); //$NON-NLS-1$
		
		List<DataPolicy.DataPermission> permissions = role.getPermissions();
		assertEquals(4, permissions.size());
		
		boolean lang = false;
		for (DataPolicy.DataPermission p: permissions) {
			if (p.getAllowLanguage() != null) {
				assertTrue(p.getAllowLanguage());
				assertEquals("javascript", p.getResourceName());
				lang = true;
				continue;
			}
			if (p.getResourceName().equalsIgnoreCase("myTable.T1")) { //$NON-NLS-1$
				assertTrue(p.getAllowRead());
				assertNull(p.getAllowDelete());
				continue;
			}
			if (p.getResourceName().equalsIgnoreCase("myTable.T2.col1")) { //$NON-NLS-1$
				assertEquals("col2", p.getMask());
				assertEquals(1, p.getOrder().intValue());
				continue;
			}
			assertFalse(p.getAllowRead());
			assertTrue(p.getAllowDelete());
			assertEquals("col1 = user()", p.getCondition());
			assertFalse(p.getConstraint());
		}
		assertTrue(lang);
	}

	static VDBMetaData buildVDB() {
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB"); //$NON-NLS-1$
		vdb.setDescription("vdb description"); //$NON-NLS-1$
		vdb.setConnectionType("NONE");
		vdb.setVersion(1);
		vdb.addProperty("vdb-property", "vdb-value"); //$NON-NLS-1$ //$NON-NLS-2$
		vdb.addProperty("vdb-property2", "vdb-value2"); //$NON-NLS-1$ //$NON-NLS-2$
		
		VDBImportMetadata vdbImport = new VDBImportMetadata();
		vdbImport.setName("x");
		vdbImport.setVersion(2);
		vdb.getVDBImports().add(vdbImport);
		
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model-one"); //$NON-NLS-1$
		modelOne.addSourceMapping("s1", "translator", "java:mybinding"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setModelType(Model.Type.PHYSICAL); //$NON-NLS-1$
		modelOne.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.addProperty("model-prop", "model-value-override"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setVisible(false);
		modelOne.addMessage("ERROR", "There is an error in VDB"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setDescription("model description");
		
		vdb.addModel(modelOne);
		
		ModelMetaData modelTwo = new ModelMetaData();
		modelTwo.setName("model-two"); //$NON-NLS-1$
		modelTwo.addSourceMapping("s1", "translator", "java:binding-one"); //$NON-NLS-1$ //$NON-NLS-2$
		modelTwo.addSourceMapping("s2", "translator", "java:binding-two"); //$NON-NLS-1$ //$NON-NLS-2$
		modelTwo.setModelType(Model.Type.VIRTUAL); //$NON-NLS-1$
		modelTwo.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
		modelTwo.setSchemaSourceType("DDL");
		modelTwo.setSchemaText("DDL Here");
		
		vdb.addModel(modelTwo);
		
		VDBTranslatorMetaData t1 = new VDBTranslatorMetaData();
		t1.setName("oracleOverride");
		t1.setType("oracle");
		t1.setDescription("hello world");
		t1.addProperty("my-property", "my-value");
		List<Translator> list = new ArrayList<Translator>();
		list.add(t1);
		vdb.setOverrideTranslators(list);
		
		DataPolicyMetadata roleOne = new DataPolicyMetadata();
		roleOne.setName("roleOne"); //$NON-NLS-1$
		roleOne.setDescription("roleOne described"); //$NON-NLS-1$
		roleOne.setAllowCreateTemporaryTables(true);
		PermissionMetaData perm1 = new PermissionMetaData();
		perm1.setResourceName("myTable.T1"); //$NON-NLS-1$
		perm1.setAllowRead(true);
		roleOne.addPermission(perm1);
		
		PermissionMetaData perm2 = new PermissionMetaData();
		perm2.setResourceName("myTable.T2"); //$NON-NLS-1$
		perm2.setAllowRead(false);
		perm2.setAllowDelete(true);
		perm2.setCondition("col1 = user()");
		perm2.setConstraint(false);
		roleOne.addPermission(perm2);
		
		PermissionMetaData perm3 = new PermissionMetaData();
		perm3.setResourceName("javascript"); //$NON-NLS-1$
		perm3.setAllowLanguage(true);
		roleOne.addPermission(perm3);
		
		PermissionMetaData perm4 = new PermissionMetaData();
		perm4.setResourceName("myTable.T2.col1"); //$NON-NLS-1$
		perm4.setMask("col2");
		perm4.setOrder(1);
		roleOne.addPermission(perm4);
		
		roleOne.setMappedRoleNames(Arrays.asList("ROLE1", "ROLE2")); //$NON-NLS-1$ //$NON-NLS-2$
		
		vdb.addDataPolicy(roleOne);
		
		EntryMetaData em = new EntryMetaData();
		em.setPath("/path-one");
		em.setDescription("entry one");
		em.addProperty("entryone", "1");
		vdb.getEntries().add(em);
		
		EntryMetaData em2 = new EntryMetaData();
		em2.setPath("/path-two");
		vdb.getEntries().add(em2);
		return vdb;
	}
	
	@Test
	public void testAdminMOCreation() {
		VDBMetaData vdb = new VDBMetaData();
		
		PropertiesUtils.setBeanProperty(vdb, "name", "x");
		
		assertEquals("x", vdb.getName());
	}
	
	@Test public void testVDBMetaDataMapper() {
		VDBMetaData vdb = buildVDB();
		
		ModelNode node = VDBMetadataMapper.INSTANCE.wrap(vdb, new ModelNode());
		
		vdb = VDBMetadataMapper.INSTANCE.unwrap(node);
		validateVDB(vdb);
	}
	
	@Test
	public void testVDBMetaDataDescribe() throws Exception {
		ModelNode node = VDBMetadataMapper.INSTANCE.describe(new ModelNode());
		String actual = node.toJSONString(false);
		
		assertEquals(ObjectConverterUtil.convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/vdb-describe.txt")), actual);
	}
	
	@Test
	public void testClone() {
		VDBMetaData vdb = buildVDB();
		vdb.setXmlDeployment(true);
		VDBMetaData clone = vdb.clone();
		assertTrue(clone.isXmlDeployment());
		assertEquals(1, vdb.getVDBImports().size());
		assertNotSame(clone.getModelMetaDatas(), vdb.getModelMetaDatas());
	}
}
