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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.core.util.PropertiesUtils;

@SuppressWarnings("nls")
public class TestVDBMetaData {

	@Test
	public void testMarshellUnmarshell() throws Exception {
		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB"); //$NON-NLS-1$
		vdb.setDescription("vdb description"); //$NON-NLS-1$
		vdb.setVersion(1);
		vdb.addProperty("vdb-property", "vdb-value"); //$NON-NLS-1$ //$NON-NLS-2$
		
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model-one"); //$NON-NLS-1$
		modelOne.addSourceMapping("s1", "translator", "java:mybinding"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setModelType(Model.Type.PHYSICAL); //$NON-NLS-1$
		modelOne.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.addProperty("model-prop", "model-value-override"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setVisible(false);
		modelOne.addError("ERROR", "There is an error in VDB"); //$NON-NLS-1$ //$NON-NLS-2$
		modelOne.setDescription("model description");
		
		vdb.addModel(modelOne);
		
		ModelMetaData modelTwo = new ModelMetaData();
		modelTwo.setName("model-two"); //$NON-NLS-1$
		modelTwo.addSourceMapping("s1", "translator", "java:binding-one"); //$NON-NLS-1$ //$NON-NLS-2$
		modelTwo.addSourceMapping("s2", "translator", "java:binding-two"); //$NON-NLS-1$ //$NON-NLS-2$
		modelTwo.setModelType(Model.Type.VIRTUAL); //$NON-NLS-1$
		modelTwo.addProperty("model-prop", "model-value"); //$NON-NLS-1$ //$NON-NLS-2$
		
		vdb.addModel(modelTwo);
		
		TranslatorMetaData t1 = new TranslatorMetaData();
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
		roleOne.addPermission(perm2);
		
		roleOne.setMappedRoleNames(Arrays.asList("ROLE1", "ROLE2")); //$NON-NLS-1$ //$NON-NLS-2$
		
		vdb.addDataPolicy(roleOne);
		
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); 		 //$NON-NLS-1$
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		Marshaller marshell = jc.createMarshaller();
		marshell.setSchema(schema);
		marshell.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		StringWriter sw = new StringWriter();
		marshell.marshal(vdb, sw);
				
		//System.out.println(sw.toString());

		// UnMarshell
		Unmarshaller un = jc.createUnmarshaller();
		un.setSchema(schema);
		vdb = (VDBMetaData)un.unmarshal(new StringReader(sw.toString()));
		
		assertEquals("myVDB", vdb.getName()); //$NON-NLS-1$
		assertEquals("vdb description", vdb.getDescription()); //$NON-NLS-1$
		assertEquals(1, vdb.getVersion());
		assertEquals("vdb-value", vdb.getPropertyValue("vdb-property")); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertNotNull(vdb.getModel("model-one")); //$NON-NLS-1$
		assertNotNull(vdb.getModel("model-two")); //$NON-NLS-1$
		assertNull(vdb.getModel("model-unknown")); //$NON-NLS-1$
		
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
		
		DataPolicyMetadata role = vdb.getDataPolicy("roleOne"); //$NON-NLS-1$
		assertTrue(role.isAllowCreateTemporaryTables());
		assertEquals("roleOne described", role.getDescription()); //$NON-NLS-1$
		assertNotNull(role.getMappedRoleNames());
		assertTrue(role.getMappedRoleNames().contains("ROLE1")); //$NON-NLS-1$
		assertTrue(role.getMappedRoleNames().contains("ROLE2")); //$NON-NLS-1$
		
		List<DataPolicy.DataPermission> permissions = role.getPermissions();
		assertEquals(2, permissions.size());
		
		for (DataPolicy.DataPermission p: permissions) {
			if (p.getResourceName().equalsIgnoreCase("myTable.T1")) { //$NON-NLS-1$
				assertTrue(p.getAllowRead());
				assertNull(p.getAllowDelete());
			}
			else {
				assertFalse(p.getAllowRead());
				assertTrue(p.getAllowDelete());
			}
		}
	}
	
	@Test
	public void testAdminMOCreation() {
		VDBMetaData vdb = new VDBMetaData();
		
		PropertiesUtils.setBeanProperty(vdb, "url", "http://teiid.org/myvdb.vdb");
		
		assertEquals("http://teiid.org/myvdb.vdb", vdb.getUrl());
	}
	
	@Test public void testSetUrlVersion() throws MalformedURLException {
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("foo");
		vdb.setUrl(new URL("file:///x/foo.2.vdb"));
		assertEquals(2, vdb.getVersion());
	}
}
