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

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.junit.Test;
import org.teiid.adminapi.Model;


public class TestVDBMetaData {

	@Test
	public void testMarshellUnmarshell() throws Exception {
		
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("myVDB");
		vdb.setDescription("vdb description");
		vdb.setVersion(1);
		vdb.addProperty("vdb-property", "vdb-value");
		
		ModelMetaData modelOne = new ModelMetaData();
		modelOne.setName("model-one");
		modelOne.addSourceMapping("s1", "java:mybinding");
		modelOne.setModelType("PHYSICAL");
		modelOne.addProperty("model-prop", "model-value");
		modelOne.addProperty("model-prop", "model-value-override");
		modelOne.setVisible(false);
		modelOne.addError("ERROR", "There is an error in VDB");
		
		vdb.addModel(modelOne);
		
		ModelMetaData modelTwo = new ModelMetaData();
		modelTwo.setName("model-two");
		modelTwo.addSourceMapping("s1", "java:binding-one");
		modelTwo.addSourceMapping("s2", "java:binding-two");
		modelTwo.setModelType("VIRTUAL");
		modelTwo.addProperty("model-prop", "model-value");
		
		vdb.addModel(modelTwo);
		

		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); 		
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		Marshaller marshell = jc.createMarshaller();
		marshell.setSchema(schema);
		marshell.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		StringWriter sw = new StringWriter();
		marshell.marshal(vdb, sw);
				
		System.out.println(sw.toString());

		// UnMarshell
		Unmarshaller un = jc.createUnmarshaller();
		un.setSchema(schema);
		vdb = (VDBMetaData)un.unmarshal(new StringReader(sw.toString()));
		
		assertEquals("myVDB", vdb.getName());
		assertEquals("vdb description", vdb.getDescription());
		assertEquals(1, vdb.getVersion());
		assertEquals("vdb-value", vdb.getPropertyValue("vdb-property"));
		
		assertNotNull(vdb.getModel("model-one"));
		assertNotNull(vdb.getModel("model-two"));
		assertNull(vdb.getModel("model-unknown"));
		
		modelOne = vdb.getModel("model-one");
		assertEquals("model-one", modelOne.getName());
		assertEquals("s1", modelOne.getSourceNames().get(0));
		assertEquals(Model.Type.PHYSICAL, modelOne.getModelType()); 
		assertEquals("model-value-override", modelOne.getPropertyValue("model-prop"));
		assertFalse(modelOne.isVisible());

		
		modelTwo = vdb.getModel("model-two");
		assertEquals("model-two", modelTwo.getName());
		assertTrue(modelTwo.getSourceNames().contains("s1"));
		assertTrue(modelTwo.getSourceNames().contains("s2"));
		assertEquals(Model.Type.VIRTUAL, modelTwo.getModelType()); // this is not persisted in the XML
		assertEquals("model-value", modelTwo.getPropertyValue("model-prop"));
		
		
		assertTrue(vdb.getValidityErrors().contains("There is an error in VDB"));
	}
}
