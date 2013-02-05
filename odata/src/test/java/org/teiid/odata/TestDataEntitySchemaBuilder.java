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
package org.teiid.odata;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.odata4j.edm.*;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.format.xml.EdmxFormatWriter;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestDataEntitySchemaBuilder {

	@Test
	public void testMetadata() throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");		
		StringWriter sw = new StringWriter();
		EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
		EdmxFormatWriter.write(eds, sw);
		
		//System.out.println(sw.toString());
	    EdmDataServices pds = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new StringReader(sw.toString())));
	    
	    assertEquals(eds.getSchemas().size(), pds.getSchemas().size());
	    
	    for (int i = 0; i < eds.getSchemas().size(); i++) {
	    	EdmSchema expected = eds.getSchemas().get(i);
	    	EdmSchema actual = pds.getSchemas().get(i);
	    	assertEdmSchema(expected, actual);
	    }
	}

	private void assertEdmSchema(EdmSchema expected, EdmSchema actual) {
		assertEquals(expected.getEntityTypes().size(), actual.getEntityTypes().size());
		assertEquals(expected.getEntityContainers().size(), actual.getEntityContainers().size());
		assertEquals(expected.getAssociations().size(), actual.getAssociations().size());
		
		for (int i = 0; i < expected.getEntityTypes().size(); i++) {
			assertEntityType(expected.getEntityTypes().get(i), actual.getEntityTypes().get(i));
		}
		
		for (int i = 0; i < expected.getAssociations().size(); i++) {
			assertEdmAssosiation(expected.getAssociations().get(i), actual.getAssociations().get(i));
		}
		
		for (int i = 0; i < expected.getEntityContainers().size(); i++) {
			assertEntityContainer(expected.getEntityContainers().get(i), actual.getEntityContainers().get(i));
		}		
	}

	private void assertEntityType(EdmEntityType expected, EdmEntityType actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getNamespace(), actual.getNamespace());
		assertArrayEquals(expected.getKeys().toArray(new String[expected.getKeys().size()]), actual.getKeys().toArray(new String[actual.getKeys().size()]));
		
		List<EdmProperty> propertiesExpected = new ArrayList<EdmProperty>();
		List<EdmProperty> propertiesActual = new ArrayList<EdmProperty>();
		
		Iterator<EdmProperty> it = expected.getProperties().iterator();
		while(it.hasNext()) {
			propertiesExpected.add(it.next());
		}

		it = actual.getProperties().iterator();
		while(it.hasNext()) {
			propertiesActual.add(it.next());
		}
		
		assertEquals(propertiesExpected.size(), propertiesActual.size());
		for (int i = 0; i < propertiesExpected.size(); i++) {
			assertEdmProperty(propertiesExpected.get(i), propertiesActual.get(i));
		}

		List<EdmNavigationProperty> navExpected = new ArrayList<EdmNavigationProperty>();
		List<EdmNavigationProperty> navActual = new ArrayList<EdmNavigationProperty>();
		
		Iterator<EdmNavigationProperty> enpIt = expected.getDeclaredNavigationProperties().iterator();
		while(enpIt.hasNext()) {
			navExpected.add(enpIt.next());
		}
		
		enpIt = actual.getDeclaredNavigationProperties().iterator();
		while(enpIt.hasNext()) {
			navActual.add(enpIt.next());
		}
		assertEquals(navExpected.size(), navActual.size());
		for (int i = 0; i < navExpected.size(); i++) {
			assertEdmNavigationProperty(navExpected.get(i), navActual.get(i));
		}
		
		assertArrayEquals(expected.getKeys().toArray(), actual.getKeys().toArray());
	}

	private void assertEdmNavigationProperty(EdmNavigationProperty expected, EdmNavigationProperty actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEdmAssosiation(expected.getRelationship(), actual.getRelationship());
		assertEdmAssociationEnd(expected.getFromRole(), actual.getFromRole());
		assertEdmAssociationEnd(expected.getToRole(), actual.getToRole());
	}

	private void assertEdmAssociationEnd(EdmAssociationEnd expected, EdmAssociationEnd actual) {
		assertEquals(expected.getRole(), actual.getRole());
	}

	private void assertEdmAssosiation(EdmAssociation expected, EdmAssociation actual) {
		assertEquals(expected.getNamespace(), actual.getNamespace());
		assertEquals(expected.getName(), actual.getName());
		assertEdmAssociationEnd(expected.getEnd1(), actual.getEnd1());
		assertEdmAssociationEnd(expected.getEnd2(), actual.getEnd2());
		
		// check referential integrity?
	}

	private void assertEdmProperty(EdmProperty expected, EdmProperty actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getType(), actual.getType());
	}

	private void assertEntityContainer(EdmEntityContainer expected, EdmEntityContainer actual) {
		assertEquals(expected.getEntitySets().size(), actual.getEntitySets().size());
		assertEquals(expected.getAssociationSets().size(), actual.getAssociationSets().size());
		for (int i = 0; i < expected.getEntitySets().size(); i++) {
			assertEnititySet(expected.getEntitySets().get(i), actual.getEntitySets().get(i));
		}
		
		for (int i = 0; i < expected.getAssociationSets().size(); i++) {
			assertAssosiationSet(expected.getAssociationSets().get(i), actual.getAssociationSets().get(i));
		}		
	}	
	
	
	private void assertAssosiationSet(EdmAssociationSet expected, EdmAssociationSet actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEdmAssosiation(expected.getAssociation(), actual.getAssociation());
		assertEdmAssociationSetEnd(expected.getEnd1(), actual.getEnd1());
		assertEdmAssociationSetEnd(expected.getEnd1(), actual.getEnd1());
	}

	private void assertEdmAssociationSetEnd(EdmAssociationSetEnd expected, EdmAssociationSetEnd actual) {
		assertEdmAssociationEnd(expected.getRole(), actual.getRole());
	}

	private void assertEnititySet(EdmEntitySet expected, EdmEntitySet actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEntityType(expected.getType(), actual.getType());
	}

	@Test
	public void testUnquieTreatedAsKey() {
		// test that unique key is treated as key, when pk is absent.
	}
}
