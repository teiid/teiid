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
import java.util.Iterator;

import org.core4j.Enumerable;
import org.junit.Test;
import org.odata4j.edm.*;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.format.xml.EdmxFormatWriter;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

public class TestDataEntitySchemaBuilder {

	@Test
	public void testMetadata() throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");		
		StringWriter sw = new StringWriter();
		EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
		EdmxFormatWriter.write(eds, sw);
		System.out.println(sw.toString());
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
		
		for (int i = 0; i < expected.getEntityTypes().size(); i++) {
			assertEntityType(expected.getEntityTypes().get(i), actual.getEntityTypes().get(i));
		}
		
		for (int i = 0; i < expected.getEntityContainers().size(); i++) {
			assertEntityContainer(expected.getEntityTypes().get(i), actual.getEntityTypes().get(i));
		}		
	}

	private void assertEntityType(EdmEntityType expected, EdmEntityType actual) {
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getNamespace(), actual.getNamespace());
		assertArrayEquals(expected.getKeys().toArray(new String[expected.getKeys().size()]), actual.getKeys().toArray(new String[actual.getKeys().size()]));
		
		Iterator<EdmProperty> propertiesExpected = expected.getProperties().iterator();
		while(propertiesExpected.hasNext()) {
			//TODO:
			propertiesExpected.next();
		}
		Iterator<EdmProperty> propertiesActual = actual.getProperties().iterator();
		
		Enumerable<EdmNavigationProperty> enpExpected = expected.getDeclaredNavigationProperties();
		Enumerable<EdmNavigationProperty> enpActual = actual.getDeclaredNavigationProperties();
	}

	private void assertEntityContainer(EdmEntityType expected, EdmEntityType actual) {
		//TODO:
	}	
	
	
	@Test
	public void testUnquieTreatedAsKey() {
		// test that unique key is treated as key, when pk is absent.
	}
}
