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
package org.teiid.translator.odata;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;
import org.odata4j.edm.*;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.parser.QueryParser;

@SuppressWarnings("nls")
public class TestODataMetadataProcessor {

	@Test
	public void testSchema() throws Exception {
		String csdl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.xml"));
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		Properties props = new Properties();
		props.setProperty("schemaNamespace", "ODataWeb.Northwind.Model");
		props.setProperty("entityContainer", "NorthwindEntities");
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		processor.getMetadata(mf, new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(new ByteArrayInputStream(csdl.getBytes())))));
		
		String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
		//System.out.println(ddl);	
		
		MetadataFactory mf2 = new MetadataFactory(null, 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null); 
		QueryParser.getQueryParser().parseDDL(mf2, ddl);		
	}

	@Test
	public void testEnititySet() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		ArrayList<EdmProperty.Builder> props = new ArrayList<EdmProperty.Builder>();
		props.add(EdmProperty.newBuilder("name").setType(EdmSimpleType.STRING).setMaxLength(25));
		props.add(EdmProperty.newBuilder("dob").setType(EdmSimpleType.DATETIME).setNullable(true));
		props.add(EdmProperty.newBuilder("ssn").setType(EdmSimpleType.INT64).setNullable(false));
		
		EdmEntityType entity = EdmEntityType.newBuilder().addProperties(props).addKeys("ssn").build();
		processor.addEntitySetAsTable(mf, "Person", entity);
		
		assertNotNull(mf.getSchema().getTable("Person"));
		
		Table person = mf.getSchema().getTable("Person");
		assertEquals(3, person.getColumns().size());
		
		assertNotNull(person.getColumnByName("name"));
		assertNotNull(person.getColumnByName("dob"));
		assertNotNull(person.getColumnByName("ssn"));
		
		Column name = person.getColumnByName("name");
		Column dob = person.getColumnByName("dob");
		Column ssn = person.getColumnByName("ssn");
		
		assertEquals("string", name.getDatatype().getRuntimeTypeName());
		assertEquals("timestamp", dob.getDatatype().getRuntimeTypeName());
		assertEquals("long", ssn.getDatatype().getRuntimeTypeName());
		
		assertTrue(name.getNullType() == NullType.No_Nulls);
		assertTrue(dob.getNullType() == NullType.Nullable);
		assertTrue(ssn.getNullType() == NullType.No_Nulls);
		
		assertEquals(25, name.getLength());
		
		assertNotNull(person.getPrimaryKey());
		
		assertEquals(1, person.getPrimaryKey().getColumns().size());
		assertEquals("ssn", person.getPrimaryKey().getColumns().get(0).getName());
		
		assertTrue(person.getForeignKeys().isEmpty());
	}
	
	@Test
	public void testEnititySetWithComplexType() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		processor.addEntitySetAsTable(mf, "Person", buildPersonEntity(buildAddressEntity().build()).build());
		
		assertEquals(1, mf.getSchema().getTables().size());
		assertNotNull(mf.getSchema().getTable("Person"));
		
		Table personTable = mf.getSchema().getTable("Person");
		assertEquals(5, personTable.getColumns().size());
		
		assertNotNull(personTable.getColumnByName("address_street"));
	}

	
	@Test
	public void testMultipleEnititySetWithSameComplexType() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		processor.addEntitySetAsTable(mf, "Person", buildPersonEntity(buildAddressEntity().build()).build());
		processor.addEntitySetAsTable(mf, "Business", buildBusinessEntity(buildAddressEntity().build()).build());
		
		assertEquals(2, mf.getSchema().getTables().size());
		assertNotNull(mf.getSchema().getTable("Person"));
		assertNotNull(mf.getSchema().getTable("Business"));
		
		Table personTable = mf.getSchema().getTable("Person");
		assertEquals(5, personTable.getColumns().size());
		
		Table businessTable = mf.getSchema().getTable("Business");
		assertEquals(4, businessTable.getColumns().size());
		
		assertNotNull(personTable.getColumnByName("address_street"));
		assertNotNull(businessTable.getColumnByName("address_street"));
	}
	
	@Test
	public void testOneToOneAssosiation() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		EdmEntityType.Builder g1Entity = entityType("g1");
		EdmEntityType.Builder g2Entity = entityType("g2");
		
		EdmAssociationEnd.Builder aend1 = EdmAssociationEnd.newBuilder()
				.setRole("source").setType(g1Entity)
				.setMultiplicity(EdmMultiplicity.ONE);
		
		EdmAssociationEnd.Builder aend2 = EdmAssociationEnd.newBuilder()
				.setRole("target").setType(g2Entity)
				.setMultiplicity(EdmMultiplicity.ONE);		
		
		EdmAssociation.Builder assocition = EdmAssociation.newBuilder()
				.setNamespace("namspace").setName("one_2_one")
				.setEnds(aend2, aend1);				
		
		EdmNavigationProperty.Builder navigation = EdmNavigationProperty
				.newBuilder("g1").setFromTo(aend2, aend1).setFromToName("source", "target").setRelationship(assocition);
		g2Entity.addNavigationProperties(navigation);
		
		processor.addEntitySetAsTable(mf, "G1", g1Entity.build());
		processor.addEntitySetAsTable(mf, "G2", g2Entity.build());
		processor.addNavigationRelations(mf, "G2", g2Entity.build());
		
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");

		ForeignKey fk = g1.getForeignKeys().get(0);
		assertEquals("one_2_one", fk.getName());
		assertNotNull(fk.getColumnByName("e1"));
	}
	
	@Test
	public void testAssosiationWithReferentialContriant() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		EdmEntityType.Builder g1Entity = entityType("g1");
		g1Entity.addProperties(EdmProperty.newBuilder("g2e2").setType(EdmSimpleType.STRING).setNullable(false));
		EdmEntityType.Builder g2Entity = entityType("g2");
		
		EdmAssociationEnd.Builder aend1 = EdmAssociationEnd.newBuilder()
				.setRole("source").setType(g1Entity)
				.setMultiplicity(EdmMultiplicity.ONE);
		
		EdmAssociationEnd.Builder aend2 = EdmAssociationEnd.newBuilder()
				.setRole("target").setType(g2Entity)
				.setMultiplicity(EdmMultiplicity.ONE);		
		
		EdmReferentialConstraint.Builder refContraint = EdmReferentialConstraint
				.newBuilder().addPrincipalReferences("e1").setPrincipalRole("source")
				.addDependentReferences("g2e2").setDependentRole("target");

		EdmAssociation.Builder assocition = EdmAssociation.newBuilder()
				.setNamespace("namspace").setName("one_2_one")
				.setEnds(aend2, aend1).setRefConstraint(refContraint);				

		EdmNavigationProperty.Builder navigation = EdmNavigationProperty
				.newBuilder("g1").setFromTo(aend2, aend1).setFromToName("source", "target").setRelationship(assocition);
		
		g2Entity.addNavigationProperties(navigation);
		
		processor.addEntitySetAsTable(mf, "G1", g1Entity.build());
		processor.addEntitySetAsTable(mf, "G2", g2Entity.build());
		processor.addNavigationRelations(mf, "G2", g2Entity.build());
		
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");

		assertNotNull(g1);
		assertNotNull(g2);
		
		ForeignKey fk = g1.getForeignKeys().get(0);
		assertEquals("one_2_one", fk.getName());
		assertNotNull(fk.getColumnByName("e1"));
		assertEquals("g2e2", fk.getReferenceColumns().get(0));
	}	
	
	
	@Test
	public void testManytoManyAssosiation() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		EdmEntityType.Builder g1Entity = entityType("g1");
		EdmEntityType.Builder g2Entity = entityType("g2");
		
		EdmAssociationEnd.Builder aend1 = EdmAssociationEnd.newBuilder()
				.setRole("source").setType(g1Entity)
				.setMultiplicity(EdmMultiplicity.MANY);
		
		EdmAssociationEnd.Builder aend2 = EdmAssociationEnd.newBuilder()
				.setRole("target").setType(g2Entity)
				.setMultiplicity(EdmMultiplicity.MANY);		
		
		EdmAssociation.Builder assocition = EdmAssociation.newBuilder()
				.setNamespace("namspace").setName("m_2_m")
				.setEnds(aend2, aend1);				
		
		EdmNavigationProperty.Builder navigation = EdmNavigationProperty
				.newBuilder("g1").setFromTo(aend2, aend1).setFromToName("source", "target").setRelationship(assocition);
		g2Entity.addNavigationProperties(navigation);
		
		processor.addEntitySetAsTable(mf, "G1", g1Entity.build());
		processor.addEntitySetAsTable(mf, "G2", g2Entity.build());
		processor.addNavigationRelations(mf, "G2", g2Entity.build());
		
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");
		Table linkTable = mf.getSchema().getTable("m_2_m");
		assertEquals(2, linkTable.getColumns().size());
		assertEquals("G2_e1", linkTable.getColumns().get(0).getName());
		assertEquals("G1_e1", linkTable.getColumns().get(1).getName());
		
		assertNotNull(linkTable);
		assertEquals("G2,G1", linkTable.getProperty(ODataMetadataProcessor.LINK_TABLES, false));

		ForeignKey fk = g1.getForeignKeys().get(0);
		assertEquals("m_2_m", fk.getName());
		assertNotNull(fk.getColumnByName("e1"));
	}	
	
	@Test
	public void testManytoManyAssosiationWithReferntialConstraint() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		EdmEntityType.Builder g1Entity = entityType("g1");
		g1Entity.addProperties(EdmProperty.newBuilder("g2e2").setType(EdmSimpleType.STRING).setNullable(false));
		
		EdmEntityType.Builder g2Entity = entityType("g2");
		
		EdmAssociationEnd.Builder aend1 = EdmAssociationEnd.newBuilder()
				.setRole("source").setType(g1Entity)
				.setMultiplicity(EdmMultiplicity.MANY);
		
		EdmAssociationEnd.Builder aend2 = EdmAssociationEnd.newBuilder()
				.setRole("target").setType(g2Entity)
				.setMultiplicity(EdmMultiplicity.MANY);		
		
		EdmReferentialConstraint.Builder refContraint = EdmReferentialConstraint
				.newBuilder().addPrincipalReferences("e1")
				.addDependentReferences("g2e2");
		
		EdmAssociation.Builder assocition = EdmAssociation.newBuilder()
				.setNamespace("namspace").setName("m_2_m")
				.setEnds(aend2, aend1).setRefConstraint(refContraint);		
		
		EdmNavigationProperty.Builder navigation = EdmNavigationProperty
				.newBuilder("g1").setFromTo(aend2, aend1).setFromToName("source", "target").setRelationship(assocition);
		g2Entity.addNavigationProperties(navigation);
		
		processor.addEntitySetAsTable(mf, "G1", g1Entity.build());
		processor.addEntitySetAsTable(mf, "G2", g2Entity.build());
		processor.addNavigationRelations(mf, "G2", g2Entity.build());
		
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");
		Table linkTable = mf.getSchema().getTable("m_2_m");
		assertEquals(2, linkTable.getColumns().size());
		assertEquals("G2_e1", linkTable.getColumns().get(0).getName());
		assertEquals("G1_g2e2", linkTable.getColumns().get(1).getName());
		
		assertNotNull(linkTable);
		assertEquals("G2,G1", linkTable.getProperty(ODataMetadataProcessor.LINK_TABLES, false));

		ForeignKey fk = g1.getForeignKeys().get(0);
		assertEquals("m_2_m", fk.getName());
		assertNotNull(fk.getColumnByName("g2e2"));

		ForeignKey fk2 = g2.getForeignKeys().get(0);
		assertEquals("m_2_m", fk2.getName());
		assertNotNull(fk2.getColumnByName("e1"));
	}	
	
	private EdmEntityType.Builder entityType(String name) {
		ArrayList<EdmProperty.Builder> props = new ArrayList<EdmProperty.Builder>();
		props.add(EdmProperty.newBuilder("e1").setType(EdmSimpleType.INT32));
		props.add(EdmProperty.newBuilder("e2").setType(EdmSimpleType.STRING).setNullable(false));
		return EdmEntityType.newBuilder().addProperties(props).addKeys("e1").setName(name).setNamespace("namespace");
	}
	
	private EdmEntityType.Builder buildBusinessEntity(EdmComplexType address) {
		ArrayList<EdmProperty.Builder> businessProperties = new ArrayList<EdmProperty.Builder>();
		businessProperties.add(EdmProperty.newBuilder("name").setType(EdmSimpleType.STRING).setMaxLength(25));
		businessProperties.add(EdmProperty.newBuilder("address").setType(address).setNullable(true));
		
		return EdmEntityType.newBuilder().addProperties(businessProperties).addKeys("name");
	}

	private EdmEntityType.Builder buildPersonEntity(EdmComplexType address) {
		ArrayList<EdmProperty.Builder> personProperties = new ArrayList<EdmProperty.Builder>();
		personProperties.add(EdmProperty.newBuilder("name").setType(EdmSimpleType.STRING).setMaxLength(25));
		personProperties.add(EdmProperty.newBuilder("ssn").setType(EdmSimpleType.INT64).setNullable(false));
		personProperties.add(EdmProperty.newBuilder("address").setType(address).setNullable(true));
		
		return EdmEntityType.newBuilder().addProperties(personProperties).addKeys("ssn");
	}
	
	private EdmComplexType.Builder buildAddressEntity() {
		ArrayList<EdmProperty.Builder> addressProperties = new ArrayList<EdmProperty.Builder>();
		addressProperties.add(EdmProperty.newBuilder("street").setType(EdmSimpleType.STRING));
		addressProperties.add(EdmProperty.newBuilder("city").setType(EdmSimpleType.STRING));
		addressProperties.add(EdmProperty.newBuilder("state").setType(EdmSimpleType.STRING));
		return EdmComplexType.newBuilder().setName("Address")
				.setNamespace("namespace").addProperties(addressProperties);
	}	
}
