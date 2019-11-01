/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.odata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;
import org.odata4j.edm.EdmAssociation;
import org.odata4j.edm.EdmAssociationEnd;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmMultiplicity;
import org.odata4j.edm.EdmNavigationProperty;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmReferentialConstraint;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.KeyRecord.Type;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings({"nls", "unused"})
public class TestODataMetadataProcessor {
    private ODataExecutionFactory translator;

    @Test
    public void testSchema() throws Exception {
        translator = new ODataExecutionFactory();
        translator.start();

        String csdl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.xml"));
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        Properties props = new Properties();
        props.setProperty("schemaNamespace", "ODataWeb.Northwind.Model");
        props.setProperty("entityContainer", "NorthwindEntities");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        processor.getMetadata(mf, new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(new ByteArrayInputStream(csdl.getBytes())))));

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "northwind", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);

        MetadataFactory mf2 = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        QueryParser.getQueryParser().parseDDL(mf2, ddl);

        Procedure p = mf.getSchema().getProcedure("executeVoid");
        assertNotNull(p);
        assertEquals(1, p.getParameters().size());
    }

    @Test
    public void testEnititySet() throws Exception {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);

        ArrayList<EdmProperty.Builder> props = new ArrayList<EdmProperty.Builder>();
        props.add(EdmProperty.newBuilder("name").setType(EdmSimpleType.STRING).setMaxLength(25));
        props.add(EdmProperty.newBuilder("dob").setType(EdmSimpleType.DATETIME).setNullable(true).setPrecision(9));
        props.add(EdmProperty.newBuilder("ssn").setType(EdmSimpleType.INT64).setNullable(false).setPrecision(2).setScale(3));

        EdmEntityType.Builder entity = EdmEntityType.newBuilder().addProperties(props).addKeys("ssn");
        EdmEntitySet es = EdmEntitySet.newBuilder().setName("Person").setEntityType(entity).build();
        processor.addEntitySetAsTable(mf, es);

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
        assertEquals(2, ssn.getPrecision());
        assertEquals(3, ssn.getScale());

        assertEquals(9, dob.getScale());

        assertEquals(1, person.getPrimaryKey().getColumns().size());
        assertEquals("ssn", person.getPrimaryKey().getColumns().get(0).getName());
        assertTrue(person.getForeignKeys().isEmpty());
    }

    @Test
    public void testEnititySetWithComplexType() throws Exception {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);

        EdmEntitySet es = EdmEntitySet.newBuilder().setName("Person").setEntityType(buildPersonEntity(buildAddressEntity().build())).build();
        processor.addEntitySetAsTable(mf, es);

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

        EdmEntitySet es = EdmEntitySet.newBuilder().setName("Person").setEntityType(buildPersonEntity(buildAddressEntity().build())).build();
        processor.addEntitySetAsTable(mf, es);

        es = EdmEntitySet.newBuilder().setName("Business").setEntityType(buildBusinessEntity(buildAddressEntity().build())).build();
        processor.addEntitySetAsTable(mf, es);

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

        EdmEntitySet g1Set = EdmEntitySet.newBuilder().setName("G1").setEntityType(g1Entity).build();
        processor.addEntitySetAsTable(mf, g1Set);

        EdmEntitySet g2Set = EdmEntitySet.newBuilder().setName("G2").setEntityType(g2Entity).build();
        processor.addEntitySetAsTable(mf, g2Set);

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
                .newBuilder().addPrincipalReferences("g2e2").setPrincipalRole("source")
                .addDependentReferences("e1").setDependentRole("target");

        EdmAssociation.Builder assocition = EdmAssociation.newBuilder()
                .setNamespace("namspace").setName("one_2_one")
                .setEnds(aend2, aend1).setRefConstraint(refContraint);

        EdmNavigationProperty.Builder navigation = EdmNavigationProperty
                .newBuilder("g1").setFromTo(aend2, aend1).setFromToName("source","target").setRelationship(assocition);

        g2Entity.addNavigationProperties(navigation);

        EdmEntitySet g1Set = EdmEntitySet.newBuilder().setName("G1").setEntityType(g1Entity).build();
        EdmEntitySet g2Set = EdmEntitySet.newBuilder().setName("G2").setEntityType(g2Entity).build();

        Table t1 = processor.addEntitySetAsTable(mf, g1Set);
        Table t2 = processor.addEntitySetAsTable(mf, g2Set);

        KeyRecord record = new KeyRecord(Type.Unique);
        record.addColumn(t1.getColumnByName("g2e2"));
        t1.setUniqueKeys(Arrays.asList(record));

        processor.addNavigationRelations(mf, "G2", g2Entity.build());

        Table g1 = mf.getSchema().getTable("G1");
        Table g2 = mf.getSchema().getTable("G2");

        assertNotNull(g1);
        assertNotNull(g2);

        ForeignKey fk = g1.getForeignKeys().get(0);
        assertEquals("one_2_one", fk.getName());
        assertNotNull(fk.getColumnByName("g2e2"));
        assertEquals("e1", fk.getReferenceColumns().get(0));
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

        EdmEntitySet g1Set = EdmEntitySet.newBuilder().setName("G1").setEntityType(g1Entity).build();
        EdmEntitySet g2Set = EdmEntitySet.newBuilder().setName("G2").setEntityType(g2Entity).build();

        processor.addEntitySetAsTable(mf, g1Set);
        processor.addEntitySetAsTable(mf, g2Set);
        processor.addNavigationRelations(mf, "G2", g2Entity.build());

        Table g1 = mf.getSchema().getTable("G1");
        Table g2 = mf.getSchema().getTable("G2");
        Table linkTable = mf.getSchema().getTable("m_2_m");
        assertEquals(1, linkTable.getColumns().size());
        assertEquals("e1", linkTable.getColumns().get(0).getName());

        assertNotNull(linkTable);
        assertEquals("G2,G1", linkTable.getProperty(ODataMetadataProcessor.LINK_TABLES, false));

        ForeignKey fk1 = linkTable.getForeignKeys().get(0);
        assertEquals("G2_FK", fk1.getName());
        assertNotNull(fk1.getColumnByName("e1"));

        ForeignKey fk2 = linkTable.getForeignKeys().get(1);
        assertEquals("G1_FK", fk2.getName());
        assertNotNull(fk2.getColumnByName("e1"));
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

        EdmEntitySet g1Set = EdmEntitySet.newBuilder().setName("G1").setEntityType(g1Entity).build();
        EdmEntitySet g2Set = EdmEntitySet.newBuilder().setName("G2").setEntityType(g2Entity).build();

        processor.addEntitySetAsTable(mf, g1Set);
        processor.addEntitySetAsTable(mf, g2Set);
        processor.addNavigationRelations(mf, "G2", g2Entity.build());

        Table g1 = mf.getSchema().getTable("G1");
        Table g2 = mf.getSchema().getTable("G2");
        Table linkTable = mf.getSchema().getTable("m_2_m");
        assertEquals(2, linkTable.getColumns().size());
        assertEquals("e1", linkTable.getColumns().get(0).getName());
        assertEquals("g2e2", linkTable.getColumns().get(1).getName());

        assertNotNull(linkTable);
        assertEquals("G2,G1", linkTable.getProperty(ODataMetadataProcessor.LINK_TABLES, false));

        ForeignKey fk = linkTable.getForeignKeys().get(0);
        assertEquals("G2_FK", fk.getName());
        assertNotNull(fk.getColumnByName("e1"));

        ForeignKey fk2 = linkTable.getForeignKeys().get(1);
        assertEquals("G1_FK", fk2.getName());
        assertNotNull(fk2.getColumnByName("g2e2"));
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

    @Test
    public void testKeyMaytches() {
        Column c1 = new Column();
        c1.setName("one");

        Column c2 = new Column();
        c2.setName("two");

        KeyRecord record = new KeyRecord(Type.Primary);
        record.addColumn(c1);
        record.addColumn(c2);

        ODataMetadataProcessor p = new ODataMetadataProcessor();
        assertTrue(p.keyMatches(Arrays.asList("one", "one"), record));
        assertTrue(p.keyMatches(Arrays.asList("two", "one"), record));
    }
}
