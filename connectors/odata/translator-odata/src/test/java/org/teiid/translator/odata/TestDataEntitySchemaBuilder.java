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

import static org.junit.Assert.*;

import java.io.FileReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.core4j.Enumerable;
import org.junit.Test;
import org.odata4j.edm.*;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.format.xml.EdmxFormatWriter;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

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

    @Test
    public void testArrayIterateMetadata() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");
        StringWriter sw = new StringWriter();
        EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
        EdmxFormatWriter.write(eds, sw);

        //System.out.println(sw.toString());
    }

    @Test
    public void testMetadataWithSelfJoin() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("categories.ddl")), "northwind", "nw");
        StringWriter sw = new StringWriter();
        EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
        EdmxFormatWriter.write(eds, sw);

        String expectedXML = "<?xml version=\"1.0\" encoding=\"utf-8\"?><edmx:Edmx Version=\"1.0\" xmlns:edmx=\"http://schemas.microsoft.com/ado/2007/06/edmx\"><edmx:DataServices m:DataServiceVersion=\"2.0\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><Schema Namespace=\"nw\" xmlns=\"http://schemas.microsoft.com/ado/2008/09/edm\"><EntityType Name=\"Category\"><Key><PropertyRef Name=\"CategoryID\"></PropertyRef></Key><Property Name=\"CategoryID\" Type=\"Edm.Int32\" Nullable=\"false\"></Property><Property Name=\"Name\" Type=\"Edm.String\" Nullable=\"false\" MaxLength=\"25\" FixedLength=\"false\" Unicode=\"true\"></Property><Property Name=\"ParentCategoryID\" Type=\"Edm.Int32\" Nullable=\"false\"></Property><NavigationProperty Name=\"Category\" Relationship=\"nw.Category_FK_CATEGORY_ID\" FromRole=\"Category\" ToRole=\"Category\"></NavigationProperty></EntityType><Association Name=\"Category_FK_CATEGORY_ID\"><End Type=\"nw.Category\" Multiplicity=\"*\" Role=\"Category\"></End><End Type=\"nw.Category\" Multiplicity=\"0..1\" Role=\"Category\"></End><ReferentialConstraint><Principal Role=\"Category\"><PropertyRef Name=\"CategoryID\"></PropertyRef></Principal><Dependent Role=\"Category\"><PropertyRef Name=\"ParentCategoryID\"></PropertyRef></Dependent></ReferentialConstraint></Association><EntityContainer Name=\"nw\" m:IsDefaultEntityContainer=\"false\"><EntitySet Name=\"Category\" EntityType=\"nw.Category\"></EntitySet><AssociationSet Name=\"Category_FK_CATEGORY_ID\" Association=\"nw.Category_FK_CATEGORY_ID\"><End EntitySet=\"Category\" Role=\"Category\"></End><End EntitySet=\"Category\" Role=\"Category\"></End></AssociationSet></EntityContainer></Schema></edmx:DataServices></edmx:Edmx>\n";
        EdmDataServices pds = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new StringReader(expectedXML)));

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

    @Test
    public void testEdmPropertyPrimaryKey() throws Exception{
        String ddl = "CREATE FOREIGN TABLE Sales (\n" +
                "	id integer PRIMARY KEY OPTIONS(NAMEINSOURCE 'myid'),\n" +
                "   name string(5));";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "sales", "sl");
        EdmDataServices edm = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
        assertFalse("Primary key is nullable.", ((EdmProperty)edm.findEdmProperty("id")).isNullable());
        assertTrue(((EdmProperty)edm.findEdmProperty("name")).isNullable());
    }

    @Test
    public void testEntityTypeName() throws Exception{
        String ddl = "CREATE FOREIGN TABLE BookingCollection (\n" +
                "	carrid bigdecimal NOT NULL,\n" +
                "	connid string(5) NOT NULL,\n" +
                "	bookid bigdecimal NOT NULL,\n" +
                "	LOCCURKEY string(5) NOT NULL,\n" +
                "	fldate timestamp NOT NULL,\n" +
                "	PRIMARY KEY(carrid, connid, fldate, bookid)\n" +
                ") OPTIONS (UPDATABLE TRUE, " +
                " \"teiid_odata:EntityType\" 'RMTSAMPLEFLIGHT.Booking');";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");
        EdmDataServices edm = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
        assertTrue(edm.findEdmEntitySet("nw.BookingCollection")!=null);
        assertTrue(edm.findEdmEntityType("nw.RMTSAMPLEFLIGHT.Booking")!=null);
    }

    @Test
    public void testEntityPropertyName() throws Exception{
        String ddl = "CREATE FOREIGN TABLE BookingCollection (\n" +
                "   carrid bigdecimal NOT NULL OPTIONS(NAMEINSOURCE '\"carrageid\"'),\n" +
                "   connid string(5) NOT NULL  OPTIONS(NAMEINSOURCE '\"connectionid\"'),\n" +
                "   PRIMARY KEY(carrid)\n" +
                ") OPTIONS (UPDATABLE TRUE, " +
                " \"teiid_odata:EntityType\" 'RMTSAMPLEFLIGHT.Booking');";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");
        EdmDataServices edm = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
        assertTrue(edm.findEdmEntitySet("nw.BookingCollection")!=null);
        Enumerable<EdmProperty> properties = edm.getEdmEntitySet("nw.BookingCollection").getType().getProperties();
        assertEquals(2, properties.count());
        Iterator<EdmProperty> it = properties.iterator();
        while(it.hasNext()) {
            EdmProperty property = it.next();
            if (!property.getName().equals("carrid") && !property.getName().equals("connid")) {
                fail();
            }
        }
    }

    @Test
    public void testEntityTypeName2() throws Exception{
        TransformationMetadata metadata = getNorthwindMetadataFromODataXML();

        EdmDataServices edm = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore().getSchema("nw"));
        assertTrue(edm.findEdmEntitySet("nw.Categories")!=null);
        assertEquals("NorthwindModel.Category", edm.findEdmEntitySet("nw.Categories").getType().getName());
        assertTrue(edm.findEdmEntityType("nw.NorthwindModel.Category")!=null);
        assertTrue(edm.findEdmFunctionImport("nw.TopCustomers")!=null);
        assertEquals("Collection(nw.NorthwindModel.Customer)", edm.findEdmFunctionImport("nw.TopCustomers").getReturnType().getFullyQualifiedTypeName());
        assertTrue(edm.findEdmEntityType("nw.NorthwindModel.Category")!=null);

        EdmComplexType complexType = edm.findEdmComplexType("nw.NorthwindModel.Address");
        assertEquals("Address", complexType.getProperties().first().getName());

        edm = new TeiidEdmMetadata("nw", edm);
        assertTrue(edm.findEdmEntitySet("Categories")!=null);
        assertEquals("NorthwindModel.Category", edm.findEdmEntitySet("Categories").getType().getName());
        assertTrue(edm.findEdmEntityType("NorthwindModel.Category")!=null);
        assertTrue(edm.findEdmFunctionImport("TopCustomers")!=null);
        assertEquals("Collection(nw.NorthwindModel.Customer)", edm.findEdmFunctionImport("TopCustomers").getReturnType().getFullyQualifiedTypeName());
        assertTrue(edm.findEdmEntityType("NorthwindModel.Category")!=null);
    }

    static TransformationMetadata getNorthwindMetadataFromODataXML() throws Exception {
        ModelMetaData model = new ModelMetaData();
        model.setName("nw");
        model.setModelType(Type.PHYSICAL);
        MetadataFactory mf = new MetadataFactory("northwind", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), model);

        EdmDataServices edm = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new FileReader(UnitTestUtil.getTestDataFile("northwind.xml"))));
        ODataMetadataProcessor metadataProcessor = new ODataMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, mf.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.getMetadata(mf, edm);

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        return metadata;
    }

    @Test
    public void testArrayType() throws Exception {
        ModelMetaData model = new ModelMetaData();
        model.setName("nw");
        model.setModelType(Type.PHYSICAL);
        MetadataFactory mf = new MetadataFactory("northwind", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), model);

        EdmDataServices edm = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new FileReader(UnitTestUtil.getTestDataFile("arraytest.xml"))));
        ODataMetadataProcessor metadataProcessor = new ODataMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, mf.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.getMetadata(mf, edm);

        Column c = mf.getSchema().getTable("G2").getColumnByName("e3");
        assertEquals("integer[]", c.getRuntimeType());

        Procedure p = mf.getSchema().getProcedure("ARRAYITERATE");
        assertEquals("varbinary[]", p.getParameters().get(0).getRuntimeType());
        assertEquals("varbinary",  p.getResultSet().getColumns().get(0).getRuntimeType());


        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);
    }
}
