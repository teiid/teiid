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
package org.teiid.translator.odata4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.olingo.client.api.edm.xml.DataServices;
import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.client.core.edm.ClientCsdlXMLMetadata;
import org.apache.olingo.client.core.edm.xml.ClientCsdlEdmx;
import org.apache.olingo.client.core.serialization.ClientODataDeserializerImpl;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmReferentialConstraint;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAction;
import org.apache.olingo.commons.api.edm.provider.CsdlActionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlFunctionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlParameter;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlReferentialConstraint;
import org.apache.olingo.commons.api.edm.provider.CsdlReturnType;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.format.ContentType;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.ProcedureParameter;
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
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.odata4.ODataExecutionFactory;
import org.teiid.translator.odata4.ODataMetadataProcessor;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;

@SuppressWarnings({"nls", "unused"})
public class TestODataMetadataProcessor {
	private ODataExecutionFactory translator;

    public static TransformationMetadata getTransformationMetadata(MetadataFactory mf, ODataExecutionFactory ef) throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "trippin", new FunctionTree("foo", new UDFSource(ef.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        return metadata;
    }
	
    static MetadataFactory tripPinMetadata() throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor() {
            protected XMLMetadata getSchema(WSConnection conn) throws TranslatorException {
                try {
                    ClientODataDeserializerImpl deserializer = new ClientODataDeserializerImpl(
                            false, ContentType.APPLICATION_XML);
                    XMLMetadata metadata = deserializer.toMetadata(
                            new FileInputStream(UnitTestUtil.getTestDataFile("trippin.xml")));
                    return metadata;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                return null;
            }           
        };
        Properties props = new Properties();
        props.setProperty("schemaNamespace", "Microsoft.OData.SampleService.Models.TripPin");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "trippin", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        processor.process(mf, null);
//        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
//        System.out.println(ddl);    
        
        return mf;
    }
    
	@Test
	public void testSchema() throws Exception {
    	translator = new ODataExecutionFactory();
    	translator.start();
		
    	MetadataFactory mf = tripPinMetadata();
    	TransformationMetadata metadata = getTransformationMetadata(mf, this.translator);
    	
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);    
		
		MetadataFactory mf2 = new MetadataFactory(null, 1, "northwind", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null); 
		QueryParser.getQueryParser().parseDDL(mf2, ddl);	
		
		Procedure p = mf.getSchema().getProcedure("ResetDataSource");
		assertNotNull(p);
		assertEquals(0, p.getParameters().size());
	}
	
	static CsdlProperty createProperty(String name, EdmPrimitiveTypeKind type) {	    
	    return new CsdlProperty().setName(name).setType(type.getFullQualifiedName());
	}
	
    static CsdlParameter createParameter(String name, EdmPrimitiveTypeKind type) {        
        return new CsdlParameter().setName(name).setType(type.getFullQualifiedName());
    }
    
	static XMLMetadata buildXmlMetadata(Object... schemaElements) {
        final CsdlSchema schema = new CsdlSchema();
        schema.setNamespace("namespace");
        schema.setEntityContainer(new CsdlEntityContainer());
        ArrayList<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
        ArrayList<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();
        ArrayList<CsdlComplexType> complexTypes = new ArrayList<CsdlComplexType>();
        
        ArrayList<CsdlFunctionImport> functionImports = new ArrayList<CsdlFunctionImport>();
        ArrayList<CsdlFunction> functions = new ArrayList<CsdlFunction>();
        ArrayList<CsdlActionImport> actionImports = new ArrayList<CsdlActionImport>();
        ArrayList<CsdlAction> actions = new ArrayList<CsdlAction>();
        
        for (Object obj:schemaElements) {
            if (obj instanceof CsdlEntitySet) {
                entitySets.add((CsdlEntitySet)obj);
            }
            else if (obj instanceof CsdlEntityType) {
                entityTypes.add((CsdlEntityType)obj);
            }
            else if (obj instanceof CsdlComplexType) {
                complexTypes.add((CsdlComplexType)obj);
            } else if (obj instanceof CsdlFunctionImport) {
                functionImports.add((CsdlFunctionImport)obj);
            } else if (obj instanceof CsdlFunction) {
                functions.add((CsdlFunction)obj);
            } else if (obj instanceof CsdlActionImport) {
                actionImports.add((CsdlActionImport)obj);
            } else if (obj instanceof CsdlAction) {
                actions.add((CsdlAction)obj);
            }
        }
        
        schema.setEntityTypes(entityTypes);
        schema.setComplexTypes(complexTypes);
        schema.setActions(actions);
        schema.setFunctions(functions);
        schema.getEntityContainer().setEntitySets(entitySets);
        schema.getEntityContainer().setFunctionImports(functionImports);
        schema.getEntityContainer().setActionImports(actionImports);
        
	    ClientCsdlEdmx edmx = new ClientCsdlEdmx();
	    edmx.setVersion("1.0");
	    edmx.setDataServices(new DataServices() {
            @Override
            public List<CsdlSchema> getSchemas() {
                return Arrays.asList(schema);
            }
            @Override
            public String getMaxDataServiceVersion() {
                return "4.0";
            }
            @Override
            public String getDataServiceVersion() {
                return "4.0";
            }
        });
	    return new ClientCsdlXMLMetadata(edmx);
	}
	
	static CsdlEntitySet createES(String name, String entityType) {
        CsdlEntitySet es = new CsdlEntitySet();
        es.setName(name);
        es.setType(new FullQualifiedName(entityType));
        return es;
	}
	
	@Test
	public void testEnititySet() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		MetadataFactory mf = new MetadataFactory("vdb", 1, "trippin", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
		properties.add(createProperty("name", EdmPrimitiveTypeKind.String).setMaxLength(25).setNullable(false));
		properties.add(createProperty("dob", EdmPrimitiveTypeKind.DateTimeOffset).setNullable(true));
		properties.add(createProperty("ssn", EdmPrimitiveTypeKind.Int64).setNullable(false));
		
		CsdlEntityType entityType = new CsdlEntityType();
		entityType.setName("Person");
		entityType.setProperties(properties);
		entityType.setKey(Arrays.asList(new CsdlPropertyRef().setName("ssn")));
		
		CsdlEntitySet es = createES("Persons", "namespace.Person");
		
		XMLMetadata metadata = buildXmlMetadata(es, entityType);

		processor.getMetadata(mf, metadata);
		
		assertNotNull(mf.getSchema().getTable("Persons"));
		
		Table person = mf.getSchema().getTable("Persons");
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
		MetadataFactory mf = getEntityWithComplexProperty();
        
		assertEquals(3, mf.getSchema().getTables().size());
		assertNotNull(mf.getSchema().getTable("Persons"));
		assertNotNull(mf.getSchema().getTable("Persons_address"));
		assertNotNull(mf.getSchema().getTable("Persons_secondaddress"));
		
		Table personTable = mf.getSchema().getTable("Persons");
		assertEquals(2, personTable.getColumns().size());
		assertNotNull(personTable.getPrimaryKey());
		
        Table addressTable = mf.getSchema().getTable("Persons_address");
        assertEquals(4, addressTable.getColumns().size());
		
		assertNotNull(addressTable.getColumnByName("ssn"));
		assertNotNull(addressTable.getColumnByName("ssn").getProperty(ODataMetadataProcessor.PSEUDO, false));
		assertTrue(addressTable.getColumnByName("ssn").isSelectable());
        assertEquals(1, addressTable.getForeignKeys().size());
        assertEquals("Persons", addressTable.getForeignKeys().get(0).getReferenceTableName());
	}

    static MetadataFactory getEntityWithComplexProperty()
            throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
		
		CsdlComplexType address = complexType("Address");
        XMLMetadata metadata = buildXmlMetadata(createES("Persons", "namespace.Person"), 
                buildPersonEntity(address), address);
        processor.getMetadata(mf, metadata);
        
        return mf;
    }

	
	@Test
	public void testMultipleEnititySetWithSameComplexType() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);

        CsdlComplexType address = complexType("Address");
        XMLMetadata metadata = buildXmlMetadata(
                createES("Persons", "namespace.Person"),
                buildPersonEntity(address), address,
                createES("Corporate", "namespace.Business"),
                buildBusinessEntity(address));
        processor.getMetadata(mf, metadata);
		
		assertEquals(5, mf.getSchema().getTables().size());
		assertNotNull(mf.getSchema().getTable("Persons"));
		assertNotNull(mf.getSchema().getTable("Corporate"));
		assertNotNull(mf.getSchema().getTable("Persons_address"));
		assertNotNull(mf.getSchema().getTable("Corporate_address"));
		
		Table personTable = mf.getSchema().getTable("Persons");
		assertEquals(2, personTable.getColumns().size());
		
		Table personAddress= mf.getSchema().getTable("Persons_address");
		assertEquals(4, personAddress.getColumns().size());
		ForeignKey fk = personAddress.getForeignKeys().get(0);
		assertNotNull(fk.getColumnByName("ssn"));
		
		Table businessTable = mf.getSchema().getTable("Corporate");
		assertEquals(1, businessTable.getColumns().size());
        Table corporateAddress= mf.getSchema().getTable("Corporate_address");
        assertEquals(4, corporateAddress.getColumns().size());
        fk = corporateAddress.getForeignKeys().get(0);
        assertNotNull(fk.getColumnByName("name"));
		
	}
		
	@Test
	public void testOneToOneAssosiation() throws Exception {
		MetadataFactory mf = oneToOneRelationMetadata();
		
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");

		ForeignKey fk = g2.getForeignKeys().get(0);
		assertEquals("one_2_one", fk.getName());
		assertNotNull(fk.getColumnByName("e1"));
	}

	@Test
	public void testFunction() throws Exception {
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");
        
        MetadataFactory mf = functionMetadata("invoke", returnType, null);
	    Procedure p = mf.getSchema().getProcedure("invoke");
	    assertNotNull(p);
	    assertEquals(3, p.getParameters().size());
	    assertNull(p.getResultSet());
	    assertNotNull(getReturnParameter(p));
	    ProcedureParameter pp = getReturnParameter(p);
	    assertEquals("string", pp.getRuntimeType());
	    ODataType type = ODataType.valueOf(p.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
	    assertEquals(ODataType.FUNCTION, type);
	}
	
    @Test
    public void testAction() throws Exception {
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");
        
        MetadataFactory mf = actionMetadata("invoke", returnType, null);
        Procedure p = mf.getSchema().getProcedure("invoke");
        assertNotNull(p);
        assertEquals(3, p.getParameters().size());
        assertNull(p.getResultSet());
        assertNotNull(getReturnParameter(p));
        ProcedureParameter pp = getReturnParameter(p);
        assertEquals("string", pp.getRuntimeType());
        ODataType type = ODataType.valueOf(p.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.ACTION, type);
    }	

    @Test
    public void testFunctionReturnPrimitiveCollection() throws Exception {
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");
        returnType.setCollection(true);
        
        MetadataFactory mf = functionMetadata("invoke", returnType, null);
        Procedure p = mf.getSchema().getProcedure("invoke");
        assertNotNull(p);
        assertEquals(3, p.getParameters().size());
        assertNull(p.getResultSet());
        assertNotNull(getReturnParameter(p));
        ProcedureParameter pp = getReturnParameter(p);
        assertEquals("string[]", pp.getRuntimeType());        
        ODataType type = ODataType.valueOf(p.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.FUNCTION, type);
    }
    
    @Test
    public void testFunctionReturnComplex() throws Exception {
        CsdlComplexType complex = complexType("Address");
        
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("namespace.Address");
        
        MetadataFactory mf = functionMetadata("invoke", returnType, complex);
        Procedure p = mf.getSchema().getProcedure("invoke");
        assertNotNull(p);
        assertEquals(2, p.getParameters().size());
        assertNotNull(p.getResultSet());
        assertNull(getReturnParameter(p));
        ColumnSet<Procedure> table = p.getResultSet();
        ODataType type = ODataType.valueOf(p.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.FUNCTION, type);
        
        type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.COMPLEX, type);
    }  
    
    @Test
    public void testFunctionReturnComplexCollection() throws Exception {
        CsdlComplexType complex = complexType("Address");
        
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("namespace.Address");
        returnType.setCollection(true);
        
        MetadataFactory mf = functionMetadata("invoke", returnType, complex);
        Procedure p = mf.getSchema().getProcedure("invoke");
        assertNotNull(p);
        assertEquals(2, p.getParameters().size());
        assertNotNull(p.getResultSet());
        assertNull(getReturnParameter(p));
        ColumnSet<Procedure> table = p.getResultSet();
        ODataType type = ODataType.valueOf(p.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.FUNCTION, type);
        
        type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        assertEquals(ODataType.COMPLEX_COLLECTION, type);
    }    
	
    private ProcedureParameter getReturnParameter(Procedure procedure) {
        for (ProcedureParameter pp:procedure.getParameters()) {
            if (pp.getType() == ProcedureParameter.Type.ReturnValue) {
                return pp;
            }
        }
        return null;
    }
    
    static MetadataFactory oneToOneRelationMetadata()
            throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
		
		CsdlEntityType g1Entity = entityType("g1");
		CsdlEntityType g2Entity = entityType("g2");
		
		CsdlNavigationProperty navProperty = new CsdlNavigationProperty();
		navProperty.setName("one_2_one");
		navProperty.setType("namespace.g2");
		navProperty.setNullable(false);
		navProperty.setPartner("PartnerPath");
		
		g1Entity.setNavigationProperties(Arrays.asList(navProperty));
		
		CsdlEntitySet g1Set = createES("G1", "namespace.g1");
		CsdlEntitySet g2Set = createES("G2", "namespace.g2");
		
		CsdlNavigationPropertyBinding navBinding = new CsdlNavigationPropertyBinding();
		navBinding.setPath("one_2_one");
		navBinding.setTarget("G2");
		g1Set.setNavigationPropertyBindings(Arrays.asList(navBinding));
		
		XMLMetadata metadata = buildXmlMetadata(g1Entity, g1Set, g2Entity, g2Set);
		processor.getMetadata(mf, metadata);
        return mf;
    }
    
    static MetadataFactory functionMetadata(String name, CsdlReturnType returnType, Object other)
            throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
                
        CsdlFunction func = function(name, returnType);
        
        CsdlFunctionImport funcImport = new CsdlFunctionImport();
        funcImport.setFunction(new FullQualifiedName("namespace."+name));
        funcImport.setName(name);
        
        XMLMetadata metadata = buildXmlMetadata(funcImport, func, other);
        processor.getMetadata(mf, metadata);
        return mf;
    }
    
    static MetadataFactory actionMetadata(String name, CsdlReturnType returnType, Object other)
            throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
                
        CsdlAction func = action(name, returnType);
        
        CsdlActionImport funcImport = new CsdlActionImport();
        funcImport.setAction(new FullQualifiedName("namespace."+name));
        funcImport.setName(name);
        
        XMLMetadata metadata = buildXmlMetadata(funcImport, func, other);
        processor.getMetadata(mf, metadata);
        
        return mf;
    }    
	
    @Test
    public void testSelfJoin() throws Exception {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
        
        CsdlEntityType g1Entity = entityType("g1");
        
        CsdlNavigationProperty navProperty = new CsdlNavigationProperty();
        navProperty.setName("self");
        navProperty.setType("Collection(namespace.g1)");
        navProperty.setNullable(false);
        navProperty.setPartner("PartnerPath");
        navProperty.setCollection(true);
        
        g1Entity.setNavigationProperties(Arrays.asList(navProperty));
        
        CsdlEntitySet g1Set = createES("G1", "namespace.g1");
        
        CsdlNavigationPropertyBinding navBinding = new CsdlNavigationPropertyBinding();
        navBinding.setPath("self");
        navBinding.setTarget("G1");
        g1Set.setNavigationPropertyBindings(Arrays.asList(navBinding));
        
        XMLMetadata metadata = buildXmlMetadata(g1Entity, g1Set);
        processor.getMetadata(mf, metadata);
                
        Table g1 = mf.getSchema().getTable("G1_self");
        assertNotNull(g1);
        assertEquals("self", g1.getForeignKeys().get(0).getName());
        assertNotNull(g1.getForeignKeys().get(0).getColumnByName("G1_e1"));
        assertEquals("self", g1.getNameInSource());
    }	
	
    @Test
    public void testOneToManyAssosiation() throws Exception {
        MetadataFactory mf = oneToManyRelationMetadata();
        
        Table g1 = mf.getSchema().getTable("G1");
        Table g2 = mf.getSchema().getTable("G2");

        ForeignKey fk = g2.getForeignKeys().get(0);
        assertEquals("one_2_many", fk.getName());
        assertNotNull(fk.getColumnByName("e1"));
    }

    static MetadataFactory oneToManyRelationMetadata() throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
        
        CsdlEntityType g1Entity = entityType("g1");
        CsdlEntityType g2Entity = multipleKeyEntityType("g2");
        
        CsdlNavigationProperty navProperty = new CsdlNavigationProperty();
        navProperty.setName("one_2_many");
        navProperty.setType("Collection(namespace.g2)");
        navProperty.setNullable(false);
        navProperty.setPartner("PartnerPath");
        navProperty.setCollection(true);
        
        g1Entity.setNavigationProperties(Arrays.asList(navProperty));
        
        CsdlEntitySet g1Set = createES("G1", "namespace.g1");
        CsdlEntitySet g2Set = createES("G2", "namespace.g2");
        
        CsdlNavigationPropertyBinding navBinding = new CsdlNavigationPropertyBinding();
        navBinding.setPath("one_2_many");
        navBinding.setTarget("G2");
        g1Set.setNavigationPropertyBindings(Arrays.asList(navBinding));
        
        XMLMetadata metadata = buildXmlMetadata(g1Entity, g1Set, g2Entity, g2Set);
        processor.getMetadata(mf, metadata);
        
        return mf;
    }	
    
    static MetadataFactory multiplePKMetadata() throws TranslatorException {
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
        CsdlEntityType entityType = multipleKeyEntityType("g1");
        CsdlEntitySet entitySet = createES("G1", "namespace.g1");
        XMLMetadata metadata = buildXmlMetadata(entityType, entitySet);
        processor.getMetadata(mf, metadata);
        return mf;
    }

    private static CsdlEntityType multipleKeyEntityType(String name) {
        ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
        properties.add(createProperty("e1", EdmPrimitiveTypeKind.Int32));
        properties.add(createProperty("e2", EdmPrimitiveTypeKind.String).setNullable(false));
        properties.add(createProperty("e3", EdmPrimitiveTypeKind.String).setNullable(false));
        
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(name);
        entityType.setProperties(properties);
        entityType.setKey(Arrays.asList(new CsdlPropertyRef().setName("e1"), new CsdlPropertyRef().setName("e2")));
        return entityType;
    }    

    @Test
    public void testMultikeyPK() throws Exception {
        MetadataFactory mf = multiplePKMetadata();
        Table g1 = mf.getSchema().getTable("G1");

        assertNotNull(g1.getPrimaryKey().getColumnByName("e1"));
        assertNotNull(g1.getPrimaryKey().getColumnByName("e2"));
        assertNull(g1.getPrimaryKey().getColumnByName("e3"));
        
    }    
    
	@Test
	public void testAssosiationWithReferentialContriant() throws Exception {
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "northwind",
                SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
		
        CsdlEntityType g1Entity = entityType("g1");
        g1Entity.getProperties().add(createProperty("g2e2", EdmPrimitiveTypeKind.String));
        g1Entity.setKey(Arrays.asList(new CsdlPropertyRef().setName("g2e2")));
        CsdlEntityType g2Entity = entityType("g2");
        
        CsdlNavigationProperty navProperty = new CsdlNavigationProperty();
        navProperty.setName("one_2_one");
        navProperty.setType("namespace.g2");
        navProperty.setNullable(false);
        navProperty.setPartner("PartnerPath");
        navProperty.setReferentialConstraints(Arrays
                .asList(new CsdlReferentialConstraint().setProperty("g2e2")
                        .setReferencedProperty("e2")));
        
        g1Entity.setNavigationProperties(Arrays.asList(navProperty));
        
        CsdlEntitySet g1Set = createES("G1", "namespace.g1");
        CsdlEntitySet g2Set = createES("G2", "namespace.g2");
        
        CsdlNavigationPropertyBinding navBinding = new CsdlNavigationPropertyBinding();
        navBinding.setPath("one_2_one");
        navBinding.setTarget("G2");
        g1Set.setNavigationPropertyBindings(Arrays.asList(navBinding));
        
        XMLMetadata metadata = buildXmlMetadata(g1Entity, g1Set, g2Entity, g2Set);
        processor.getMetadata(mf, metadata);
		
        
		Table g1 = mf.getSchema().getTable("G1");
		Table g2 = mf.getSchema().getTable("G2");

		assertNotNull(g1);
		assertNotNull(g2);
		
		ForeignKey fk = g2.getForeignKeys().get(0);
		assertEquals("one_2_one", fk.getName());
		assertNotNull(fk.getColumnByName("e2"));
		assertEquals("g2e2", fk.getReferenceColumns().get(0));
	}	
	
	static CsdlEntityType entityType(String name) {
	    ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
        properties.add(createProperty("e1", EdmPrimitiveTypeKind.Int32));
        properties.add(createProperty("e2", EdmPrimitiveTypeKind.String).setNullable(false));
	    
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(name);
        entityType.setProperties(properties);
        entityType.setKey(Arrays.asList(new CsdlPropertyRef().setName("e1")));
        return entityType;        
	}
	
    static CsdlFunction function(String name, CsdlReturnType returnType) {
        ArrayList<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
        parameters.add(createParameter("e1", EdmPrimitiveTypeKind.Int32));
        parameters.add(createParameter("e2", EdmPrimitiveTypeKind.String).setNullable(false));
        
        CsdlFunction function = new CsdlFunction();
        function.setName(name);
        function.setParameters(parameters);
        function.setReturnType(returnType);
        return function;        
    }
    
    static CsdlAction action(String name, CsdlReturnType returnType) {
        ArrayList<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
        parameters.add(createParameter("e1", EdmPrimitiveTypeKind.Int32));
        parameters.add(createParameter("e2", EdmPrimitiveTypeKind.String).setNullable(false));
        
        CsdlAction action = new CsdlAction();
        action.setName(name);
        action.setParameters(parameters);
        action.setReturnType(returnType);
        return action;        
    }    
	
    private CsdlEntityType buildBusinessEntity(CsdlComplexType address) {
        ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
        properties.add(createProperty("name", EdmPrimitiveTypeKind.String).setMaxLength(25));
        properties.add(new CsdlProperty().setName("address").setType(address.getName()));
        
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName("Business");
        entityType.setProperties(properties);
        entityType.setKey(Arrays.asList(new CsdlPropertyRef().setName("name")));
        return entityType;
    }

	static CsdlEntityType buildPersonEntity(CsdlComplexType address) {
        ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
        properties.add(createProperty("name", EdmPrimitiveTypeKind.String).setMaxLength(25));
        properties.add(createProperty("ssn", EdmPrimitiveTypeKind.Int64).setNullable(false));
        properties.add(new CsdlProperty().setName("address").setType(address.getName()));
        properties.add(new CsdlProperty().setName("secondaddress").setType(address.getName()));
        
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName("Person");
        entityType.setProperties(properties);
        entityType.setKey(Arrays.asList(new CsdlPropertyRef().setName("ssn")));
		return entityType;
	}
	
	static CsdlComplexType complexType(String name) {
        ArrayList<CsdlProperty> properties = new ArrayList<CsdlProperty>();
        properties.add(createProperty("street", EdmPrimitiveTypeKind.String));
        properties.add(createProperty("city", EdmPrimitiveTypeKind.String));
        properties.add(createProperty("state", EdmPrimitiveTypeKind.String));

	    CsdlComplexType type = new CsdlComplexType();
	    type.setName(name).setProperties(properties);
	    
	    return type;
	}	
}
