package org.teiid.translator.infinispan.dsl.metadata;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;
import org.teiid.translator.infinispan.dsl.InfinispanExecutionFactory;

@SuppressWarnings("nls")
public class TestProtobufMetadataProcessor {
	protected static InfinispanExecutionFactory TRANSLATOR;

	@BeforeClass
	public static void setUp() throws TranslatorException {
		TRANSLATOR = new InfinispanExecutionFactory();
		TRANSLATOR.start();
	}

	@Test
	public void testMetadata() throws Exception {
		ProtobufMetadataProcessor mp = new ProtobufMetadataProcessor();

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanConnection conn = PersonCacheSource.createConnection();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected = "CREATE FOREIGN TABLE child (\n"
				+ "\tcol1 string,\n"
				+ "\tcol2 string\n"
				+ ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table');\n"
				+ "\n" + "CREATE FOREIGN TABLE \"table\" (\n"
				+ "\t\"_id\" integer,\n" + "\tcol2 double,\n"
				+ "\tcol3 long,\n" + "\tcol5 boolean,\n" + "\tcol6 string,\n"
				+ "\tcol7 string[] OPTIONS (SEARCHABLE 'Unsearchable'),\n"
				+ "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n"
				+ "\tCONSTRAINT FK_col6 FOREIGN KEY(col6) REFERENCES ns \n"
				+ ") OPTIONS (UPDATABLE TRUE);";
		
/*
  -- these are the nested field descriptors
  
 Schema: CREATE FOREIGN TABLE Descriptor (
	number string OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	type string OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String')
) OPTIONS (entity_class 'com.google.protobuf.Descriptors$Descriptor');


CREATE FOREIGN TABLE Person (
	PersonObject object OPTIONS (NAMEINSOURCE 'this', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person'),
	name string OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	id integer OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.Integer'),
	email string OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),
	phone string OPTIONS (NAMEINSOURCE 'nis', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String')
) OPTIONS (NAMEINSOURCE 'Persons', entity_class 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person');
  
  
 */

	}

}
