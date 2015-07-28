package org.teiid.translator.infinispan.dsl.metadata;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.Before;
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
	}
	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new InfinispanExecutionFactory();
		TRANSLATOR.start();
    }

	@Test
	public void testPersonMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanConnection conn = PersonCacheSource.createConnection(true);

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected =  "CREATE FOREIGN TABLE Person (\n"
				+ "\tPersonObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person'),\n"
				+ "\tname string OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tid integer NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\temail string OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
				+ ") OPTIONS (UPDATABLE TRUE);\n"
				+ "\n"
				+ "CREATE FOREIGN TABLE PhoneNumber (\n"
				+ "\tnumber string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\ttype string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Enum'),\n"
				+ "\tid integer NOT NULL OPTIONS (SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tCONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')\n"
				+ ") OPTIONS (UPDATABLE TRUE);"
				;
		assertEquals(expected, metadataDDL);	

	}
	
	@Test
	public void testAllTypesMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanConnection conn = AllTypesCacheSource.createConnection();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		
		String expected =  "CREATE FOREIGN TABLE AllTypes (\n"
				+ "\tAllTypesObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.jboss.teiid.jdg_remote.pojo.AllTypes'),\n"
				+ "\tintKey integer NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tstringNum string OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tstringKey string OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tfloatNum float OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'float'),\n"
				+ "\tbigIntegerValue biginteger OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),\n"
				+ "\tshortValue short OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),\n"
				+ "\tdoubleNum double OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'double'),\n"
				+ "\tobjectValue varbinary OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'byte[]'),\n"
				+ "\tintNum integer OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tbigDecimalValue bigdecimal OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'double'),\n"
				+ "\tlongNum long OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),\n"
				+ "\tbooleanValue boolean NOT NULL OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),\n"
				+ "\ttimeStampValue timestamp OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),\n"
				+ "\ttimeValue time OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),\n"
				+ "\tdateValue date OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'long'),\n"
				+ "\tcharValue char OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),\n"
				+ "\tCONSTRAINT PK_INTKEY PRIMARY KEY(intKey)\n"
				+ ") OPTIONS (UPDATABLE TRUE);"
				;
		
		assertEquals(expected, metadataDDL );	

	}	

}
