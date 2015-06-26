package org.teiid.translator.infinispan.dsl.metadata;

import static org.junit.Assert.*;

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

		InfinispanConnection conn = PersonCacheSource.createConnection();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected =  "CREATE FOREIGN TABLE Person (\n"
				+ "\tPersonObject object OPTIONS (NAMEINSOURCE 'this', UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Object'),\n"
				+ "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\temail string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', UPDATABLE TRUE);\n"
				+ "\n"
				+ "CREATE FOREIGN TABLE PhoneNumber (\n"
				+ "\tnumber string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\ttype object OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneType'),\n"
				+ "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tCONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', UPDATABLE TRUE);"
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
				+ "\tAllTypesObject object OPTIONS (NAMEINSOURCE 'this', UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Object'),\n"
				+ "\tintKey integer OPTIONS (NAMEINSOURCE 'intKey', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.Integer'),\n"
				+ "\tstringNum string OPTIONS (NAMEINSOURCE 'stringNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tstringKey string OPTIONS (NAMEINSOURCE 'stringKey', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tfloatNum float OPTIONS (NAMEINSOURCE 'floatNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Float'),\n"
				+ "\tbigIntegerValue biginteger OPTIONS (NAMEINSOURCE 'bigIntegerValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.math.BigInteger'),\n"
				+ "\tshortValue short OPTIONS (NAMEINSOURCE 'shortValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Short'),\n"
				+ "\tdoubleNum double OPTIONS (NAMEINSOURCE 'doubleNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Double'),\n"
				+ "\tobjectValue object OPTIONS (NAMEINSOURCE 'objectValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Object'),\n"
				+ "\tintNum integer OPTIONS (NAMEINSOURCE 'intNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Integer'),\n"
				+ "\tbigDecimalValue bigdecimal OPTIONS (NAMEINSOURCE 'bigDecimalValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.math.BigDecimal'),\n"
				+ "\tlongNum long OPTIONS (NAMEINSOURCE 'longNum', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Long'),\n"
				+ "\tbooleanValue boolean OPTIONS (NAMEINSOURCE 'booleanValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Boolean'),\n"
				+ "\ttimeStampValue timestamp OPTIONS (NAMEINSOURCE 'timeStampValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.sql.Timestamp'),\n"
				+ "\ttimeValue time OPTIONS (NAMEINSOURCE 'timeValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.sql.Time'),\n"
				+ "\tdateValue date OPTIONS (NAMEINSOURCE 'dateValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.sql.Date'),\n"
				+ "\tcharValue char OPTIONS (NAMEINSOURCE 'charValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Character'),\n"
				+ "\tbyteArrayValue varbinary OPTIONS (NAMEINSOURCE 'byteArrayValue', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'byte[]'),\n"
				+ "\tCONSTRAINT PK_INTKEY PRIMARY KEY(intKey)\n"
				+ ") OPTIONS (NAMEINSOURCE 'AllTypesCache', UPDATABLE TRUE);"
				;

		
		assertEquals(expected, metadataDDL );	

	}	

}
