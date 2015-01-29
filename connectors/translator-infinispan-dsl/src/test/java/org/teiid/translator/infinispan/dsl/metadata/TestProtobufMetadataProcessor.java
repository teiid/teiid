package org.teiid.translator.infinispan.dsl.metadata;

import static org.junit.Assert.assertEquals;

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
				+ "\tid integer NOT NULL OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tname string OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\temail string OPTIONS (SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', UPDATABLE TRUE);\n"
				+ "\n"
				+ "CREATE FOREIGN TABLE PhoneNumber (\n"
				+ "\tnumber string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\ttype string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tid integer NOT NULL OPTIONS (SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\tCONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', UPDATABLE TRUE);"
				;
		
		assertEquals(metadataDDL, expected);	

	}

}
