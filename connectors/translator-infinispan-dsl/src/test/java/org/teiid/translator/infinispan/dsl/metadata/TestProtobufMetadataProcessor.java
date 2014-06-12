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
		String expected =  "SET NAMESPACE 'http://www.teiid.org/translator/infinispan/2014' AS teiid_infinispan;\n"
				+ "\n"
				+ "CREATE FOREIGN TABLE Person (\n"
				+ "\tPersonObject object OPTIONS (NAMEINSOURCE 'this', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Object'),\n"
				+ "\tname string OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tid integer OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.Integer'),\n"
				+ "\temail string OPTIONS (SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', \"teiid_infinispan:entity_class\" 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person');\n"
				+ "\n"
				+ "CREATE FOREIGN TABLE PhoneNumber (\n"
				+ "\tnumber string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\ttype string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tId integer NOT NULL OPTIONS (SELECTABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'int'),\n"
				+ "\tCONSTRAINT FK_PERSON FOREIGN KEY(Id) REFERENCES Person (Id) OPTIONS (NAMEINSOURCE 'phones')\n"
				+ ") OPTIONS (NAMEINSOURCE 'PersonsCache', \"teiid_infinispan:entity_class\" 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneNumber');"
				;
		
		//, \"entity_class\" 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person'
		// , \"entity_class\" 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneNumber'
		
		assertEquals(metadataDDL, expected);	

	}

}
