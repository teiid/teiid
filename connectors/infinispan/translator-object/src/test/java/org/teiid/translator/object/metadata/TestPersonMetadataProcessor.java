package org.teiid.translator.object.metadata;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.person.PersonCacheSource;


@SuppressWarnings("nls")
public class TestPersonMetadataProcessor {
	protected static ObjectExecutionFactory TRANSLATOR;

	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new SimpleMapCacheExecutionFactory();
    }

	@Test
	public void testPersonMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = PersonCacheSource.createConnection();

		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		String expected =  "CREATE FOREIGN TABLE Person (\n"
				+ "\tPersonObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.person.Person'),\n"
				+ "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
				+ "\temail string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
				+ ") OPTIONS (UPDATABLE TRUE);"		
				;
		assertEquals(expected, metadataDDL);	

	}
	
	@Test
	public void testPersonMetadataNoKey() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = PersonCacheSource.createConnection(null);

		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadataNoKey.ddl")).trim(), metadataDDL.trim() );	

	}


}
