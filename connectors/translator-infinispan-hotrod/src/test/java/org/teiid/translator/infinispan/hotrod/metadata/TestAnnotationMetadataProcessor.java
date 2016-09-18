package org.teiid.translator.infinispan.hotrod.metadata;

import static org.junit.Assert.*;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodExecutionFactory;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestAnnotationMetadataProcessor {

	protected static ObjectExecutionFactory TRANSLATOR;

	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new InfinispanHotRodExecutionFactory();
	}
	
	
	public void testPersonMetadataNoObject() throws Exception {
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = PersonCacheSource.createConnection(false, Version.getVersion("6.6"));
		
		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();
		
		MetadataProcessor mp = TRANSLATOR.getMetadataProcessor();
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		String expected = "CREATE FOREIGN TABLE Person (\n"
	    + "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
	    + "\temail string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tname string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"	    
		+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
		+ ") OPTIONS (UPDATABLE TRUE);";
		
		assertEquals(expected, metadataDDL);
	}
		 
	
	@Test
	public void testPersonMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = PersonCacheSource.createConnection(false, Version.getVersion("10.2"));

		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		MetadataProcessor mp =  TRANSLATOR.getMetadataProcessor();
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("testAnnotatedMetadata.ddl")), metadataDDL );	


	}

}
