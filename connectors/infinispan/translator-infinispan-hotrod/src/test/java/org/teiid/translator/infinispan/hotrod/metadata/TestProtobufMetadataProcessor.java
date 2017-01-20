package org.teiid.translator.infinispan.hotrod.metadata;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodExecutionFactory;
import org.teiid.translator.object.ObjectConnection;

@SuppressWarnings("nls")
public class TestProtobufMetadataProcessor {
	protected static InfinispanHotRodExecutionFactory TRANSLATOR;

	@BeforeClass
	public static void setUp()  {
	}
	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new InfinispanHotRodExecutionFactory();
		TRANSLATOR.setSupportsSearchabilityUsingAnnotations(false);
		TRANSLATOR.start();
    }

	@Test
	public void testPersonMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = PersonCacheSource.createConnection(true);

		ProtobufMetadataProcessor mp = (ProtobufMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(false);		
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")).trim(), metadataDDL.trim());	

	}
	
	@Test
	public void testPersonMetadataUpperCaseKeyField() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = PersonCacheSource.createConnection("ID", false, null);
				
		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")).trim(), metadataDDL.trim());	
	}
	
	@Test
	public void testPersonMetadataMixedCaseKeyField() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = PersonCacheSource.createConnection("Id", false, null);
				
		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")).trim(), metadataDDL.trim());	
	}
	
	@Test
	public void testAllTypesMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = AllTypesCacheSource.createConnection();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("allTypesMetadata.ddl")).trim(), metadataDDL.trim() );	

	}	
	
	@Test
	public void testMatPersonMetadata() throws Exception {
		ObjectConnection conn = PersonCacheSource.createConnection(true, true, null);
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);		

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMatMetadata.ddl")).trim(), metadataDDL.trim() );	

	}	
	
	@Test
	public void testPersonMetadataNoPKKey() throws Exception {
		ObjectConnection conn = PersonCacheSource.createConnection(null, false, null);
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);		


		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personNoKey.ddl")).trim(), metadataDDL.trim() );	

	}	


}
