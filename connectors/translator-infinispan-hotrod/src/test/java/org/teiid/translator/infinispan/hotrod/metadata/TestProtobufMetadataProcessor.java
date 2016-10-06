package org.teiid.translator.infinispan.hotrod.metadata;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheConnection;
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

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")), metadataDDL);	

	}
	
	@Test
	public void testPersonMetadataUpperCaseKeyField() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = PersonCacheConnection.createConnection(PersonCacheSource.loadCache(), "ID", false, null);

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")), metadataDDL);	
	}
	
	@Test
	public void testPersonMetadataMixedCaseKeyField() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		InfinispanHotRodConnection conn = PersonCacheConnection.createConnection(PersonCacheSource.loadCache(), "Id", false, null);

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("personMetadata.ddl")), metadataDDL);	
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

		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("allTypesMetadata.ddl")), metadataDDL );	

	}	

}
