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
import org.teiid.translator.object.testdata.trades.TradesCacheSource;


@SuppressWarnings("nls")
public class TestTradesMetadataProcessor {
	protected static ObjectExecutionFactory TRANSLATOR;

	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new SimpleMapCacheExecutionFactory();
    }

	@Test
	public void testTradeChildMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = TradesCacheSource.createConnection(false);

		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tradeChildMetadata.ddl")), metadataDDL );	

	}
	
	@Test
	public void testTradeMetadataFromVDB() throws Exception {
		ObjectConnection conn = TradesCacheSource.createConnection(true);
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "ObjectSchema",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);		


		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tradeMetadataFromVDB.ddl")), metadataDDL );	

	}	
	
	@Test
	public void testMatTradeMetadataFromVDB() throws Exception {
		ObjectConnection conn = TradesCacheSource.createConnection(true, true);
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "ObjectSchema",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);		


		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);
		
		assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tradeMatMetadataFromVDB.ddl")).trim(), metadataDDL.trim() );	

	}	
	


}
