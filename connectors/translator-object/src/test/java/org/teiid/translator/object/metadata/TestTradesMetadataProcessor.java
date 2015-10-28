package org.teiid.translator.object.metadata;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
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
		TRANSLATOR.start();
    }

	@Test
	public void testTradeMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = TradesCacheSource.createConnection();

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected =  "CREATE FOREIGN TABLE Trade (\n"
				+ "\tTradeObject object OPTIONS (NAMEINSOURCE 'this', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.trades.Trade'),\n"
				+ "\ttradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),\n"
				+ "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tsettled boolean NOT NULL OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),\n"
				+ "\ttradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.util.Date'),\n"
				+ "\tCONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)\n"
				+ ") OPTIONS (NAMEINSOURCE 'Trades', UPDATABLE TRUE);"		
				;
		assertEquals(expected, metadataDDL);	

	}
	
	//@Test
	public void testTradeMetadataFromVDB() throws Exception {
		ObjectConnection conn = TradesCacheSource.createConnection();
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);		


		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected =  "CREATE FOREIGN TABLE Trade (\n"
				+ "\tTradeObject object OPTIONS (NAMEINSOURCE 'this', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.trades.Trade'),\n"
				+ "\ttradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),\n"
				+ "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
				+ "\tsettled boolean NOT NULL OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'boolean'),\n"
				+ "\ttradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.util.Date'),\n"
				+ "\tCONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)\n"
				+ ") OPTIONS (NAMEINSOURCE 'Trades', UPDATABLE TRUE);"		
				;
		assertEquals(expected, metadataDDL);	

	}	
	


}
