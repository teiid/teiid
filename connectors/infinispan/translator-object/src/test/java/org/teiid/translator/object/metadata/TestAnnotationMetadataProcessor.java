package org.teiid.translator.object.metadata;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;

@SuppressWarnings("nls")
public class TestAnnotationMetadataProcessor {

	protected static ObjectExecutionFactory TRANSLATOR;

	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new SimpleMapCacheExecutionFactory();
    }
	
	@Test
	public void testTradesMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = TradesAnnotatedCacheSource.createConnection();
		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();


		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		String expected = "CREATE FOREIGN TABLE Trade (\n"
	    + "\tTradeObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.annotated.Trade'),\n"
	    + "\ttradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),\n"	    
	    + "\tdescription string OPTIONS (NAMEINSOURCE 'description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tsettled boolean OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Searchable', NATIVE_TYPE 'boolean'),\n"
	    + "\ttradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.util.Date'),\n"
		+ "\tCONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)\n"
		+ ") OPTIONS (UPDATABLE TRUE);"		
		;
		assertEquals(expected, metadataDDL);	

	}
	
	@Test
	public void testPersonMetadataWithNoObjectColumn() throws Exception {
		

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = TradesAnnotatedCacheSource.createConnection();

		TRANSLATOR.initCapabilities(conn);
		TRANSLATOR.start();

		JavaBeanMetadataProcessor mp = (JavaBeanMetadataProcessor) TRANSLATOR.getMetadataProcessor();
		mp.setClassObjectColumn(true);
		mp.process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		String expected = "CREATE FOREIGN TABLE Trade (\n"
	    + "\tTradeObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.teiid.translator.object.testdata.annotated.Trade'),\n"
	    + "\ttradeId long NOT NULL OPTIONS (NAMEINSOURCE 'tradeId', SEARCHABLE 'Searchable', NATIVE_TYPE 'long'),\n"	    
	    + "\tdescription string OPTIONS (NAMEINSOURCE 'description', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tname string OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tsettled boolean OPTIONS (NAMEINSOURCE 'settled', SEARCHABLE 'Searchable', NATIVE_TYPE 'boolean'),\n"
	    + "\ttradeDate date OPTIONS (NAMEINSOURCE 'tradeDate', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.util.Date'),\n"
		+ "\tCONSTRAINT PK_TRADEID PRIMARY KEY(tradeId)\n"
		+ ") OPTIONS (UPDATABLE TRUE);"		
		;
		assertEquals(expected, metadataDDL);	

	}

}
