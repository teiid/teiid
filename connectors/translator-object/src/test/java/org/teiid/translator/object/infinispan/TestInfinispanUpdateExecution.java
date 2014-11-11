package org.teiid.translator.object.infinispan;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.TradesCacheSource;
//import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.*;

public class TestInfinispanUpdateExecution {
	private static ObjectConnection CONNECTION;
	private static TranslationUtility translationUtility = VDBUtility.TRANSLATION_UTILITY;

	private  InfinispanExecutionFactory factory;
	
	@Mock
	private static ExecutionContext context;

	@BeforeClass
    public static void beforeClass() throws Exception {  
		 
		context = mock(ExecutionContext.class);
		
		CONNECTION = TestInfinispanConnection.createConnection();

    }
	
	@AfterClass
	public static void afterClass() {
		((TestInfinispanConnection) CONNECTION).cleanUp();
	}
	
	@Before public void beforeEachTest() throws Exception{	
		factory = new InfinispanExecutionFactory();
		factory.setSupportsLuceneSearching(true);
		factory.start();
    }	
	
	
	/** 
	 * controller method for all tests, this is so only 1 cacheManager is created
	 * once for the duration (not 1 for each test).  This is done so that this also test
	 * the behavior when multiple changes are made.
	 * @throws Exception
	 */

	@Test
	public void testScenarios() throws Exception {
		
		testInsertRootClass();
		testUpdateByKey();
		testUpdateByValue();
		testDeleteRootByKey();
		testDeleteRootByValue();

	}


	public void testInsertRootClass() throws Exception {

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("99");
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Trade_Object.Trade (tradeId, TradeName, settled) VALUES (99, 'TestName', 'true')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("99");

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.isSettled());
		
	}

//	@Test
//	public void testInsertChildClass() throws Exception {
//		
//		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2));
//
//		Command command = translationUtility
//				.parseCommand("Insert into PhoneNumber (id, number, type) VALUES (2, '999-888-7777', 'HOME')");
//
//		// no search required by the UpdateExecution logic
//		@SuppressWarnings("unchecked")
//		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);
//
//		ie.execute();
//
//		Person p = (Person) CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2);
//		assertNotNull(p);
//
//		boolean found = false;
//		for (PhoneNumber ph : p.getPhones()) {
//			if (ph.getNumber().equals("999-888-7777") && ph.getType().equals(PhoneType.HOME)) {
//				found = true;
//			}
//		}
//		assertTrue(found);
//	}
//	


	public void testUpdateByKey() throws Exception {
		
		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("1");
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Trade  SET TradeName='Person 1 Changed', settled='true' WHERE TradeId=1");

		InfinispanUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("1");

		assertNotNull(p);
		assertTrue(p.getName().equals("Person 1 Changed"));
		assertTrue(p.isSettled());
		
	}
	

	public void testUpdateByValue() throws Exception {
	
		Command command = translationUtility
				.parseCommand("Update Trade SET settled='false' WHERE settled='true'");

		InfinispanUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("2");

		assertNotNull(p);
		assertFalse(p.isSettled());
		
		p = (Trade) CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("99");

		assertNotNull(p);
		assertFalse(p.isSettled());
		
	}	

	public void testDeleteRootByKey() throws Exception {
		
		Object o = CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("99");
		assertNotNull(o);

		Command command = translationUtility
				.parseCommand("Delete From Trade Where TradeId = 99");

		InfinispanUpdateExecution ie = createExecution(command);

		ie.execute();
		
		// sleep so that the async delete is completed
		Thread.sleep(3000);

		Trade p = (Trade) CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("99");

		assertNull(p);

	}

	public void testDeleteRootByValue() throws Exception {

		assertNotNull(CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("2"));
		assertNotNull(CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("3"));

		Command command = translationUtility
				.parseCommand("Delete From Trade Where settled = 'false'");


		InfinispanUpdateExecution ie = createExecution(command);
		
		ie.execute();
		
		// sleep so that the async delete is completed
		Thread.sleep(3000);

		
		assertNull(CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("2"));
		assertNull(CONNECTION.getCacheContainer().getCache(TradesCacheSource.TRADES_CACHE_NAME).get("3"));

	}
//
//// TODD - add support for deleting container class
//	public void testDeleteChildByValue() throws Exception {
//		String phoneNumber="(111)222-3451";	
//		
//		// check the phone number exists
//		Iterator it=CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).values().iterator();
//		boolean found = false;
//		while (it.hasNext()) {
//			Person p = (Person) it.next();
//			for (PhoneNumber ph : p.getPhones()) {
//				if (ph.getNumber().equals(phoneNumber)) {
//					found = true;
//					break;
//				}
//			}
//		}
//		assertTrue("Phone number " + phoneNumber + " was found", found);
//		
//		Command command = translationUtility
//				.parseCommand("Delete From PhoneNumber Where Number = '" + phoneNumber + "'");	
//
//		List rows = new ArrayList();
//		rows.addAll(TestInfinispanUpdateExecution.DATA.values());
//
//		InfinispanUpdateExecution ie = createExecution(command, rows);
//
//		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2));
//		
//		ie.execute();
//		int[] cnts = ie.getUpdateCounts();
//		assertTrue(cnts[0] == 10);
//		
//		it=CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).values().iterator();
//		// verify phone was completely removed
//		while (it.hasNext()) {
//			Person p = (Person) it.next();
//			for (PhoneNumber ph : p.getPhones()) {
//				assertFalse(ph.getNumber().equals(phoneNumber));
//			}
//		}
//	}
//	
	protected InfinispanUpdateExecution createExecution(Command command) {

		return (InfinispanUpdateExecution) factory.createUpdateExecution(
				command,
				context,
				VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
