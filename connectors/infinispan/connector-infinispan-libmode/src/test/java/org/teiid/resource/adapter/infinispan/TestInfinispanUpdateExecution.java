package org.teiid.resource.adapter.infinispan;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.libmode.InfinispanCacheExecutionFactory;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectUpdateExecution;
import org.teiid.translator.object.testdata.annotated.Trade;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanUpdateExecution {
	private static TranslationUtility translationUtility = VDBUtility.TRANSLATION_UTILITY;

    private static InfinispanManagedConnectionFactory factory = null;
	private static ExecutionContext context;
	private static ObjectConnection CONNECTION;
	
	private static InfinispanCacheExecutionFactory TRANS_FACTORY = null;

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		context = mock(ExecutionContext.class);

		factory = new InfinispanManagedConnectionFactory();

		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(InfinispanTestHelper.TRADE_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.annotated.Trade;longValue:long");

		
		TRANS_FACTORY = new InfinispanCacheExecutionFactory();
		TRANS_FACTORY.start();
		
		CONNECTION = factory.createConnectionFactory().getConnection();
		
		TradesAnnotatedCacheSource.loadCache(  (Map<Object,Object>)CONNECTION.getCache(InfinispanTestHelper.TRADE_CACHE_NAME ));

	}
	
	@AfterClass
	public static void afterClass() {
		CONNECTION.cleanUp();
		factory.shutDownCache();
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
		Object o = CONNECTION.get(Long.valueOf(599));
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Trade_Object.Trade (tradeId, TradeName, settled) VALUES (599, 'TestName', 'true')");

		// no search required by the UpdateExecution logic
		ObjectUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.get(Long.valueOf(599));

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
		
		// check the object exists
		Object o = CONNECTION.get(Long.valueOf(1));
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Trade  SET TradeName='Person 1 Changed', settled='true' WHERE TradeId=1");

		ObjectUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.get(Long.valueOf(1));

		assertNotNull(p);
		assertTrue(p.getName().equals("Person 1 Changed"));
		assertTrue(p.isSettled());
		
	}
	

	public void testUpdateByValue() throws Exception {
	
		Command command = translationUtility
				.parseCommand("Update Trade SET settled='false' WHERE settled='true'");

		ObjectUpdateExecution ie = createExecution(command);

		ie.execute();

		Trade p = (Trade) CONNECTION.get(Long.valueOf(2));

		assertNotNull(p);
		assertFalse(p.isSettled());
		
		p = (Trade) CONNECTION.get(Long.valueOf(99));

		assertNotNull(p);
		assertFalse(p.isSettled());
		
	}	

	public void testDeleteRootByKey() throws Exception {
		
		Object o = CONNECTION.get(Long.valueOf(99));
		assertNotNull(o);

		Command command = translationUtility
				.parseCommand("Delete From Trade Where TradeId = 99");

		ObjectUpdateExecution ie = createExecution(command);

		ie.execute();
		
		// sleep so that the async delete is completed
		Thread.sleep(3000);

		Trade p = (Trade) CONNECTION.get(Long.valueOf(99));

		assertNull(p);

	}

	public void testDeleteRootByValue() throws Exception {

		assertNotNull(CONNECTION.get(Long.valueOf(2)));
		assertNotNull(CONNECTION.get(Long.valueOf(3)));
		
		Command command = translationUtility
				.parseCommand("Delete From Trade Where settled = 'false'");


		ObjectUpdateExecution ie = createExecution(command);
		
		ie.execute();
		
		// sleep so that the async delete is completed
		Thread.sleep(3000);

		
		assertNull(CONNECTION.get(Long.valueOf(2)));
		assertNull(CONNECTION.get(Long.valueOf(3)));

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
	protected ObjectUpdateExecution createExecution(Command command) throws TranslatorException {

		return (ObjectUpdateExecution) TRANS_FACTORY.createUpdateExecution(
				command,
				context,
				VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
