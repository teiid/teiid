package org.teiid.translator.object;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.trades.Leg;
import org.teiid.translator.object.testdata.trades.Trade;
import org.teiid.translator.object.testdata.trades.TradesCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectUpdateExecution {
	private static ObjectConnection CONNECTION;
	private static TranslationUtility translationUtility = VDBUtility.TRANSLATION_UTILITY;

	private static ObjectExecutionFactory TRANSLATOR;
	
	
	@Mock
	private ExecutionContext context;

	@Before public void before() {
		MockitoAnnotations.initMocks(this);
	}

	@Before public void init() throws Exception{
		
		
		// pre-test of the object connection 
		CONNECTION = TradesCacheSource.createConnection();

    }
	

	@Test
	public void testInsertRootClass() throws Exception {
		// check the object doesn't exist before inserting
		Object o = CONNECTION.get(new Long(99).longValue());
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Trade_Object.Trade (tradeId, TradeName, settled) VALUES (99, 'TestName', 'true')");

		// no search required by the UpdateExecution logicconn
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Trade p = (Trade) CONNECTION.get((new Long(99).longValue()));

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.getTradeId() == 99);
		assertTrue(p.isSettled());
	
	}
	
	@Test( expected = TranslatorException.class )
	public void testInsertOfDuplicateRootObject() throws Exception {
		Object o = CONNECTION.get(new Long(2).longValue());
		assertNotNull(o);
		Command command = translationUtility
				.parseCommand("Insert into Trade_Mat.Trade_Mat.Trade (tradeId, TradeName, settled) VALUES (2, 'Duplicate 2', 'true')");
		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

	}
	
	@Test
	public void testInsertUsingFolders() throws Exception {
		// check the object doesn't exist before inserting
		Object o = CONNECTION.get(new Long(99).longValue());
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Trade_Mat.Trade_Mat.Trade (tradeId, TradeName, settled) VALUES (99, 'TestName', 'true')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Trade p = (Trade) CONNECTION.get((new Long(99).longValue()));

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.getTradeId() == 99);
		assertTrue(p.isSettled());
	
	}
	
	@Test
	public void testInsertChildClass() throws Exception {
		CONNECTION = TradesCacheSource.createConnection();
		assertNotNull(CONNECTION.get((new Long(2).longValue())));

		Command command = translationUtility
				.parseCommand("Insert into Trade_Object.Leg (tradeID, notational, Name) VALUES (2, 3.456, 'legName 2a')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Trade p = (Trade) CONNECTION.get((new Long(2).longValue()));
		assertNotNull(p);

		boolean found = false;
		for (Leg ph : p.getLegs()) {
			if (ph.getLegName().equals("legName 2a")) {
				found = true;
			}
		}
		assertTrue(found);
	}

	@Test
	public void testUpdateRootClass() throws Exception {
		
		// check the object doesn't exist before inserting
		Object o = CONNECTION.get(new Long(2).longValue());
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Trade_Object.Trade  SET TradeName='Person 2 Changed', settled='true' WHERE TradeId=2");

		// no search required by the UpdateExecution logic
		List<Object> rows = new ArrayList<Object>();
		rows.add(o);

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		Trade p = (Trade) CONNECTION.get(new Long(2).longValue());

		assertNotNull(p);
		assertTrue(p.getName().equals("Person 2 Changed"));
		assertTrue(p.isSettled());
		
	}

	@Test
	public void testDeleteRootByKey() throws Exception {
		
		Object o = CONNECTION.get(new Long(1).longValue());
		assertNotNull(o);

		Command command = translationUtility
				.parseCommand("Delete From Trade_Object.Trade Where tradeId = 1");

		List<Object> rows = new ArrayList<Object>();
		rows.add(o);

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(CONNECTION.get(new Long(1).longValue()));

	}
	
	@Test
	public void testDeleteAll() throws Exception {
		
		Object o = CONNECTION.get(new Long(1).longValue());
		assertNotNull(o);

		Command command = translationUtility
				.parseCommand("Delete From Trade_Object.Trade");

		List<Object> rows = new ArrayList<Object>();

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		
		Collection<Object> all = CONNECTION.getAll();
		assertTrue(all.size() == 0);

	}


// TODD - add support for deleting container class
//	public void testDeleteChildByValue() throws Exception {
//		String phoneNumber="(111)222-3451";	
//		
//		// check the phone number exists
//		Iterator it=CONNECTION.getCache().values().iterator();
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
//		rows.addAll(TestObjectUpdateExecution.DATA.values());
//
//		ObjectUpdateExecution ie = createExecution(command, rows);
//
//		assertNotNull(CONNECTION.getCache().get(2));
//		
//		ie.execute();
//		int[] cnts = ie.getUpdateCounts();
//		assertTrue(cnts[0] == 10);
//		
//		it=CONNECTION.getCache().values().iterator();
//		// verify phone was completely removed
//		while (it.hasNext()) {
//			Person p = (Person) it.next();
//			for (PhoneNumber ph : p.getPhones()) {
//				assertFalse(ph.getNumber().equals(phoneNumber));
//			}
//		}
//	}
	
	protected ObjectUpdateExecution createExecution(Command command, final List<Object> results)
			throws TranslatorException {
		TRANSLATOR = new SimpleMapCacheExecutionFactory();

		TRANSLATOR.start();

		return (ObjectUpdateExecution) TRANSLATOR.createUpdateExecution(
				command,
				context,
				VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
	

}
