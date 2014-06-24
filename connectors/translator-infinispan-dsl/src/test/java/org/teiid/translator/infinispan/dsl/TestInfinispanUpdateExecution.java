package org.teiid.translator.infinispan.dsl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneNumber;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Update;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.util.VDBUtility;

public class TestInfinispanUpdateExecution {
	private static InfinispanConnection CONNECTION;
	private static TranslationUtility translationUtility = VDBUtility.TRANSLATION_UTILITY;

	private static Map<Object, Object> DATA = PersonCacheSource.loadCache();
	private static InfinispanExecutionFactory TRANSLATOR;
	
	@Mock
	private ExecutionContext context;


	@Before public void beforeEach() throws Exception{	
		 
		MockitoAnnotations.initMocks(this);
		CONNECTION = PersonCacheSource.createConnection();
    }

	@Test
	public void testInsertRootClass() throws Exception {

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Person (id, name, email) VALUES (99, 'TestName', 'testName@mail.com')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(99);

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.getEmail().equals("testName@mail.com"));
		
	}

	@Test
	public void testInsertChildClass() throws Exception {
		
		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2));

		Command command = translationUtility
				.parseCommand("Insert into PhoneNumber (id, number, type) VALUES (2, '999-888-7777', 'HOME')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2);
		assertNotNull(p);

		boolean found = false;
		for (PhoneNumber ph : p.getPhones()) {
			if (ph.getNumber().equals("999-888-7777") && ph.getType().equals(PhoneType.HOME)) {
				found = true;
			}
		}
		assertTrue(found);
	}
	
	@Test
	public void testUpdateRootClass() throws Exception {

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(5);
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Person  SET name='Person 5 Changed', email='person5@mail.com' WHERE id=5");

		// no search required by the UpdateExecution logic
		List rows = new ArrayList();
		rows.add(o);

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		Person p = (Person) CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(5);

		assertNotNull(p);
		assertTrue(p.getName().equals("Person 5 Changed"));
		assertTrue(p.getEmail().equals("person5@mail.com"));
		
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteRootByKey() throws Exception {
		
		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(1));

		Command command = translationUtility
				.parseCommand("Delete From Person Where id = 1");

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get(1));

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME)
				.get(new Integer(1).intValue()));

	}

	@Test
	public void testDeleteRootByValue() throws Exception {

		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2));

		Command command = translationUtility
				.parseCommand("Delete From Person Where Name = 'Person 2'");


		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get(2));

		InfinispanUpdateExecution ie = createExecution(command, rows);
		
		ie.execute();
		assertNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME)
				.get(2));

	}

// TODD - add support for deleting container class
	public void testDeleteChildByValue() throws Exception {
		String phoneNumber="(111)222-3451";	
		
		// check the phone number exists
		Iterator it=CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).values().iterator();
		boolean found = false;
		while (it.hasNext()) {
			Person p = (Person) it.next();
			for (PhoneNumber ph : p.getPhones()) {
				if (ph.getNumber().equals(phoneNumber)) {
					found = true;
					break;
				}
			}
		}
		assertTrue("Phone number " + phoneNumber + " was found", found);
		
		Command command = translationUtility
				.parseCommand("Delete From PhoneNumber Where Number = '" + phoneNumber + "'");	

		List rows = new ArrayList();
		rows.addAll(TestInfinispanUpdateExecution.DATA.values());

		InfinispanUpdateExecution ie = createExecution(command, rows);

		assertNotNull(CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).get(2));
		
		ie.execute();
		int[] cnts = ie.getUpdateCounts();
		assertTrue(cnts[0] == 10);
		
		it=CONNECTION.getCache(PersonCacheSource.PERSON_CACHE_NAME).values().iterator();
		// verify phone was completely removed
		while (it.hasNext()) {
			Person p = (Person) it.next();
			for (PhoneNumber ph : p.getPhones()) {
				assertFalse(ph.getNumber().equals(phoneNumber));
			}
		}
	}
	
	protected InfinispanUpdateExecution createExecution(Command command, final List<Object> results)
			throws TranslatorException {
		
		TRANSLATOR = new InfinispanExecutionFactory() {

			@Override
			public List<Object> search(Delete command, String cacheName,
					InfinispanConnection conn, ExecutionContext executionContext)
					throws TranslatorException {
				return results;
			}

			@Override
			public List<Object> search(Update command, String cacheName,
					InfinispanConnection conn, ExecutionContext executionContext)
					throws TranslatorException {
				return results;
			}

			@Override
			public Object performKeySearch(String cacheName, String colunName,
					Object value, InfinispanConnection conn,
					ExecutionContext executionContext)
					throws TranslatorException {
				return DATA.get(value);
			}

		};
		TRANSLATOR.start();

		return (InfinispanUpdateExecution) TRANSLATOR.createUpdateExecution(
				command,
				context,
				VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
