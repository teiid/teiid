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
import org.teiid.translator.infinispan.dsl.util.PersonSchemaVDBUtility;

public class TestInfinispanUpdateExecution {
	private static InfinispanConnection CONNECTION;
	private static TranslationUtility translationUtility = PersonSchemaVDBUtility.TRANSLATION_UTILITY;

	private static Map<Object, Object> DATA = PersonCacheSource.loadCache();
	private static InfinispanExecutionFactory TRANSLATOR;
	
	@Mock
	private ExecutionContext context;


	@Before public void beforeEach() throws Exception{	
		 
		MockitoAnnotations.initMocks(this);
    }

	@Test
	public void testInsertRootClass() throws Exception {

		CONNECTION = PersonCacheSource.createConnection(true);
		insertRoot();
	}
	
	@Test
	public void testInsertRootClassNoCacheTypeClassDefined() throws Exception {

		CONNECTION = PersonCacheSource.createConnection(false);
		insertRoot();
	}

	
	private void insertRoot() throws Exception {
		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache().get((new Integer(99).intValue()));
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Person (id, name, email) VALUES (99, 'TestName', 'testName@mail.com')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) CONNECTION.getCache().get((new Integer(99).intValue()));

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.getEmail().equals("testName@mail.com"));
		
	}
	
	@Test
	public void testInsertChildClass() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(true);
		insertChildClass();
	}

	@Test
	public void testInsertChildClassNoClassTypeDefined() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(false);
		insertChildClass();
	}
		
	private void insertChildClass() throws Exception {
		assertNotNull(CONNECTION.getCache().get((new Integer(2).intValue())));

		Command command = translationUtility
				.parseCommand("Insert into PhoneNumber (id, number, type) VALUES (2, '999-888-7777', 'HOME')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) CONNECTION.getCache().get((new Integer(2).intValue()));
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
		CONNECTION = PersonCacheSource.createConnection(true);
		updateRootClass();
	}
		
	@Test
	public void testUpdateRootClassNoClassTypeDefined() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(false);
		updateRootClass();
	}
	
	private void updateRootClass() throws Exception {
		
		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache().get((new Integer(5).intValue()));
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Person  SET name='Person 5 Changed', email='person5@mail.com' WHERE id=5");

		// no search required by the UpdateExecution logic
		List rows = new ArrayList();
		rows.add(o);

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		Person p = (Person) CONNECTION.getCache().get((new Integer(5).intValue()));

		assertNotNull(p);
		assertTrue(p.getName().equals("Person 5 Changed"));
		assertTrue(p.getEmail().equals("person5@mail.com"));
		
	}

	@Test
	public void testDeleteRootByKey() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(true);
		deleteRootClass();
	}

	@Test
	public void testDeleteRootByKeyNoClassTypeDefined() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(false);
		deleteRootClass();
	}
	
	private void deleteRootClass() throws Exception {
		
		assertNotNull(CONNECTION.getCache().get(new Integer(1).intValue()));

		Command command = translationUtility
				.parseCommand("Delete From Person Where id = 1");

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get(new Integer(1).intValue()));

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(CONNECTION.getCache()
				.get(new Integer(1).intValue()));

	}

	@Test
	public void testDeleteRootByValue() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(true);
		deleteRootByValue();
	}

	@Test
	public void testDeleteRootByValueNoClassTypeDefined() throws Exception {
		CONNECTION = PersonCacheSource.createConnection(false);
		deleteRootByValue();
	}
	
	private void deleteRootByValue() throws Exception {
			
		assertNotNull(CONNECTION.getCache().get(new Integer(2).intValue()));

		Command command = translationUtility
				.parseCommand("Delete From Person Where Name = 'Person 2'");


		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get((new Integer(2).intValue())));

		InfinispanUpdateExecution ie = createExecution(command, rows);
		
		ie.execute();
		assertNull(CONNECTION.getCache()
				.get((new Integer(2).intValue())));

	}

// TODD - add support for deleting container class
	public void testDeleteChildByValue() throws Exception {
		String phoneNumber="(111)222-3451";	
		
		// check the phone number exists
		Iterator it=CONNECTION.getCache().values().iterator();
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

		assertNotNull(CONNECTION.getCache().get(2));
		
		ie.execute();
		int[] cnts = ie.getUpdateCounts();
		assertTrue(cnts[0] == 10);
		
		it=CONNECTION.getCache().values().iterator();
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
				PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
