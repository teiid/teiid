package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//import org.teiid.translator.object.testdata.person.*;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneNumber;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectUpdateExecution;
import org.teiid.translator.object.testdata.person.PersonSchemaVDBUtility;

public class TestInfinispanUpdateExecution {
	private static InfinispanHotRodConnection CONNECTION;
	private static TranslationUtility translationUtility = PersonSchemaVDBUtility.TRANSLATION_UTILITY;

	@SuppressWarnings("unchecked")
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
		Object o = getCache().get((new Integer(99).intValue()));
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into Person (id, name, email) VALUES (99, 'TestName', 'testName@mail.com')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) getCache().get((new Integer(99).intValue()));

		assertNotNull(p);
		assertTrue(p.getName().equals("TestName"));
		assertTrue(p.getEmail().equals("testName@mail.com"));
		
		command = translationUtility
                .parseCommand("upsert into Person (id, email) VALUES (98, 'new@mail.com')");

		ie = createExecution(command, Collections.EMPTY_LIST);

        ie.execute();

		p = (Person) getCache().get((new Integer(98).intValue()));

        assertNotNull(p);
        assertTrue(p.getEmail().equals("new@mail.com"));
        
        command = translationUtility
                .parseCommand("upsert into Person (id, email) VALUES (99, 'new1@mail.com')");
        
        ie = createExecution(command, Collections.EMPTY_LIST);

        ie.execute();
        
        p = (Person) getCache().get((new Integer(99).intValue()));

        assertNotNull(p);
        assertTrue(p.getName().equals("TestName"));
        assertTrue(p.getEmail().equals("new1@mail.com"));
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
		assertNotNull(getCache().get((new Integer(2).intValue())));

		Command command = translationUtility
				.parseCommand("Insert into PhoneNumber (id, number, type) VALUES (2, '999-888-7777', 'HOME')");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		Person p = (Person) getCache().get((new Integer(2).intValue()));
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
		Object o = getCache().get((new Integer(5).intValue()));
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update Person  SET name='Person 5 Changed', email='person5@mail.com' WHERE id=5");

		// no search required by the UpdateExecution logic
		List rows = new ArrayList();
		rows.add(o);

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		Person p = (Person) getCache().get((new Integer(5).intValue()));

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
		
		assertNotNull(getCache().get(new Integer(1).intValue()));

		Command command = translationUtility
				.parseCommand("Delete From Person Where id = 1");

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get(new Integer(1).intValue()));

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(getCache().get(new Integer(1).intValue()));

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
			
		assertNotNull(getCache().get(new Integer(2).intValue()));

		Command command = translationUtility
				.parseCommand("Delete From Person Where id = 2");


		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateExecution.DATA.get((new Integer(2).intValue())));

		ObjectUpdateExecution ie = createExecution(command, rows);
		
		ie.execute();
		assertNull(getCache().get((new Integer(2).intValue())));

	}

// TODD - add support for deleting container class
	public void testDeleteChildByValue() throws Exception {
		String phoneNumber="(111)222-3451";	
		
		// check the phone number exists
		Iterator it=getCache().values().iterator();
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

		ObjectUpdateExecution ie = createExecution(command, rows);

		assertNotNull(getCache().get(2));
		
		ie.execute();
		int[] cnts = ie.getUpdateCounts();
		assertTrue(cnts[0] == 10);
		
		it=getCache().values().iterator();
		// verify phone was completely removed
		while (it.hasNext()) {
			Person p = (Person) it.next();
			for (PhoneNumber ph : p.getPhones()) {
				assertFalse(ph.getNumber().equals(phoneNumber));
			}
		}
	}
	
	protected ObjectUpdateExecution createExecution(Command command, final List<Object> results)
			throws TranslatorException {
		
		TRANSLATOR = new InfinispanExecutionFactory() {
//			@Override
//			public List<Object> search(ObjectVisitor visitor, ObjectConnection connection, ExecutionContext executionContext)
//					throws TranslatorException {
//    			return results;
//			}

       
//			@Override
//			public Object performKeySearch(String columnName, Object value, ObjectConnection connection, ExecutionContext executionContext) throws TranslatorException {
//				return DATA.get(value);
//			}


		};
		TRANSLATOR.start();

		return (ObjectUpdateExecution) TRANSLATOR.createUpdateExecution(
				command,
				context,
				PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
	
	private static Map<Object,Object> getCache() throws TranslatorException {
		return (Map<Object,Object>) CONNECTION.getCache();
	}
}
