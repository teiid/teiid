package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.jboss.teiid.jdg_remote.pojo.AllTypes;
import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.util.AllTypesSchemaVDBUtility;
import org.teiid.translator.object.ObjectUpdateExecution;


public class TestInfinispanUpdateUsingAllTypes {
	
	private static InfinispanHotRodConnection CONNECTION;
	private static TranslationUtility translationUtility = AllTypesSchemaVDBUtility.TRANSLATION_UTILITY;

	private static RemoteCache DATA = AllTypesCacheSource.loadCache();
	private static InfinispanHotRodExecutionFactory TRANSLATOR;
	
	@Mock
	private ExecutionContext context;


	@Before public void beforeEach() throws Exception{	
		 
		MockitoAnnotations.initMocks(this);
    }

	@Test
	public void testInsertRootClass() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection();
		insertRootClass();
	}
	
	@Test
	public void testInsertRootClassNoClasTypeKeyDefined() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection(false);
		insertRootClass();
	}	
	
	private void insertRootClass() throws Exception {

		// check the object doesn't exist before inserting
		Object o = getCache().get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', true, 1200  )");

		
//		.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum, doubleNum, floatNum) " + 
//				"VALUES (99, 991, 'string key value', '999', true, 1200, 23.45, 12.456  )");


		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		AllTypes p = (AllTypes) getCache().get(99);

		String stringNum = String.valueOf("999");
		
		
		assertNotNull(p);
		assertTrue(p.getIntKey().equals(Integer.valueOf(99)));
		assertTrue(p.getIntNum().equals(Integer.valueOf(991)) );
		assertTrue(p.getStringNum().equals(stringNum));
		assertTrue(p.getStringKey().equals( String.valueOf("string key value")));
		assertTrue(p.getLongNum().equals(Long.valueOf(1200)) );
		assertTrue(p.getBooleanValue().equals(Boolean.TRUE) );

			
	}
	
	private void insertBytes() throws Exception {

		// check the object doesn't exist before inserting
		Object o = getCache().get(199);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
				"VALUES (199, 199, 'string key value', '999', true, 1200  ) ");  

						//new String(b, "UTF-8")  + "'  )");
		
		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		AllTypes p = (AllTypes) getCache().get(199);

		String stringNum = String.valueOf("199");
		
		
		assertNotNull(p);
		assertTrue(p.getIntKey().equals(Integer.valueOf(199)));
		assertTrue(p.getIntNum().equals(Integer.valueOf(199)) );
		assertTrue(p.getStringNum().equals(stringNum));
		assertTrue(p.getStringKey().equals( String.valueOf("string key value")));
		assertTrue(p.getLongNum().equals(Long.valueOf(1200)) );
		assertTrue(p.getBooleanValue().equals(Boolean.TRUE) );
//		assertTrue(p.getByteArrayValue().equals(b));

		//		assertTrue(p.getDoubleNum().equals(Double.valueOf(23.45d)) );
//		assertTrue(p.getFloatNum().equals(Float.valueOf(12.456f)) );
			
	}	

	// TEIID-3534 - cannot insert Boolean attribute with null
	@Test
	public void testInsertBooleanOfNull() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection();
		insertBooleanOfNull();
	}

	@Test
	public void testInsertBooleanOfNullNoClassKeyTypeDefined() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection(false);
		insertBooleanOfNull();
	}

	private void insertBooleanOfNull() throws Exception {

		// check the object doesn't exist before inserting
		Object o = getCache().get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		AllTypes p = (AllTypes) getCache().get(99);

		String stringNum = String.valueOf("999");
		
		
		assertNotNull(p);
		assertTrue(p.getIntKey().equals(Integer.valueOf(99)));
		assertTrue(p.getIntNum().equals(Integer.valueOf(991)) );
		assertTrue(p.getStringNum().equals(stringNum));
		assertTrue(p.getStringKey().equals( String.valueOf("string key value")));
		assertTrue(p.getLongNum().equals(Long.valueOf(1200)) );
		assertTrue(p.getBooleanValue() == null );
		
		command = (Select)translationUtility.parseCommand("select intKey, intNum From AllTypes where booleanValue Is Not Null"); //$NON-NLS-1$

			
	}
	
	// TEIID-3539 -  Can't use IS [NOT] NULL operator for non-string columns
	//@Test
	public void testInsertIsNotNullOperator() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection();

		// check the object doesn't exist before inserting
		Object o = getCache().get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		ObjectUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();
		
		command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();
		
		

		AllTypes p = (AllTypes) getCache().get(99);

		String stringNum = String.valueOf("999");
		
		
		assertNotNull(p);
		assertTrue(p.getIntKey().equals(Integer.valueOf(99)));
		assertTrue(p.getIntNum().equals(Integer.valueOf(991)) );
		assertTrue(p.getStringNum().equals(stringNum));
		assertTrue(p.getStringKey().equals( String.valueOf("string key value")));
		assertTrue(p.getLongNum().equals(Long.valueOf(1200)) );
		assertTrue(p.getBooleanValue() == null );
			
	}	

	@Test
	public void testUpdateRootClass() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection();
		updateRootClass();
	}

	@Test
	public void testUpdateRootClassNoClassKeyTypeDefined() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection(false);
		updateRootClass();
	}
	
	private void updateRootClass() throws Exception {
	
	
	// check the object doesn't exist before inserting
		Object o = getCache().get(5);
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update AllTypes  SET stringNum='10000' WHERE intKey=5");

		// no search required by the UpdateExecution logic
		List rows = new ArrayList();
		rows.add(o);

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		AllTypes p = (AllTypes) getCache().get(5);

		String stringNum = String.valueOf("10000");
		
		assertNotNull(p);
		assertTrue(p.getStringNum().equals(stringNum));
		
	}

	@Test
	public void testDeleteRootByKey() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection();
		deleteRootByKey();
	}
		
	@Test
	public void testDeleteRootByKeyNoClassKeyTypeDefined() throws Exception {
		CONNECTION = AllTypesCacheSource.createConnection(false);
		deleteRootByKey();
	}

	private void deleteRootByKey() throws Exception {
	
		assertNotNull(getCache().get(1));

		Command command = translationUtility
				.parseCommand("Delete From AllTypes Where intKey=1");

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateUsingAllTypes.DATA.get(1));

		ObjectUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(getCache().get(new Integer(1).intValue()));

	}


	protected ObjectUpdateExecution createExecution(Command command, final List<Object> results)
			throws TranslatorException {
		
		TRANSLATOR = new InfinispanHotRodExecutionFactory() {

		};
		TRANSLATOR.start();

		return (ObjectUpdateExecution) TRANSLATOR.createUpdateExecution(
				command,
				context,
				AllTypesSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
	
	@SuppressWarnings("unchecked")
	private static Map<Object,Object> getCache() throws TranslatorException {
		return (Map<Object,Object>) CONNECTION.getCache();
	}
	
}
