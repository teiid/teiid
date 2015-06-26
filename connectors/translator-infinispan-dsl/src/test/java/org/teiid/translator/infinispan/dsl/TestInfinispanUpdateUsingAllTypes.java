package org.teiid.translator.infinispan.dsl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.teiid.jdg_remote.pojo.AllTypes;
import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.util.AllTypesSchemaVDBUtility;


public class TestInfinispanUpdateUsingAllTypes {
	
	private static InfinispanConnection CONNECTION;
	private static TranslationUtility translationUtility = AllTypesSchemaVDBUtility.TRANSLATION_UTILITY;

	private static Map<Object, Object> DATA = AllTypesCacheSource.loadCache();
	private static InfinispanExecutionFactory TRANSLATOR;
	
	@Mock
	private ExecutionContext context;


	@Before public void beforeEach() throws Exception{	
		 
		MockitoAnnotations.initMocks(this);
		CONNECTION = AllTypesCacheSource.createConnection();
    }

	@Test
	public void testInsertRootClass() throws Exception {

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', true, 1200  )");

		
//		.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum, doubleNum, floatNum) " + 
//				"VALUES (99, 991, 'string key value', '999', true, 1200, 23.45, 12.456  )");

	/**	
		private Character charValue;
		private Double doubleNum;
		private BigInteger bigIntegerValue;
		private Short shortValue;
		private Float floatNum;
		private Object  objectValue;

		private BigDecimal bigDecimalValue;
		private Timestamp timeStampValue;
		private Time timeValue;
		private Date dateValue;
		private byte[] byteArrayValue;
		*/
		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		AllTypes p = (AllTypes) CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);

		String stringNum = String.valueOf("999");
		
		
		assertNotNull(p);
		assertTrue(p.getIntKey().equals(Integer.valueOf(99)));
		assertTrue(p.getIntNum().equals(Integer.valueOf(991)) );
		assertTrue(p.getStringNum().equals(stringNum));
		assertTrue(p.getStringKey().equals( String.valueOf("string key value")));
		assertTrue(p.getLongNum().equals(Long.valueOf(1200)) );
		assertTrue(p.getBooleanValue().equals(Boolean.TRUE) );
//		assertTrue(p.getDoubleNum().equals(Double.valueOf(23.45d)) );
//		assertTrue(p.getFloatNum().equals(Float.valueOf(12.456f)) );
			
	}

	// TEIID-3534 - cannot insert Boolean attribute with null
	@Test
	public void testInsertBooleanOfNull() throws Exception {

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();

		AllTypes p = (AllTypes) CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);

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

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);
		assertNull(o);
		
		Command command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		@SuppressWarnings("unchecked")
		InfinispanUpdateExecution ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();
		
		command = translationUtility
				.parseCommand("Insert into AllTypes (intKey, intNum, stringKey, stringNum, booleanValue, longNum) " + 
							"VALUES (99, 991, 'string key value', '999', null, 1200  )");

		// no search required by the UpdateExecution logic
		ie = createExecution(command, Collections.EMPTY_LIST);

		ie.execute();
		
		

		AllTypes p = (AllTypes) CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(99);

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

		// check the object doesn't exist before inserting
		Object o = CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(5);
		assertNotNull(o);
		
		Command command = translationUtility
				.parseCommand("Update AllTypes  SET stringNum='10000' WHERE intKey=5");

		// no search required by the UpdateExecution logic
		List rows = new ArrayList();
		rows.add(o);

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();

		AllTypes p = (AllTypes) CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(5);

		String stringNum = String.valueOf("10000");
		
		assertNotNull(p);
		assertTrue(p.getStringNum().equals(stringNum));
		
	}

//	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteRootByKey() throws Exception {
		
		assertNotNull(CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME).get(1));

		Command command = translationUtility
				.parseCommand("Delete From AllTypes Where intKey=99");

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		rows.add(TestInfinispanUpdateUsingAllTypes.DATA.get(1));

		InfinispanUpdateExecution ie = createExecution(command, rows);

		ie.execute();
		assertNull(CONNECTION.getCache(AllTypesCacheSource.ALLTYPES_CACHE_NAME)
				.get(new Integer(1).intValue()));

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
				AllTypesSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
