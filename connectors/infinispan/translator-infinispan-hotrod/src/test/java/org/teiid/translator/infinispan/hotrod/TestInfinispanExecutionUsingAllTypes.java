/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.util.AllTypesSchemaVDBUtility;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.person.PersonSchemaVDBUtility;

/**
 * NOTES: 
 * 
 * <li>These test queries only test based on the source query.  A VIEW query will not 
 * resolve correctly to produce a source query that will be sent to the translator.
 * <li>The WHERE clause cannot be tested to confirm filtering at this time.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TestInfinispanExecutionUsingAllTypes {
	
	private static InfinispanHotRodConnection CONNECTION;
	
	private static TranslationUtility translationUtility = AllTypesSchemaVDBUtility.TRANSLATION_UTILITY;
	
	static Map<?, ?> DATA = AllTypesCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
        
		CONNECTION = AllTypesCacheSource.createConnection();

    }	


	@Test public void testExecution() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select intKey, intNum, stringNum, stringKey, floatNum, bigIntegerValue, shortValue, doubleNum, byteArrayValue, bigDecimalValue, longNum, booleanValue, timeStampValue, timeValue, dateValue, charValue  From AllTypes as T"); //$NON-NLS-1$
		
		performTest(10, 16, command);

	}
	
	/**
	 * Test that only the 'object' instance is returned in the result set
	 * @throws Exception
	 */
	@Ignore
	@Test public void testReturningObject() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select AllTypesObject From AllTypes as T"); //$NON-NLS-1$
		
		performTest(10, 1, command);

	}
	
	@Test public void testByteArray() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select intKey, byteArrayValue  From AllTypes as T"); //$NON-NLS-1$
		

		List<Class<?>> expectedResultTypes = new ArrayList<Class<?>>();
		expectedResultTypes.add(Integer.class);
		expectedResultTypes.add(byte[].class);

		List<List<Object>> rows = performTest(10, 2, command, expectedResultTypes);

	}

	protected List<List<Object>> performTest(int rowcnt, int colCount, Select command) 
			throws TranslatorException {
		
		ObjectExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<List<Object>> rows = new ArrayList<List<Object>>();
		
		int cnt = 0;
		List<Object> row = (List<Object>) exec.next();
	
		while (row != null) {
			rows.add(row);
			assertEquals("column count did not match", colCount, row.size());
			++cnt;
			row = (List<Object>) exec.next();
		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}
	
	protected List<List<Object>> performTest(int rowcnt, int colCount, Select command, List<Class<?>> expectedResultTypes)
			throws TranslatorException {
		
		ObjectExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<List<Object>> rows = new ArrayList<List<Object>>();
		
		int cnt = 0;
		List<Object> row = (List<Object>) exec.next();
	
		while (row != null) {
			rows.add(row);
			assertEquals("column count did not match", colCount, row.size());
			
			for (int i=0; i<expectedResultTypes.size(); i++) {
				
				assertEquals("values don't match for row " + cnt + " column " + i, row.get(i).getClass() ,expectedResultTypes.get(i));
				
			}
			
			++cnt;
			row = (List<Object>) exec.next();

		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}

	protected ObjectExecution createExecution(Select command, int rowCount, int colCount) throws TranslatorException {
		InfinispanExecutionFactory translator = new InfinispanExecutionFactory() {
//			@Override
//			public List<Object> search(ObjectVisitor visitor, ObjectConnection connection, ExecutionContext executionContext)
//					throws TranslatorException {
//				List<Object> rows = new ArrayList<Object>(DATA.values());
//    			return rows;
//			}

        };
        translator.start();

		
		return (ObjectExecution) translator.createExecution(command, Mockito.mock(ExecutionContext.class), PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
