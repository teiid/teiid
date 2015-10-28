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

package org.teiid.translator.object;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.person.PersonCacheSource;
import org.teiid.translator.object.testdata.person.PersonSchemaVDBUtility;
import org.teiid.translator.object.testdata.trades.VDBUtility;

/**
 * NOTES: 
 * 
 * <li>These test queries only test based on the source query.  A VIEW query will not 
 * resolve correctly to produce a source query that will be sent to the translator.
 * <li>The WHERE clause cannot be tested to confirm filtering at this time.
 * 
 * These test also test parent-child relationship between Person and PhoneNumber.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TestPersonKeySearch {
	
	private static ObjectConnection CONNECTION;
	
	private static TranslationUtility translationUtility = PersonSchemaVDBUtility.TRANSLATION_UTILITY;
	
	static Map<?, ?> DATA = PersonCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
        
		CONNECTION = PersonCacheSource.createConnection();

    }	


	@Test public void testExecution() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select name, id, email From Person as T"); //$NON-NLS-1$
		
		performTest(10, 3, command);

	}
	
	@Test public void testExecutionByKey() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select name, id, email From Person as T where id = 2"); //$NON-NLS-1$
		
		performTest(1, 3, command);

	}
	
	
	/**
	 * Test that only the 'object' instance is returned in the result set
	 * @throws Exception
	 */
	@Test public void testReturningObject() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select PersonObject From Person as T"); //$NON-NLS-1$

		
		performTest(10, 1, command);

	}
	
	@Test public void test1toManyOnlyChild() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select number From PhoneNumber as T"); //$NON-NLS-1$

		
		performTest(20, 1, command);

	}
	
	@Test public void test1toManyOnlyChildByKey() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select number From PhoneNumber as T where id = 3"); //$NON-NLS-1$

		
		performTest(2, 1, command);

	}	
	
	@Test public void test1toManyA() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select a.name, a.id, a.email, b.number From Person as A, PhoneNumber as b where a.id = b.id"); //$NON-NLS-1$

		
		performTest(20, 4, command);

	}
	
	@Test public void test1toManyB() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select a.name, a.id, a.email, b.number, b.type From Person as A, PhoneNumber as b where a.id = b.id and a.id = 4"); //$NON-NLS-1$

		
		performTest(2, 5, command);

	}	
	
	@Test public void testLimit() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select a.name, a.id, a.email From Person As a   LIMIT 4"); //$NON-NLS-1$

		
		performTest(4, 3, command);

	}	
	

	protected List<Object> performTest(int rowcnt, int colCount, Select command)
			throws TranslatorException {
		
		ObjectExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
	
		while (row != null) {
			rows.add(row);
			assertEquals("column count did not match", colCount, row.size());
			++cnt;
			row = exec.next();
		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}
	
	protected List<Object> performTest(int rowcnt, int colCount, Select command, List<Object> expectedResults)
			throws TranslatorException {
		
		ObjectExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
	
		while (row != null) {
			rows.add(row);
			assertEquals("column count did not match", colCount, row.size());
			
			for (int i=0; i<expectedResults.size(); i++) {
				
				assertEquals("values don't match for row " + cnt + " column " + i, row.get(i), expectedResults.get(i));
				
			}
			
			++cnt;
			row = exec.next();
		}
		
		assertEquals("Did not get expected number of rows", rowcnt, cnt); //$NON-NLS-1$
		
		exec.close();
		return rows;
	}

	protected ObjectExecution createExecution(Select command, int rowCount, int colCount) throws TranslatorException {
		ObjectExecutionFactory translator = new SimpleMapCacheExecutionFactory();
		//		ObjectExecutionFactory translator = new ObjectExecutionFactory() {
//			@Override
//			public List<Object> search(Select command, String cacheName,
//					ObjectConnection connection, ExecutionContext executionContext) {
//					List<Object> rows = new ArrayList<Object>(DATA.values());
//        			return rows;
//         	}
//
//        };
        translator.start();

		
		return (ObjectExecution) translator.createExecution(command, Mockito.mock(ExecutionContext.class), VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
