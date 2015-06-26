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

package org.teiid.translator.infinispan.dsl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.jboss.teiid.jdg_remote.pojo.AllTypesCacheSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.util.AllTypesSchemaVDBUtility;
import org.teiid.translator.infinispan.dsl.util.PersonSchemaVDBUtility;

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
	
	private static InfinispanConnection CONNECTION;
	
	private static TranslationUtility translationUtility = AllTypesSchemaVDBUtility.TRANSLATION_UTILITY;
	
	static Map<?, ?> DATA = AllTypesCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
        
		CONNECTION = AllTypesCacheSource.createConnection();

    }	


	@Test public void testExecution() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select intKey, intNum, stringNum, stringKey, floatNum, bigIntegerValue, shortValue, doubleNum, objectValue, bigDecimalValue, longNum, booleanValue, timeStampValue, timeValue, dateValue, charValue  From AllTypes as T"); //$NON-NLS-1$
		
		performTest(10, 16, command);

	}
	
	/**
	 * Test that only the 'object' instance is returned in the result set
	 * @throws Exception
	 */
	@Test public void testReturningObject() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select AllTypesObject From AllTypes as T"); //$NON-NLS-1$

		
		performTest(10, 1, command);

	}


	protected List<Object> performTest(int rowcnt, int colCount, Select command)
			throws TranslatorException {
		
		InfinispanExecution exec = createExecution(command, rowcnt, colCount);

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
		
		InfinispanExecution exec = createExecution(command, rowcnt, colCount);

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

	protected InfinispanExecution createExecution(Select command, int rowCount, int colCount) throws TranslatorException {
		InfinispanExecutionFactory translator = new InfinispanExecutionFactory() {
			@Override
			public List<Object> search(Select command, String cacheName,
					InfinispanConnection connection, ExecutionContext executionContext) {
					List<Object> rows = new ArrayList<Object>(DATA.values());
        			return rows;
         	}

        };
        translator.start();

		
		return (InfinispanExecution) translator.createExecution(command, Mockito.mock(ExecutionContext.class), PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
