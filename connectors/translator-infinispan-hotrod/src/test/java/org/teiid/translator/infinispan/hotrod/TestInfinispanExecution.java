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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.person.PersonSchemaVDBUtility;
import org.teiid.util.Version;

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
public class TestInfinispanExecution {
	
	private static InfinispanHotRodConnection CONNECTION;
	
	private static TranslationUtility translationUtility = PersonSchemaVDBUtility.TRANSLATION_UTILITY;
	
	static Map<?, ?> DATA = PersonCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
        
		CONNECTION = PersonCacheSource.createConnection(true, Version.getVersion("7.2.3"));

    }	


	@Test public void testExecution() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select name, id, email From Person as T"); //$NON-NLS-1$
		
		performTest(10, 3, command);

	}
	
	@Test public void testExecutionLimitAndIn() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select name, id From Person as T where id in (-1, 1, 5, 9) limit 2"); //$NON-NLS-1$
        
        performTest(2, 2, command);

    }
	
	/**
	 * Test that only the 'object' instance is returned in the result set
	 * @throws Exception
	 */
	@Test public void testReturningObject() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select PersonObject From Person as T"); //$NON-NLS-1$

		
		performTest(10, 1, command);

	}
	
	@Test public void test1toManyOnlyChildColumn() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select number From PhoneNumber as T"); //$NON-NLS-1$

		
		performTest(20, 1, command);

	}
	
	@Test public void test1toManyParentAndChildColumn() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select b.id, b.number From  PhoneNumber as b"); //$NON-NLS-1$

		
		performTest(20, 2, command);

	}
	
	@Test public void test1toManyB() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select a.name, a.id, a.email, b.number, b.type From Person as A, PhoneNumber as b where a.id = b.id"); //$NON-NLS-1$

		
		performTest(20, 5, command);

	}	

	@Test public void test1to1Child() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select City From Address as T"); //$NON-NLS-1$

		
		performTest(10, 1, command);

	}
	
	@Test public void test1to1Child_B() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select id, City From Address as T"); //$NON-NLS-1$

		
		performTest(10, 2, command);

	}
	
	@Test public void test1to1Child_All() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select * From Address as T"); //$NON-NLS-1$

		
		performTest(10, 4, command);

	}

	protected List<Object> performTest(int rowcnt, int colCount, Select command)
			throws TranslatorException {
		
		ObjectExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<?> row = exec.next();
	
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
		List<?> row = exec.next();
	
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
		InfinispanHotRodExecutionFactory translator = new InfinispanHotRodExecutionFactory() {
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
