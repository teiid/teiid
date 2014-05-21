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
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.util.VDBUtility;

/**
 * NOTE: These test queries only test based on the source query.  A VIEW query will not 
 * resolve correctly to produce a source query that will be sent to the translator.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TestInfinispanExecution {
	
	private static InfinispanConnection CONNECTION;
	
	private static TranslationUtility translationUtility = VDBUtility.TRANSLATION_UTILITY;
	
	private static Map<?, ?> DATA = PersonCacheSource.loadCache();
	
	
    @BeforeClass
    public static void setUp()  {
        
		CONNECTION = PersonCacheSource.createConnection();

    }	


	@Test public void testExecution() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select PersonName, PersonID, Email From Persons_Object_Model.Persons_Cache as T"); //$NON-NLS-1$
		
		performTest(10, 3, command);

	}
	
	/**
	 * Test that only the 'object' instance is returned in the result set
	 * @throws Exception
	 */
	@Test public void testReturningObject() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select PersonObject From Persons_Object_Model.Persons_Cache as T"); //$NON-NLS-1$

		
		performTest(10, 1, command);

	}
	
//	/**
//	 * Test querying the view
//	 * @throws Exception
//	 */
//	@Test public void testPersonView() throws Exception {
//		Select command = (Select)translationUtility.parseCommand("select * From Persons_Object_Model.Persons_Cache"); //$NON-NLS-1$
//	
//		performTest(10, 4, command);
//
//	}

	protected List<Object> performTest(int rowcnt, int colCount, Select command)
			throws TranslatorException {
		
		InfinispanExecution exec = createExecution(command, rowcnt, colCount);

		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
	
		while (row != null) {
			rows.add(row);
			assertEquals(colCount, row.size());
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
					InfinispanConnection connection, ExecutionContext executionContext)
					throws TranslatorException {
					List<Object> rows = new ArrayList(DATA.values());
        			return rows;
         	}

        };
        translator.start();

		
		return (InfinispanExecution) translator.createExecution(command, Mockito.mock(ExecutionContext.class), VDBUtility.RUNTIME_METADATA, CONNECTION);
	}
}
