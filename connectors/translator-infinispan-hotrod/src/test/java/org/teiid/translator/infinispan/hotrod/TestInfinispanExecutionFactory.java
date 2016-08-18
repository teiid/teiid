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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.person.PersonSchemaVDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanExecutionFactory {
	
	@SuppressWarnings("deprecation")
	protected static InfinispanExecutionFactory TRANSLATOR;
	protected static InfinispanHotRodConnection CONNECTION;

	
	@Mock
	private ExecutionContext context;
	
	@Mock
	private Select command;

	@Before public void beforeEach() throws Exception{	

		MockitoAnnotations.initMocks(this);
    }

	@Test public void testFactory() throws Exception {
	   	CONNECTION = PersonCacheSource.createConnection(true);
    	TestInfinispanHotRodConnection conn = (TestInfinispanHotRodConnection) CONNECTION;
    	conn.setVersion("6.6");
    	
        TRANSLATOR = new InfinispanExecutionFactory();
        TRANSLATOR.initCapabilities(CONNECTION);
        TRANSLATOR.start();

        assertTrue(TRANSLATOR.supportsCompareCriteriaOrdered());
        
		ObjectExecution exec = (ObjectExecution) TRANSLATOR.createExecution(command, context, PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
		
		assertNotNull(exec);
		assertNotNull(TRANSLATOR.getMetadataProcessor());
	}	
	
	@Test public void testFactoryVersion7() throws Exception {
	   	CONNECTION = PersonCacheSource.createConnection(true);
    	TestInfinispanHotRodConnection conn = (TestInfinispanHotRodConnection) CONNECTION;
    	conn.setVersion("7.2.3");
    	
        TRANSLATOR = new InfinispanExecutionFactory();
        TRANSLATOR.initCapabilities(CONNECTION);
        TRANSLATOR.start();
        
        assertTrue(TRANSLATOR.supportsCompareCriteriaOrdered());


		ObjectExecution exec = (ObjectExecution) TRANSLATOR.createExecution(command, context, PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
		
		assertNotNull(exec);
		assertNotNull(TRANSLATOR.getMetadataProcessor());
	}	
	
	@Test public void testFactoryVersion65() throws Exception {
	   	CONNECTION = PersonCacheSource.createConnection(true);
    	TestInfinispanHotRodConnection conn = (TestInfinispanHotRodConnection) CONNECTION;
    	conn.setVersion("6.5");
    	
        TRANSLATOR = new InfinispanExecutionFactory();
        TRANSLATOR.initCapabilities(CONNECTION);
        TRANSLATOR.start();
        
        assertTrue(!TRANSLATOR.supportsCompareCriteriaOrdered());


		ObjectExecution exec = (ObjectExecution) TRANSLATOR.createExecution(command, context, PersonSchemaVDBUtility.RUNTIME_METADATA, CONNECTION);
		
		assertNotNull(exec);
		assertNotNull(TRANSLATOR.getMetadataProcessor());
	}	
	
	
}
