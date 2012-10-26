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

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectExecutionFactory {
	
	public class TestFactory extends ObjectExecutionFactory {
		public TestFactory() {
			
		}

	}
	
	@Mock
	private ExecutionContext context;
	
	@Mock
	private Select command;
	
	private ObjectExecutionFactory factory;

	@Before public void beforeEach() throws Exception{	
 
		MockitoAnnotations.initMocks(this);
		
		factory = new TestFactory();
    }

	@Test public void testFactory() throws Exception {
		factory.start();
			
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		assertNotNull(exec);
	}	
	
	@Test public void testGetMetadata() throws Exception {
		
		MetadataFactory mfactory = new MetadataFactory("TestVDB", 1, "Trade",  SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		
		factory.start();
		
		factory.getMetadata(mfactory, null);

	}

}
