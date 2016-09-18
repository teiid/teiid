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
package org.teiid.translator.infinispan.libmode;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.BasicSearchTest;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.trades.VDBUtility;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestInfinispanConfigFileKeySearch extends BasicSearchTest {

	private static ExecutionContext context;

    private static InfinispanCacheExecutionFactory factory = null;
    
    private static ObjectConnection conn;

	@BeforeClass
    public static void beforeClass() throws Exception {  
        // Set up the mock JNDI ...
   		context = mock(ExecutionContext.class);
   		
   		Version v = Version.getVersion("10.1");
		
		conn = TestInfinispanConnectionHelper.createNonAnnotatedConnection("./src/test/resources/infinispan_persistent_indexing_config.xml", v);

	}
	
	@AfterClass
	public static void afterClass() {
		
		conn.cleanUp();
	}	
	
	@Before public void beforeEachTest() throws Exception{	
		factory = new InfinispanCacheExecutionFactory();
		factory.start();
		
		factory.initCapabilities(conn);
    }
	
	@Override
	protected ObjectExecution createExecution(Select command) throws TranslatorException {
		
		assertEquals("supportsCompareCriteriaOrdered was not configurred correctly ", factory.supportsCompareCriteriaOrdered(), true); //$NON-NLS-1$

		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, conn);
	}
	
}
