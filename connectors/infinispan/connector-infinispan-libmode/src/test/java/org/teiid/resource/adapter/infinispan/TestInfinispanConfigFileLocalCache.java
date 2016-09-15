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
package org.teiid.resource.adapter.infinispan;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.infinispan.libmode.BasicAnnotatedSearchTest;
import org.teiid.translator.infinispan.libmode.InfinispanCacheExecutionFactory;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.trades.Trade;
import org.teiid.translator.object.testdata.trades.VDBUtility;

@SuppressWarnings("nls")
@Ignore
public class TestInfinispanConfigFileLocalCache extends BasicAnnotatedSearchTest {	
    
    private static InfinispanManagedConnectionFactory factory = null;
	private static ExecutionContext context;
	private static ObjectConnection CONNECTION;
	
	private static InfinispanCacheExecutionFactory TRANS_FACTORY = null;

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		context = mock(ExecutionContext.class);

		factory = new InfinispanManagedConnectionFactory();

		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(InfinispanTestHelper.TRADE_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade;longValue:long");

		
		TRANS_FACTORY = new InfinispanCacheExecutionFactory();
		TRANS_FACTORY.start();
		
		CONNECTION = factory.createConnectionFactory().getConnection();
//		CONNECTION = TestInfinispanConnectionHelper.createConnection(false, "org.teiid.resource.adapter.infinispan.local.LocalCacheConnection");

	}
	
	@AfterClass
    public static void closeConnection() throws Exception {    	    
    	    
		factory.shutDownCache();
    }
	
	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) TRANS_FACTORY.createExecution(command, context, VDBUtility.RUNTIME_METADATA, CONNECTION);
	}	
	
    @Test
    public void testConnection() throws Exception {
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		Class<?> clz = conn.getCacheClassType();
    		
    		assertEquals(Trade.class, clz);
    		
    		Class<?> t = conn.getCacheKeyClassType();
    		
    		assertEquals(long.class, t);
    		
    		assertEquals("longValue", conn.getPkField());
    		
    		conn.cleanUp();
    }
}
