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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.testdata.trades.Trade;

@SuppressWarnings("nls")
public class TestInfinispanManagedConnectionFactory {
	
    @Test
    public void testCacheTypeMap1() throws Exception {
    	InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
 
		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TRADE_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade");
    	   	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		Class<?> clz = conn.getCacheClassType();
    		
    		assertEquals(Trade.class, clz);
    		
    		assertNull(conn.getCacheKeyClassType());
    		
    		assertNull(conn.getPkField());
    		
    		assertEquals(RemoteInfinispanTestHelper.TRADE_CACHE_NAME, conn.getCacheName());
    		assertNotNull(conn.getClassRegistry());
     		
    		factory.cleanUp();
    }
    
    @Test
    public void testCacheTypeMap2() throws Exception {
    	InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
 
		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TEST_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade;longValue");
    	
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		
    		assertEquals(conn.getCacheName(), RemoteInfinispanTestHelper.TEST_CACHE_NAME);

    		Class<?> clz = conn.getCacheClassType();
    		
    		assertEquals(Trade.class, clz);
    		    		
    		assertNull(conn.getCacheKeyClassType());
    		
    		assertEquals("longValue", conn.getPkField());
    		
    		factory.cleanUp();
    }   
    
    @Test
    public void testCacheTypeMap3() throws Exception {
    	InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
 
		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TEST_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade;longValue:long");
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		assertEquals(conn.getCacheName(), RemoteInfinispanTestHelper.TEST_CACHE_NAME);

    		Class<?> clz = conn.getCacheClassType();
    		
    		assertEquals(Trade.class, clz);
    		
    		Class<?> t = conn.getCacheKeyClassType();
    		
    		assertEquals(long.class, t);
    		
    		assertEquals("longValue", conn.getPkField());
    		
    		factory.cleanUp();
    }   
    
    @Test
    public void testCacheTypeMap4() throws Exception {
    	InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
 
		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TEST_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade;longValue:java.lang.Long");
    	
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		
    		assertEquals(conn.getCacheName(), RemoteInfinispanTestHelper.TEST_CACHE_NAME);
    		
    		Class<?> clz = conn.getCacheClassType();
    		
    		assertEquals(Trade.class, clz);
    		
    		Class<?> t = conn.getCacheKeyClassType();
    		
    		assertEquals(Long.class, t);
    		
    		assertEquals("longValue", conn.getPkField());
    		
    		factory.cleanUp();
    }          
}
