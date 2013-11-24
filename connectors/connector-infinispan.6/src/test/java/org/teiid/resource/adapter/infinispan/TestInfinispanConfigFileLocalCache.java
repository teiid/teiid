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

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.translator.object.CacheContainerWrapper;
import org.teiid.translator.object.ObjectConnection;

@SuppressWarnings("nls")
public class TestInfinispanConfigFileLocalCache {
    
    private static InfinispanManagedConnectionFactory factory = null;
    
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
 		
		factory = new InfinispanManagedConnectionFactory();

		factory.setConfigurationFileNameForLocalCache("./src/test/resources/infinispan_persistent_config.xml");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TEST_CACHE_NAME + ":" + "java.lang.Long;longValue");
		
	}
	
    @AfterClass
    public static void closeConnection() throws Exception {    	    
    	    CacheContainerWrapper ccw = factory.getCacheContainer();
    	    
    	    ReflectionHelper h = new ReflectionHelper(ccw.getClass());
    	    
    	    (h.findBestMethodWithSignature("cleanUp", Collections.EMPTY_LIST)).invoke(ccw);
    	    
    	    factory.cleanUp();
    	    
        RemoteInfinispanTestHelper.releaseServer();
    }
	
    @Test
    public void testConnection() throws Exception {
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		Map<?, ?> m = conn.getCacheNameClassTypeMapping();
    		
    		assertNotNull(m);
    		
    		Class<?> t = conn.getType(RemoteInfinispanTestHelper.TEST_CACHE_NAME);
    		
    		assertEquals(Long.class, t);
    		
    		assertEquals("longValue", conn.getPkField(RemoteInfinispanTestHelper.TEST_CACHE_NAME));

    }
}
