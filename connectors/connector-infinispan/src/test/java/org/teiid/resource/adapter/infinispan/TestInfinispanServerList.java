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

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.object.ObjectConnection;


@SuppressWarnings("nls")
@Ignore
public class TestInfinispanServerList {
	    private InfinispanManagedConnectionFactory factory = null;
			
		@BeforeClass
	    public static void beforeEachClass() throws Exception { 
			RemoteInfinispanTestHelper.createServer();
	
		}

		@Before public void beforeEachTest() throws Exception{	
	        
			factory = new InfinispanManagedConnectionFactory();

			factory.setRemoteServerList(RemoteInfinispanTestHelper.hostAddress() + ":" + RemoteInfinispanTestHelper.hostPort());
			factory.setCacheTypeMap(RemoteInfinispanTestHelper.CACHE_NAME + ":" + "java.lang.String");
	    }
		
	    @AfterClass
	    public static void closeConnection() throws Exception {
	        RemoteInfinispanTestHelper.releaseServer();
	    }

	    @Test
	    public void testRemoteConnection() throws Exception {
	    	
	    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
	    		Map<?, ?> m = conn.getMap(RemoteInfinispanTestHelper.CACHE_NAME);
	    	
	    		assertNotNull(m);
	    		
	    		Class<?> t = conn.getType(RemoteInfinispanTestHelper.CACHE_NAME);
	    		
	    		 assertEquals(String.class, t);
	    }


		


}
