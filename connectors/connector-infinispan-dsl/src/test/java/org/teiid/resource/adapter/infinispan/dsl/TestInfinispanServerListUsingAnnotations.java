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

package org.teiid.resource.adapter.infinispan.dsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.object.ObjectConnection;



@SuppressWarnings("nls")
public class TestInfinispanServerListUsingAnnotations {
	    private static InfinispanManagedConnectionFactory factory = null;
	    private static RemoteInfinispanTestHelper REMOTEHELPER = new RemoteInfinispanTestHelper();

		@BeforeClass
	    public static void beforeEachClass() throws Exception {  
			REMOTEHELPER.startServer();
			
			factory = new InfinispanManagedConnectionFactory();

			factory.setRemoteServerList(REMOTEHELPER.hostAddress() + ":" + REMOTEHELPER.hostPort());
			factory.setCacheTypeMap(RemoteInfinispanTestHelper.PERSON_CACHE_NAME + ":" + RemoteInfinispanTestHelper.PERSON_CLASS.getName()+ ";" + RemoteInfinispanTestHelper.PKEY_COLUMN);
			
		}
		
		
		@AfterClass
	    public static void closeConnection() throws Exception {
			REMOTEHELPER.releaseServer();
	    }

	    @Test
	    public void testRemoteConnection() throws Exception {
	    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
	    		Class<?> clz = conn.getCacheClassType();
	    
	    		assertEquals(RemoteInfinispanTestHelper.PERSON_CLASS, clz);
	    		
	    		Class<?> t = conn.getCacheKeyClassType();
	    		
	    		 assertNull(t);
	    		 
	    		 assertEquals(RemoteInfinispanTestHelper.PKEY_COLUMN, conn.getPkField());
	    		
	    		 assertNotNull(conn.getCache());
	    		 
	    		 conn.cleanUp();
	    		 
	    }
}
