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
package org.teiid.resource.adapter.infinispan.hotrod;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.translator.object.ObjectConnection;


@SuppressWarnings("nls")
@Ignore
public class TestInfinispanConfigFileRemoteCache {
    
    private static InfinispanManagedConnectionFactory factory = null;
		

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		RemoteInfinispanTestHelper.startServer();
		
  		System.out.println("Hostaddress " + RemoteInfinispanTestHelper.hostAddress());
  		
  		// read in the properties template file and set the server host:port and then save for use
  		File f = new File("./src/test/resources/jdg.properties");
  		
  		Properties props = PropertiesUtils.load(f.getAbsolutePath());
  		props.setProperty("infinispan.client.hotrod.server_list", RemoteInfinispanTestHelper.hostAddress() + ":" + RemoteInfinispanTestHelper.hostPort());
		
  		PropertiesUtils.print("./target/jdg.properties", props);

		factory = new InfinispanManagedConnectionFactory();

		factory.setHotRodClientPropertiesFile("./target/hotrod-client.properties");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.PERSON_CACHE_NAME + ":" + RemoteInfinispanTestHelper.PERSON_CLASS.getName()+ ";" + RemoteInfinispanTestHelper.PKEY_COLUMN);

		
	}
	
	@AfterClass
    public static void closeConnection() throws Exception {
          RemoteInfinispanTestHelper.releaseServer();
    }

	
    @Test
    public void testConnection() throws Exception {
    	try {
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		Class<?> clz = conn.getCacheClassType();
    
    		assertEquals(RemoteInfinispanTestHelper.PERSON_CLASS, clz);
    		
    		Class<?> t = conn.getCacheKeyClassType();
    		
    		 assertNull(t);
    		 
    		 assertEquals(RemoteInfinispanTestHelper.PKEY_COLUMN, conn.getPkField());
    		
    		 assertNotNull(conn.getCache());
    		 
    			
    		 assertEquals("Version doesn't start with 7.2", conn.getVersion().startsWith("7.2"));

    		 
    		 conn.cleanUp();
    		 
    	} finally {
	    	factory.cleanUp();
	    	RemoteInfinispanTestHelper.releaseServer();
    	}
    }
}
