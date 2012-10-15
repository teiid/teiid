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

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.translator.object.ObjectConnection;

@SuppressWarnings("nls")
public class TestInfinispanConfigFileRemoteCache {
    
    private static InfinispanManagedConnectionFactory factory = null;
		

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		RemoteInfinispanTestHelper.createServer();
		
  		System.out.println("Hostaddress " + RemoteInfinispanTestHelper.hostAddress());
  		
  		// read in the properties template file and set the server host:port and then save for use
  		File f = new File("./src/test/resources/hotrod-client.properties");
  		
  		Properties props = PropertiesUtils.load(f.getAbsolutePath());
  		props.setProperty("infinispan.client.hotrod.server_list", RemoteInfinispanTestHelper.hostAddress() + ":" + RemoteInfinispanTestHelper.hostPort());
		
  		PropertiesUtils.print("./target/hotrod-client.properties", props);

		factory = new InfinispanManagedConnectionFactory();

		factory.setHotRodClientPropertiesFile("./target/hotrod-client.properties");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.CACHE_NAME + ":" + "java.lang.String");

	}
	
    @AfterClass
    public static void closeConnection() throws Exception {
        RemoteInfinispanTestHelper.releaseServer();
    }

	
    @Test
    public void testConnection() throws Exception {
    	
    		ObjectConnection conn = factory.createConnectionFactory().getConnection();
    		Map<?, ?> m = conn.getMap(RemoteInfinispanTestHelper.CACHE_NAME);
    	
    		assertNotNull(m);
    		
    		Class<?> t = conn.getType(RemoteInfinispanTestHelper.CACHE_NAME);
    		
    		assertEquals(String.class, t);
    }
}
