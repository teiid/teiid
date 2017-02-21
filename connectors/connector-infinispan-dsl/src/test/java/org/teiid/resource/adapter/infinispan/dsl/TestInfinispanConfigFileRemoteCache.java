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

import java.io.File;
import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Address;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PhoneNumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.translator.infinispan.dsl.InfinispanDSLConnection;
import org.teiid.translator.object.Version;



@SuppressWarnings("nls")
public class TestInfinispanConfigFileRemoteCache {
    
    private static InfinispanManagedConnectionFactory factory = null;
	
    private static RemoteInfinispanTestHelper REMOTEHELPER = new RemoteInfinispanTestHelper();

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		REMOTEHELPER.startServer();
		
  		// read in the properties template file and set the server host:port and then save for use
  		File f = new File("./src/test/resources/jdg.properties");
  		
  		Properties props = PropertiesUtils.load(f.getAbsolutePath());
  		props.setProperty("infinispan.client.hotrod.server_list", REMOTEHELPER.hostAddress() + ":" + REMOTEHELPER.hostPort());
		
  		PropertiesUtils.print("./target/hotrod-client.properties", props);

		factory = new InfinispanManagedConnectionFactory();

		factory.setHotRodClientPropertiesFile("./target/hotrod-client.properties");
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.PERSON_CACHE_NAME + ":" + RemoteInfinispanTestHelper.PERSON_CLASS.getName()+ ";" + RemoteInfinispanTestHelper.PKEY_COLUMN);

		
	}
	
	@AfterClass
    public static void closeConnection() throws Exception {
		REMOTEHELPER.releaseServer();
    }

	
    @Test
    public void testConnection() throws Exception {
    	try {
    		InfinispanDSLConnection conn = factory.createConnectionFactory().getConnection();
    		
    		Class<?> clz = conn.getCacheClassType();
    
    		assertEquals(RemoteInfinispanTestHelper.PERSON_CLASS, clz);
    		
    		Class<?> t = conn.getCacheKeyClassType();
    		
    		 assertNull(t);
    		 
    		 assertEquals(RemoteInfinispanTestHelper.PKEY_COLUMN, conn.getPkField());
    		
    		 assertNotNull(conn.getCache());
    		 
    		 assertEquals("Support For Compare is false", conn.getVersion().compareTo(Version.getVersion("6.6")) >= 0, false );
 //   		 assertEquals("Version doesn't start with 7.2", conn.getVersion().startsWith("7.2"));

    		 assertNotNull(conn.getDescriptor(Address.class));
    		 assertNotNull(conn.getDescriptor(PhoneNumber.class));
    		 
    		 conn.cleanUp();
    		 
    	} finally {
	    	factory.cleanUp();
	    	REMOTEHELPER.releaseServer();
    	}
    }
}
