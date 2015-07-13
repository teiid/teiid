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
package org.teiid.translator.infinispan.dsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.jboss.teiid.jdg_remote.pojo.AllTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.resource.adapter.infinispan.dsl.base.AbstractInfinispanManagedConnectionFactory;


@SuppressWarnings("nls")
public class TestAbstractInfinispanConnectionFactory  {
	protected static final String JNDI_NAME = "java/MyCacheManager";


	private static AbstractInfinispanManagedConnectionFactory afactory;
	
	@Before
    public void beforeEach() throws Exception {  
  
        afactory = new AbstractInfinispanManagedConnectionFactory() {
			/**
			 */
			private static final long serialVersionUID = 1L;

			@Override
			protected SerializationContext getContext() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			protected RemoteCacheManager createRemoteCacheWrapperFromProperties(
					ClassLoader classLoader) throws ResourceException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			protected RemoteCacheManager createRemoteCacheWrapperFromServerList(
					ClassLoader classLoader) throws ResourceException {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			protected void registerMarshallers(SerializationContext ctx, ClassLoader cl) throws ResourceException {
			
			}

		};
		
	}	
	
    @SuppressWarnings("unchecked")
	@After
    public void closeConnection() throws Exception {
 
      	afactory.cleanUp();

    }
    
    /**
     * Test CacheTypeMap 1:
     * - cache key type is the same
     * - 
     * @throws Exception
     */
	@Test public void testtCacheTypeMap1() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey:" + Integer.class.getName());
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheName());
		
		assertEquals("CacheKeyTypeClass not the same", afactory.getCacheKeyClassType(), Integer.class);
	}
	
	 /**
     * Test tCacheTypeMap 2:
     * - cache key type is different, meaning transformation will be needed for updates
     * - 
     * @throws Exception
     */
	@Test public void testCacheTypeMap2() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey:" + String.class.getName());
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheName());
		
		// should not be equal because the cache key type is not the same as its method return value for getIntKey
		assertNotEquals("CacheKeyClass Type not the same", Integer.class, afactory.getCacheKeyClassType());
	}
	
	 /**
     * Test tCacheTypeMap 3:
     * - CacheTypeMap doesn't define the key or cache key type.  Used in cases where no updates are being done
     * - 
     * @throws Exception
     */
	//  @Test( expected = SQLException.class )
	@Test public void testCacheTypeMap3() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheName());
		
	}

	 /**
     * testCacheTypeMapNoCacheKeytype:
     * - defining only the cache key
     * - 
     * @throws Exception
     */
	@Test public void testCacheTypeMapNoCacheKeytype() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey");
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheName());
		
		assertNull(afactory.getCacheKeyClassType());
	}
	
	 /**
     * Test tCacheTypeMap 3:
     * - CacheTypeMap doesn't define the key or cache key type.  Used in cases where no updates are being done
     * - 
     * @throws Exception
     */
	@Test( expected = InvalidPropertyException.class )
	public void testInvalidCacheTypeMap() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache");
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
	}	


}
