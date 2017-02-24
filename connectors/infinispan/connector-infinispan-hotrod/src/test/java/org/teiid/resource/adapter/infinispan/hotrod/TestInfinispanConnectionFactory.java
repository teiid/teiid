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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.jboss.teiid.jdg_remote.pojo.AllTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.CommandContext;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory.InfinispanConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;



@SuppressWarnings("nls")
public class TestInfinispanConnectionFactory  {
	protected static final String JNDI_NAME = "java/MyCacheManager";


	private static InfinispanManagedConnectionFactory afactory;
	
	@Mock
	private ExecutionContext context;
	
	@Mock
	private CommandContext comcontext;
	

	
	@Before
    public void beforeEach() throws Exception {  
		MockitoAnnotations.initMocks(this);
		
		 when(context.getCommandContext()).thenReturn(comcontext);
		 when(comcontext.getVDBClassLoader()).thenReturn(this.getClass().getClassLoader());

  
        afactory = new InfinispanManagedConnectionFactory();
 //            {
//        	@Override
//        	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
//        			throws ResourceException {
//        		
//        		validation();
//        		
//        		this.setClassLoader();
//        		
//        		return new InfinispanConnectionFactory(this);
//
////        		validation();
////        		loadClasses(this.getClass().getClassLoader());
////        		super.loadClasses();
//   //     		return null;
//        	}
//			/**
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public SerializationContext getContext() {
//				// TODO Auto-generated method stub
//				return null;
//			}
//
//			@Override
//			public RemoteCacheManager createRemoteCacheFromProperties(
//					ClassLoader classLoader) throws ResourceException {
//				// TODO Auto-generated method stub
//				return null;
//			}
//
//			@Override
//			public RemoteCacheManager createRemoteCacheFromServerList(
//					ClassLoader classLoader) throws ResourceException {
//				// TODO Auto-generated method stub
//				return null;
//			}
//			
//			@Override
//			protected void registerWithCacheManager() throws ResourceException {
//				// don't call JDG
//			}			
//
//		};
		
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
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey:" + Integer.class.getName());
		afactory.setHotRodClientPropertiesFile("./src/test/resources/jdg.properties");
		afactory.setTrustStoreFileName("/the/trustore/filename");
		afactory.setTrustStorePassword( "abc");
		afactory.setSNIHostName("sniHostName");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheNameProxy().getPrimaryCacheKey());
		
		assertEquals("CacheKeyTypeClass not the same", afactory.getCacheKeyClassType(), Integer.class);
		assertEquals("/the/trustore/filename", afactory.getTrustStoreFileName());
		assertEquals("abc", afactory.getTrustStorePassword());
		assertEquals("sniHostName", afactory.getSNIHostName());
	}
	
	 /**
	 * TEIID-4582 support all colon delimiters
     * @throws Exception
     */
	@Test public void testtCacheTypeMap11a() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes:intKey:" + Integer.class.getName());
		afactory.setHotRodClientPropertiesFile("");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheNameProxy().getPrimaryCacheKey());
		
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
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey:" + String.class.getName());
		afactory.setHotRodClientPropertiesFile("./src/test/resources/jdg.properties");
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", "AllTypesCache", afactory.getCacheNameProxy().getPrimaryCacheKey());
		
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
		String aliasCacheName = "AliasCacheName";
		String stagingCacheName = "StagingCacheName";

		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setHotRodClientPropertiesFile("./src/test/resources/jdg.properties");
		afactory.setAliasCacheName(aliasCacheName);
		afactory.setStagingCacheName(stagingCacheName);
				
	    afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", aliasCacheName, afactory.getCacheNameProxy().getAliasCacheName());
		assertEquals("Cache name not the same", stagingCacheName, afactory.getCacheNameProxy().getStageCacheKey());
		
	}

	 /**
     * testCacheTypeMapNoCacheKeytype:
     * - defining only the cache key
     * - 
     * @throws Exception
     */
	@Test public void testCacheTypeMapNoCacheKeytype() throws Exception {
		String aliasCacheName = "AliasCacheName";
		String stagingCacheName = "StagingCacheName";

		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes;intKey");
		afactory.setHotRodClientPropertiesFile("");
		afactory.setAliasCacheName(aliasCacheName);
		afactory.setStagingCacheName(stagingCacheName);
		
		
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", aliasCacheName, afactory.getCacheNameProxy().getAliasCacheName());
		assertEquals("Cache name not the same", stagingCacheName, afactory.getCacheNameProxy().getStageCacheKey());

		assertNull(afactory.getCacheKeyClassType());
	}
	
	/**
	 * Test all colon delimiter for cacheTypeMap
	 * @throws Exception
	 */
	@Test public void testCacheTypeMapNoCacheKeytypeB() throws Exception {
		String aliasCacheName = "AliasCacheName";
		String stagingCacheName = "StagingCacheName";
		
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes:intKey");
		afactory.setHotRodClientPropertiesFile("");
		afactory.setAliasCacheName(aliasCacheName);
		afactory.setStagingCacheName(stagingCacheName);
				
		afactory.createConnectionFactory().getConnection();
		
		assertEquals("CacheClass Type not the same", AllTypes.class, afactory.getCacheClassType());
		assertEquals("Cache name not the same", aliasCacheName, afactory.getCacheNameProxy().getAliasCacheName());
		assertEquals("Cache name not the same", stagingCacheName, afactory.getCacheNameProxy().getStageCacheKey());
		
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
	
	 /**
     * Test invalid property - TEIID25033=Truststore filename and password must be specified
     * - 
     * @throws Exception
     */
	@Test( expected = InvalidPropertyException.class )
	public void testInvalidTruststoreSetting() throws Exception {
		afactory.setProtobufDefinitionFile("allTypes.proto");
		afactory.setMessageMarshallers("org.jboss.teiid.jdg_remote.pojo.AllTypes:org.jboss.teiid.jdg_remote.pojo.marshaller.AllTypesMarshaller");
		afactory.setMessageDescriptor("org.jboss.teiid.jdg_remote.pojo.AllTypes");
		afactory.setCacheTypeMap("AllTypesCache:org.jboss.teiid.jdg_remote.pojo.AllTypes:intKey");
		afactory.setHotRodClientPropertiesFile("");
		
		afactory.setTrustStoreFileName("filename");
		
		
		afactory.createConnectionFactory().getConnection();
		
	}



}
