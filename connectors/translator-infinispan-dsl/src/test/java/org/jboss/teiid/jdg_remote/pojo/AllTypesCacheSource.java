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
package org.jboss.teiid.jdg_remote.pojo;



import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.protostream.impl.parser.SquareProtoParser;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.ClassRegistry;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;

/**
 * Sample cache of objects
 * 
 * @author vhalbert
 * @param <K>
 * @param <V>
 *
 */
@SuppressWarnings({ "nls" })
public class AllTypesCacheSource<K, V>  implements RemoteCache<K, V>{
	
	public static final String ALLTYPES_CACHE_NAME = "AllTypesCache";
	
	public static final String ALLTYPES_CLASS_NAME = AllTypes.class.getName();
	
	public static Descriptor DESCRIPTOR;
	
	public static Random RANDOM = new Random();
	
	
	static ClassRegistry CLASS_REGISTRY = new ClassRegistry();
	
	private Map cache = Collections.synchronizedMap(new HashMap<Object, Object>());
	
	static {
		try {
			DESCRIPTOR = (Descriptor) createDescriptor();

			CLASS_REGISTRY.registerClass(AllTypes.class);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	public static InfinispanConnection createConnection() {
		return createConnection(true);
		
	}

		
	public static InfinispanConnection createConnection(final boolean useKeyClassType) {

		final Map <Object, Object> objects = AllTypesCacheSource.loadCache();

		return new InfinispanConnection() {

			@Override
			public String getPkField() throws TranslatorException {
				return "intKey";
			}

			@Override
			public Class<?> getCacheKeyClassType() throws TranslatorException {
				if (useKeyClassType) {
					return Integer.class;
				} 
				return null;
			}
			@Override
			public String getCacheName() throws TranslatorException {
				return AllTypesCacheSource.ALLTYPES_CACHE_NAME;
			}


			@Override
			public Class<?> getCacheClassType() throws TranslatorException {
				return AllTypes.class;
			}


			@Override
			public Descriptor getDescriptor() throws TranslatorException {
				return DESCRIPTOR;
			}


			@Override
			public Map<Object, Object> getCache() throws TranslatorException {
				return objects;
			}


			@Override
			public QueryFactory getQueryFactory() throws TranslatorException {
				return null;
			}

			
	        public ClassRegistry getClassRegistry() {
		        return AllTypesCacheSource.CLASS_REGISTRY;
		    }
		};
	}
	
	public static void loadCache(Map<Object, Object> cache) {
		
		for (int i = 1; i <= 10; i++) {
			
			AllTypes p = new AllTypes();			
			p.setIntKey(i);
			p.setIntNum(i+100);
			p.setStringKey("aaa");
			p.setStringNum( String.valueOf(RANDOM.nextInt()));
			p.setLongNum(RANDOM.nextLong());
			
			p.setBooleanValue(RANDOM.nextBoolean());
			
			
			p.setDoubleNum(RANDOM.nextDouble());
			
			p.setFloatNum(RANDOM.nextFloat());
			
			byte[] bytes = new byte[RANDOM.nextInt(100)];		
			p.setObjectValue(bytes);
			
			p.setCharValue((char)(RANDOM.nextInt(26) + 'a'));
			p.setShortValue((short) RANDOM.nextInt(Short.MAX_VALUE + 1));
				
			p.setDateValue(generateRandomDate());
			p.setTimeValue(generateRandomTime());
			p.setTimeStampValue(generateRandomTimeStamp());
			
			p.setBigIntegerValue(BigInteger.valueOf(RANDOM.nextLong()));
			p.setBigDecimalValue(BigDecimal.valueOf(RANDOM.nextLong()));
				
			cache.put(p.getIntKey(), p);


		}
	}
	
	private static Date generateRandomDate() {
		Date    dt;
		long    ms;

		// Get an Epoch value roughly between 1940 and 2010
		// -946771200000L = January 1, 1940
		// Add up to 70 years to it (using modulus on the next long)
		ms = -946771200000L + (Math.abs(RANDOM.nextLong()) % (70L * 365 * 24 * 60 * 60 * 1000));

		// Construct a date
		dt = new Date(ms);
		return dt;
	}
	
	private static Time generateRandomTime() {
		final int millisInDay = 24*60*60*1000;
		Time time = new Time((long)RANDOM.nextInt(millisInDay));
		return time;
	}
	
	private static Timestamp generateRandomTimeStamp() {
		long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
		long end = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
		long diff = end - offset + 1;
		Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
		return rand;
	}

	public static Map<Object, Object>  loadCache() {
		AllTypesCacheSource tcs = new AllTypesCacheSource();
		AllTypesCacheSource.loadCache(tcs);
		return tcs;
	}
	
	public List<Object> get(int key) {
		List<Object> objs = new ArrayList<Object>(1);
		objs.add(this.get(key));
		return objs;
	}	
	
	private static Descriptor createDescriptor() throws Exception {
		Configuration config = new Configuration.Builder().build();
		
		try {
			FileDescriptorSource fileDescriptorSource = FileDescriptorSource.fromResources(
	              "allTypes.proto");

	      Map<String, FileDescriptor> files = new SquareProtoParser(config).parseAndResolve(fileDescriptorSource);

	      FileDescriptor descriptor = files.get("allTypes.proto");
	      
	      String descriptorName = "org.jboss.qe.jdg_remote.protobuf.AllTypes";
	      Map<String, Descriptor> messages = new HashMap<String, Descriptor>();
	      for (Descriptor m : descriptor.getMessageTypes()) {
	         messages.put(m.getFullName(), m);
	         System.out.println("Descriptor Name: " + m.getFullName());
	      }

	      Descriptor testClass = messages.get(descriptorName);
	      
	      if (testClass == null) {
	    	  throw new Exception("Did not get descriptor: " + descriptorName );
	      }
	      
	      return testClass;
	      
	  } catch (Exception e) {
    	e.printStackTrace();
    	throw e;
	  }
		
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#getName()
	 */
	@Override
	public String getName() {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#getVersion()
	 */
	@Override
	public String getVersion() {
		return null;
	}

	/**
	 * 
	 *
	 * @param arg0 
	 * @param arg1 
	 * @return V
	 * @see org.infinispan.commons.api.BasicCache#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V put(K arg0, V arg1) {
		cache.put(arg0, arg1);
		return arg1;
	}
	

	/**
	 *
	 *
	 * @see org.infinispan.commons.api.BasicCache#put(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V put(K arg0, V arg1, long arg2, TimeUnit arg3) {
		return null;
	}

	/**
	 *
	 *
	 * @see org.infinispan.commons.api.BasicCache#put(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V put(K arg0, V arg1, long arg2, TimeUnit arg3, long arg4,
			TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#putIfAbsent(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V putIfAbsent(K arg0, V arg1, long arg2, TimeUnit arg3) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#putIfAbsent(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V putIfAbsent(K arg0, V arg1, long arg2, TimeUnit arg3, long arg4,
			TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#remove(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object arg0) {
		return (V) cache.remove(arg0);
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#replace(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V replace(K arg0, V arg1, long arg2, TimeUnit arg3) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#replace(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean replace(K arg0, V arg1, V arg2, long arg3, TimeUnit arg4) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#replace(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V replace(K arg0, V arg1, long arg2, TimeUnit arg3, long arg4,
			TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.BasicCache#replace(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean replace(K arg0, V arg1, V arg2, long arg3, TimeUnit arg4,
			long arg5, TimeUnit arg6) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#clearAsync()
	 */
	@Override
	public NotifyingFuture<Void> clearAsync() {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#getAsync(java.lang.Object)
	 */
	@Override
	public NotifyingFuture<V> getAsync(K arg0) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putAsync(java.lang.Object, java.lang.Object)
	 */
	@Override
	public NotifyingFuture<V> putAsync(K arg0, V arg1) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> putAsync(K arg0, V arg1, long arg2, TimeUnit arg3) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> putAsync(K arg0, V arg1, long arg2,
			TimeUnit arg3, long arg4, TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putIfAbsentAsync(java.lang.Object, java.lang.Object)
	 */
	@Override
	public NotifyingFuture<V> putIfAbsentAsync(K arg0, V arg1) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putIfAbsentAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> putIfAbsentAsync(K arg0, V arg1, long arg2,
			TimeUnit arg3) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#putIfAbsentAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> putIfAbsentAsync(K arg0, V arg1, long arg2,
			TimeUnit arg3, long arg4, TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#removeAsync(java.lang.Object)
	 */
	@Override
	public NotifyingFuture<V> removeAsync(Object arg0) {
		this.cache.remove(arg0);
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#removeAsync(java.lang.Object, java.lang.Object)
	 */
	@Override
	public NotifyingFuture<Boolean> removeAsync(Object arg0, Object arg1) {
		this.cache.remove(arg0);
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object)
	 */
	@Override
	public NotifyingFuture<V> replaceAsync(K arg0, V arg1) {
		this.cache.put(arg0, arg1);
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceAsync(K arg0, V arg1, V arg2) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> replaceAsync(K arg0, V arg1, long arg2,
			TimeUnit arg3) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceAsync(K arg0, V arg1, V arg2,
			long arg3, TimeUnit arg4) {
		return null;
	}

	/**
	 *  
	 *
	 * @param arg0 
	 * @param arg1 
	 * @param arg2 
	 * @param arg3 
	 * @param arg4 
	 * @param arg5 
	 * @return NotifyingFuture
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<V> replaceAsync(K arg0, V arg1, long arg2,
			TimeUnit arg3, long arg4, TimeUnit arg5) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.AsyncCache#replaceAsync(java.lang.Object, java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceAsync(K arg0, V arg1, V arg2,
			long arg3, TimeUnit arg4, long arg5, TimeUnit arg6) {
		return null;
	}

	/**
	 *  
	 *
	 * @see java.util.concurrent.ConcurrentMap#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V putIfAbsent(K arg0, V arg1) {
		return null;
	}

	/**
	 *  
	 *
	 * @see java.util.concurrent.ConcurrentMap#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(Object arg0, Object arg1) {
		boolean exist = cache.containsKey(arg0);
		if (exist) cache.remove(arg0);
		return exist;
	}

	/**
	 *  
	 *
	 * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V replace(K arg0, V arg1) {
		cache.put(arg0, arg1);
		return arg1;
	}

	/**
	 *  
	 *
	 * @see java.util.concurrent.ConcurrentMap#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(K arg0, V arg1, V arg2) {
		return false;
	}

	/**
	 *  
	 *
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		cache.clear();
	}

	/**
	 *  
	 *
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(Object arg0) {
		return cache.containsKey(arg0);
	}

	/**
	 *  
	 *
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public V get(Object arg0) {
		return (V) cache.get(arg0);
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.Lifecycle#start()
	 */
	@Override
	public void start() {
	}

	/**
	 *  
	 *
	 * @see org.infinispan.commons.api.Lifecycle#stop()
	 */
	@Override
	public void stop() {
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#removeWithVersion(java.lang.Object, long)
	 */
	@Override
	public boolean removeWithVersion(K key, long version) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#removeWithVersionAsync(java.lang.Object, long)
	 */
	@Override
	public NotifyingFuture<Boolean> removeWithVersionAsync(K key, long version) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersion(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public boolean replaceWithVersion(K key, V newValue, long version) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersion(java.lang.Object, java.lang.Object, long, int)
	 */
	@Override
	public boolean replaceWithVersion(K key, V newValue, long version,
			int lifespanSeconds) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersion(java.lang.Object, java.lang.Object, long, int, int)
	 */
	@Override
	public boolean replaceWithVersion(K key, V newValue, long version,
			int lifespanSeconds, int maxIdleTimeSeconds) {
		return false;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersionAsync(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue,
			long version) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersionAsync(java.lang.Object, java.lang.Object, long, int)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue,
			long version, int lifespanSeconds) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#replaceWithVersionAsync(java.lang.Object, java.lang.Object, long, int, int)
	 */
	@Override
	public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue,
			long version, int lifespanSeconds, int maxIdleSeconds) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getVersioned(java.lang.Object)
	 */
	@Override
	public VersionedValue<V> getVersioned(K key) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getWithMetadata(java.lang.Object)
	 */
	@Override
	public MetadataValue<V> getWithMetadata(K key) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#size()
	 */
	@Override
	public int size() {
		return cache.size();
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return cache.isEmpty();
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		return cache.containsValue(value);
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#keySet()
	 */
	@Override
	public Set<K> keySet() {
		return cache.keySet();
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#values()
	 */
	@Override
	public Collection<V> values() {
		return cache.values();
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return cache.entrySet();
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map, long lifespan,
			TimeUnit unit) {
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAll(java.util.Map, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map, long lifespan,
			TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAllAsync(java.util.Map)
	 */
	@Override
	public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAllAsync(java.util.Map, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<Void> putAllAsync(
			Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAllAsync(java.util.Map, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public NotifyingFuture<Void> putAllAsync(
			Map<? extends K, ? extends V> data, long lifespan,
			TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#stats()
	 */
	@Override
	public ServerStatistics stats() {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#withFlags(org.infinispan.client.hotrod.Flag[])
	 */
	@Override
	public RemoteCache<K, V> withFlags(Flag... flags) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getRemoteCacheManager()
	 */
	@Override
	public RemoteCacheManager getRemoteCacheManager() {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getBulk()
	 */
	@Override
	public Map<K, V> getBulk() {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getBulk(int)
	 */
	@Override
	public Map<K, V> getBulk(int size) {
		return null;
	}

	/**
	 *  
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getProtocolVersion()
	 */
	@Override
	public String getProtocolVersion() {
		return null;
	}

	@Override
	public void addClientListener(Object arg0, Object[] arg1, Object[] arg2) {
	}

	@Override
	public void addClientListener(Object arg0) {
	}

	@Override
	public Set<Object> getListeners() {
		return null;
	}

	@Override
	public void removeClientListener(Object arg0) {
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.infinispan.client.hotrod.RemoteCache#getAll(java.util.Set)
	 */
	@Override
	public Map<K, V> getAll(Set<? extends K> keys) {
		return null;
	}


    @Override
    public <T> T execute(String arg0, Map<String, ?> arg1) {
        return null;
    }
}
