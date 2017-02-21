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
package org.teiid.resource.adapter.infinispan.remote;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.query.dsl.QueryFactory;
import org.jboss.marshalling.ContextClassResolver;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper;
import org.teiid.resource.adapter.infinispan.InfinispanManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.SearchType;
import org.teiid.translator.object.Version;
import org.teiid.translator.object.simpleMap.SearchByKey;


/**
 * This wrapper will contain a remote Infinispan cache.  It will also perform the task 
 * of connecting to the cache by either:
 * <li>{@link #createUsingServerList() Server List (host:port,..)}
 * <li>{@link #createUsingPropertiesFile() Server Property File}
 * 
 * @author vanhalbert
 * @param <K> 
 * @param <V> 
 *
 */
public class RemoteCacheConnection<K,V>  extends InfinispanCacheWrapper<K,V> {
	private RemoteCacheManager rcm;
	private InfinispanManagedConnectionFactory config;

	@Override
	public InfinispanManagedConnectionFactory getConfig() {
		return config;
	}
	
	@Override
	public void init(InfinispanManagedConnectionFactory config, Object cacheManager) {
		
		
		this.config = config;
		rcm = (RemoteCacheManager) cacheManager;
	}

	@Override
	public  void init(InfinispanManagedConnectionFactory config) throws ResourceException {
		
		this.config = config;
		
		if (getConfig().getHotRodClientPropertiesFile() != null) {
			rcm = createUsingPropertiesFile();
		} else if (getConfig().getRemoteServerList() != null) {
			rcm = createUsingServerList();
		} else {
			if (rcm == null) {
				throw new ResourceException("Program Logic Error: DefaultCacheManager was not configured");
			}
		}	
	}
	
	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	@Override
	public boolean isAlive() {		
		if (rcm == null) return false;
		return (rcm.isStarted());
	}	

	@Override
	public List<Object> getAll() {
		@SuppressWarnings("rawtypes")
		RemoteCache cache = getCache();

		Map<Object, Object> c = cache.getBulk();
		List<Object> results = new ArrayList<Object>();
		for (Object k:c.keySet()) {
			Object v = cache.get(k);
			results.add(v);
			
		}

		return results;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public RemoteCache getCache() {
		return getCache(getTargetCacheName());
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public RemoteCache getCache(String cacheName) {
		return rcm.getCache(cacheName);
	}	

	private RemoteCacheManager createUsingPropertiesFile() throws ResourceException {

		File f = new File(getConfig().getHotRodClientPropertiesFile());
		if (!f.exists()) {
			throw new ResourceException(
					InfinispanManagedConnectionFactory.UTIL.getString("InfinispanManagedConnectionFactory.clientPropertiesFileDoesNotExist", getConfig().getHotRodClientPropertiesFile()));
		}
		try {
			Properties props = PropertiesUtils.load(f.getAbsolutePath());

			LogManager
					.logInfo(LogConstants.CTX_CONNECTOR,
							"=== Using RemoteCacheManager (loaded by properties file) ==="); //$NON-NLS-1$

			return createManager(props);

		} catch (IOException e) {
			throw new ResourceException(e);
		}

	}

	private RemoteCacheManager createUsingServerList() throws ResourceException {

		Properties props = new Properties();
		props.put(
				"infinispan.client.hotrod.server_list", getConfig().getRemoteServerList()); //$NON-NLS-1$

		LogManager.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

		return createManager(props);
	}
	
	private RemoteCacheManager createManager(Properties props) throws ResourceException {
		RemoteCacheManager remoteCacheManager;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.marshaller(new PojoMarshaller(getConfig().getClassLoader()));
			cb.withProperties(props);
			remoteCacheManager = new RemoteCacheManager(cb.build(), true);
		} catch (Exception err) {
			throw new ResourceException(err);
		}
		
		return remoteCacheManager;
	}
	
	@Override
	public void cleanUp() {
		rcm = null;
		config = null;
	}

	@Override
	public void forceCleanUp() {
		cleanUp();
	}
	/**
	 * {@inheritDoc}
	 *
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public QueryFactory getQueryFactory() {
		return Search.getQueryFactory(getCache());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key) {
		return getCache().get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#add(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void add(Object key, Object value)  {
		getCache(getTargetCacheName()).put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key)  {
		return getCache(getTargetCacheName()).removeAsync(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) {
		getCache(getTargetCacheName()).replace(key, value);

	}


	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {
         RemoteCache cache = getCache(cacheName);

		Map<Object, Object> c = cache.getBulk();
		List<Object> results = new ArrayList<Object>();
		for (Object k:c.keySet()) {
			cache.removeAsync(k);
			
		}
	}
	
	/**
	 * Note:  This is used in testing only to enable shutting down the cache so that the next test can recreate it
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#shutDownCacheManager()
	 */
	@Override
	protected void shutDownCacheManager() {
		rcm.stop();
	}

	
	/** 
	 * Returns the <code>SearchType</code> that will be used to perform
	 * dynamic searching of the cache.
	 * @return SearchType
	 */
	@Override
	public SearchType getSearchType() {
		return new SearchByKey(this);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getVersion()
	 */
	@Override
	public Version getVersion() throws TranslatorException {
		return null;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#configuredUsingAnnotations()
	 */
	@Override
	public boolean configuredUsingAnnotations() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#configuredForMaterialization()
	 */
	@Override
	public boolean configuredForMaterialization() {
		return false;
	}


}

class PojoMarshaller extends AbstractJBossMarshaller {

	public PojoMarshaller(final ClassLoader cl) {
		super();
		baseCfg.setClassResolver(new ContextClassResolver(){
		    protected ClassLoader getClassLoader() {
		    			return cl;
		    }
		});
	}

}
