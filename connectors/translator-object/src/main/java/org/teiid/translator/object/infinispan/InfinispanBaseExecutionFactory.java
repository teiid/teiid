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
package org.teiid.translator.object.infinispan;

import javax.resource.cci.ConnectionFactory;

import org.infinispan.Cache;
import org.infinispan.api.BasicCache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.object.JavaBeanMetadataProcessor;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectPlugin;

/**
 * 
 * @author vhalbert
 *
 */
public abstract class InfinispanBaseExecutionFactory extends ObjectExecutionFactory {

	private String cacheName = null;
	private String configurationFileName = null;

	public InfinispanBaseExecutionFactory() {
		super();
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		if (this.getCacheName() == null || this.getCacheName().isEmpty()) {
			String msg = ObjectPlugin.Util.getString("InfinispanBaseExecutionFactory.cacheNameNotDefined"); //$NON-NLS-1$
			throw new TranslatorException(msg); 
		}

	}
	
	/**
	 * Will return <code>true</code> if FullText searching is supported for this implementation.
	 * 
	 * @return True if full text searching is reported.
	 */
	public abstract boolean isFullTextSearchingSupported();
	
	/** 
	 * Will return <code>true</code> if access to the cache is still allowed.
	 * @return True if access is alive
	 */
	public abstract boolean isAlive();

	/**
	 * Method for obtaining the CacheContainer by {@link InfinispanConnectionImpl#getCache() the connection}.
	 * 
	 * @return BasicCacheContainer
	 * @throws TranslatorException
	 *             if there an issue obtaining the cache
	 * @see #getCache()
	 */
	protected abstract BasicCacheContainer getCacheContainer() throws TranslatorException;

	

	/**
	 * Get the cacheName that will be used by this factory instance to access
	 * the named cache. However, if not specified a default configuration will
	 * be created.
	 * 
	 * @return
	 * @see #setCacheName(String)
	 */
	@TranslatorProperty(display = "CacheName", advanced = true)
	public String getCacheName() {
		return this.cacheName;
	}

	/**
	 * Set the cacheName that will be used to find the named cache.
	 * 
	 * @param cacheName
	 * @see #getCacheName()
	 */

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}


	/**
	 * Get the name of the configuration resource or file that should be used if
	 * a {@link Cache cache} is to be created using the
	 * {@link DefaultCacheManager}. If not specified, a default configuration
	 * will be used.
	 * 
	 * @return the name of the resource or file configuration that should be
	 *         passed to the {@link CacheContainer}, or null if the default
	 *         configuration should be used
	 * @see #setConfigurationFileName(String)
	 */
	@TranslatorProperty(display = "ConfigurationFileName", advanced = true)
	public String getConfigurationFileName() {
		return configurationFileName;
	}

	/**
	 * Set the name of the configuration that should be used if a {@link Cache
	 * cache} is to be created using the {@link DefaultCacheManager}.
	 * 
	 * @param configurationFileName
	 *            the name of the configuration file that should be used to load
	 *            the {@link CacheContainer}, or null if the default
	 *            configuration should be used
	 * @see #getConfigurationFileName()
	 */
	public synchronized void setConfigurationFileName(
			String configurationFileName) {
		if (this.configurationFileName == configurationFileName
				|| this.configurationFileName != null
				&& this.configurationFileName.equals(configurationFileName))
			return; // unchanged
		this.configurationFileName = configurationFileName;
	}
	
	public BasicCache<String, Object> getCache() throws TranslatorException {
		BasicCache<String, Object> cache = null;
		BasicCacheContainer container = getCacheContainer();
		if (getCacheName() != null) {
			cache = container.getCache(getCacheName());
		} else {
			cache = container.getCache();
		}

		if (cache == null) {
			String msg = ObjectPlugin.Util.getString(
					"InfinispanBaseExecutionFactory.cacheNotFound", new Object[] { (getCacheName() != null ? getCacheName() //$NON-NLS-1$
							: "DefaultCache") }); //$NON-NLS-1$
			throw new TranslatorException(msg); 
		}

		return cache;

	}
	
	   @Override
		public void getMetadata(MetadataFactory metadataFactory, ObjectConnection conn)
				throws TranslatorException {
		   if (this.isFullTextSearchingSupported()) {
			   
		   } else {
			   JavaBeanMetadataProcessor processor = new JavaBeanMetadataProcessor();
			   processor.getMetadata(metadataFactory, this);
		   }
		}
	

	@Override
	public ObjectConnection getConnection(ConnectionFactory factory,
			ExecutionContext executionContext) throws TranslatorException {
				
		return new InfinispanConnectionImpl(this);
	}
	
	public void cleanUp() {

	}
	
	@Override
	public boolean supportsOrCriteria() {
		return isFullTextSearchingSupported();
	}
}
