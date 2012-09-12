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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.api.BasicCache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.infinispan.search.LuceneSearch;
import org.teiid.translator.object.infinispan.search.SearchByKey;

@Translator(name = "infinispan-cache", description = "The Infinispan Cache Translator")
public class InfinispanExecutionFactory extends ObjectExecutionFactory {
	public static final String PROPERTIES_FILE = "META-INF" + File.separator
			+ "datagrid.properties";

	private boolean useLeceneSearching = false;
	private String cacheName = null;
	private String configurationFileName = null;

	private BasicCacheContainer cacheContainer = null;

	public InfinispanExecutionFactory() {
		super();

		this.setSearchStrategyClassName(SearchByKey.class.getName());
	}

	@Override
	public void start() throws TranslatorException {

		if (this.getCacheName() == null || this.getCacheName().isEmpty()) {
			String msg = ObjectPlugin.Util.getString(
					"InfinispanExecutionFactory.cacheNameNotDefined",
					new Object[] {});
			throw new TranslatorException(msg); //$NON-NLS-1$
		}

		super.start();

		if (createCacheContainer()) {
			cacheContainer = this.getCacheContainer();
		}
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Object connection) throws TranslatorException {

		return new ObjectExecution((Select) command, metadata, this,
				(cacheContainer != null ? cacheContainer : connection));

	}

	protected boolean createCacheContainer() {
		if (this.getConfigurationFileName() != null) {
			return true;
		}
		return false;

	}

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
	 * Indicates if Hibernate Search and Apache Lucene are used to index and
	 * search objects
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Use Lucene Searching", description = "True, assumes objects have Hibernate Search annotations", advanced = true)
	public boolean useLuceneSearching() {
		return useLeceneSearching;
	}

	public void setUseLeceneSearching(boolean useLeceneSearching) {
		this.useLeceneSearching = useLeceneSearching;
		if (useLeceneSearching) {
			this.setSearchStrategyClassName(LuceneSearch.class.getName());
		} else {
			this.setSearchStrategyClassName(SearchByKey.class.getName());
		}
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

	public BasicCache<String, Object> getCache(Object connection)
			throws TranslatorException {
		BasicCache<String, Object> cache = null;
		if (connection instanceof BasicCacheContainer) {
			BasicCacheContainer bc = (BasicCacheContainer) connection;

			if (this.getCacheName() != null) {
				cache = bc.getCache(this.getCacheName());
			} else {
				cache = bc.getCache();
			}
		} else if (connection instanceof BasicCache) {
			cache = (BasicCache) connection;
		} else {
			String msg = ObjectPlugin.Util.getString(
					"InfinispanExecutionFactory.unsupportedConnectionType",
					new Object[] { connection.getClass().getName(),
							"either BasicCache or BasicCacheContainer" });
			throw new TranslatorException(msg); //$NON-NLS-1$

		}

		if (cache == null) {
			String msg = ObjectPlugin.Util.getString(
					"InfinispanExecutionFactory.noCache", new Object[] { (this
							.getCacheName() != null ? this.getCacheName()
							: "DefaultCache") });
			throw new TranslatorException(msg); //$NON-NLS-1$
		}

		return cache;

	}

	/**
	 * Method for obtaining the CacheContainer. This method will be called to
	 * create a container based on the <code>configurationFileName</code>
	 * specified.
	 * 
	 * @return BasicCacheContainer
	 * @throws TranslatorException
	 *             if there an issue obtaining the cache
	 * @see #getCache()
	 */
	protected synchronized BasicCacheContainer getCacheContainer()
			throws TranslatorException {
		BasicCacheContainer container = null;

		try {
			container = new DefaultCacheManager(
					this.getConfigurationFileName());
			LogManager
					.logInfo(LogConstants.CTX_CONNECTOR,
							"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
		
		return container;
	}

	protected String jdgProperty(String name) {
		Properties props = new Properties();
		try {
			props.load(this.getClass().getClassLoader()
					.getResourceAsStream(PROPERTIES_FILE));
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		return props.getProperty(name);
	}

}
