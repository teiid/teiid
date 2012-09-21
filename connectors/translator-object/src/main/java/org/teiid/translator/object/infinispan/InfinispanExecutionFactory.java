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

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.object.ObjectPlugin;

/**
 * InfinispanExecutionFactory is the translator that will access an Infinispan local cache.
 * <p>
 * The default settings are:
 * <li>{@link #supportsLuceneSearching dynamic Searching} - will be set to <code>false</code>, supporting only Key searching.
 * This is because you must have your objects in your cache annotated before Hibernate/Lucene searching will work.
 * </li>
 * <p>
 * The required settings are:
 * <li>{@link #setCacheJndiName(String) jndiName} OR {@link #setConfigurationFileName(String) configFileName} - 
 * must be specified to indicate how the Infinispan container will be obtained</li>
 * <li>{@link #setCacheName(String) cacheName} - identifies the cache located in the Infinispan container</li>
 * <p>
 * Optional settings are:
 * <li>{@link #setSupportsLuceneSearching(boolean) dynamic Searching} - when <code>true</code>, will use the 
 * Hibernate/Lucene searching to locate objects in the cache</li>
 * 
 * @author vhalbert
 *
 */
@Translator(name = "infinispan-cache", description = "The Infinispan Cache Translator")
public class InfinispanExecutionFactory extends InfinispanBaseExecutionFactory {
	public static final String PROPERTIES_FILE = "META-INF" + File.separator
			+ "datagrid.properties";

	private boolean supportsLuceneSearching = false;

	protected BasicCacheContainer cacheContainer = null;
	private boolean useJndi = true;
	
	public InfinispanExecutionFactory() {
		super();
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		
		String configFile = this.getConfigurationFileName();
		String jndiName = getCacheJndiName();
		if ( jndiName == null || jndiName.trim().length() == 0) {
			if (configFile == null || configFile.trim().length() == 0) {
	   			String msg = ObjectPlugin.Util
    			.getString(
    					"InfinispanExecutionFactory.undefinedHowToGetCache");
        		throw new TranslatorException(msg); //$NON-NLS-1$	
			}
			useJndi = false;
			
		} else {
			useJndi = true;
		}			

	}
	
	public boolean isAlive() {
		return (cacheContainer != null ? true : false);
	}
	
	public boolean isFullTextSearchingSupported() {
		return this.supportsLuceneSearching;
	}


	/**
	 * Indicates if Hibernate Search and Apache Lucene are used to index and
	 * search objects
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Support Using Lucene Searching", description = "True, assumes objects have Hibernate Search annotations", advanced = true)
	public boolean supportsLuceneSearching() {
		return this.supportsLuceneSearching;
	}

	public void setSupportsLuceneSearching(boolean supportsLuceneSearching) {
		this.supportsLuceneSearching = supportsLuceneSearching;
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
		if (this.cacheContainer != null) return this.cacheContainer;
		
		this.cacheContainer = createCacheContainer();
		
		return this.cacheContainer;

	}
	
	private BasicCacheContainer createCacheContainer() throws TranslatorException {
		BasicCacheContainer container = null;

		if (useJndi) {
			Object object = findCacheUsingJNDIName();
			if (object instanceof BasicCacheContainer) {
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using CacheContainer (loaded from Jndi) ==="); //$NON-NLS-1$

				return (BasicCacheContainer) object;
			}
			String msg = ObjectPlugin.Util.getString(
			"InfinispanExecutionFactory.unsupportedContainerType",
			new Object[] { object.getClass().getName(),
					"BasicCacheContainer" });
			throw new TranslatorException(msg); //$NON-NLS-1$
			
			
		}
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

	public void cleanUp() {
		this.cacheContainer = null;
	}
}
