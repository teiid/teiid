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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;

public class InfinispanManagedConnectionFactory extends
		BasicManagedConnectionFactory {
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);
	
	private static final long serialVersionUID = -9153717006234080627L;
	private String configurationFileNameForLocalCache = null;
	private String cacheJndiName = null;
	private String cacheTypeMapping = null;
	private ClassRegistry methodUtil = new ClassRegistry();
	private EmbeddedCacheManager cacheManager = null;

	private String module;
	private String stagingCacheName;
	private String aliasCacheName;
	private String pkKey;
	private Class<?> pkCacheKeyJavaType = null;
	private Class<?> cacheTypeClass = null; // cacheName ==> ClassType
	private ClassLoader cl;
	private CacheNameProxy cacheNameProxy;

	@Override
	public BasicConnectionFactory<InfinispanCacheRAConnection> createConnectionFactory()
			throws ResourceException {
		if (this.cacheTypeMapping == null) {
			throw new InvalidPropertyException(
					UTIL.getString("InfinispanManagedConnectionFactory.cacheTypeMapNotSet")); //$NON-NLS-1$
		}

		boolean isConfigured = determineConnectionType();
		if (!isConfigured) {
			throw new InvalidPropertyException(
					UTIL.getString("InfinispanManagedConnectionFactory.invalidServerConfiguration")); //$NON-NLS-1$			
		}

		/*
		 * the cache container that will be accessing a local infinispan cache
		 * cannot be created at the time of the factory, because at server
		 * startup, the internal infinispan caches have not been created yet. So
		 * the container creation has to be delayed until the connection is
		 * requested.
		 */
		return new BasicConnectionFactory<InfinispanCacheRAConnection>() {

			private static final long serialVersionUID = 2579916624625349535L;

			@Override
			public InfinispanCacheRAConnection getConnection()
					throws ResourceException {
				return InfinispanManagedConnectionFactory.this.createCacheConnection();

			}
		};

	}
	
//	public String getCacheName() {
//		// return the cacheName that is mapped as the alias
//		return cacheNameProxy.getPrimaryCacheAliasName();
//	}
//
//	public String getCacheStagingName() {
//		return cacheNameProxy.getStageCacheAliasName();
//	}
	
	/** 
	 * Call to set the name of the cache to access when calling getCache
	 * @param cacheName
	 * @throws ResourceException 
	 */
	protected void setCacheName(String cacheName) throws ResourceException {
		if (getAliasCacheName() != null && getStagingCacheName() != null) {
			cacheNameProxy = new CacheNameProxy(cacheName, getStagingCacheName(),getAliasCacheName() );
			
		} else if (getStagingCacheName() != null || getAliasCacheName() != null)  {
			throw new InvalidPropertyException(
					UTIL.getString("InfinispanManagedConnectionFactory.invalidMaterializationSettings")); //$NON-NLS-1$	

		} else {
			cacheNameProxy = new CacheNameProxy(cacheName);
		}
	}
	
	public CacheNameProxy getCacheNameProxy() {
		return cacheNameProxy;
	}
	/**
	 * Get the <code>cacheName:className[;pkFieldName[:cacheJavaType]]</code> cache
	 * type mappings.
	 * 
	 * @return <code>cacheName:className[;pkFieldName[:cacheJavaType]]</code> cache
	 *         type mappings
	 * @see #setCacheTypeMap(String)
	 */
	public String getCacheTypeMap() {
		return cacheTypeMapping;
	}

	/**
	 * Set the cache type mapping
	 * <code>cacheName:className[;pkFieldName[:cacheJavaType]]</code> that represent
	 * the root node class type for an available cache for access.
	 * The following is how the string parsed:
	 * <li>cacheName = is the name used to retrieve the named cache
	 * <li>className = is the class that is stored in the cache, and will used to create new instances
	 * <li> [optional] pkFieldName = defined which attribute is defined as the key to the cache, this will be 
	 * the key used to access the cache for updates.  If not defined, updates will be disabled.
	 * Also,  pkFieldName is required if the root object contains child objects that will accessible. 
	 * <li> [optional] cacheJavaType = is defined when the pkFieldName java type defined in class is different
	 * than how its stored as the key of the cache.  This will enable the correct java type to be used
	 * when updates are performed and the object can be directly accessed.
	 * 
	 * @param cacheTypeMap
	 *            the cache type mappings passed in the form of
	 *            <code>cacheName:className[;pkFieldName[:cacheJavaType]]</code>
	 * @see #getCacheTypeMap()
	 */
	public void setCacheTypeMap(String cacheTypeMap) {
		this.cacheTypeMapping = cacheTypeMap;
	}
	
	public String getStagingCacheName() {
		return this.stagingCacheName;
	}

	/**
	 * An option to configure the staging cache name to use when using JDG to materialize data.
	 * @param cacheName
	 */
	public void setStagingCacheName(String cacheName) {
		this.stagingCacheName = cacheName;
	}
	
	public String getAliasCacheName() {
		return this.aliasCacheName;
	}
	
	/**
	 * An option to configure the alias cache name to use when using JDG to materialize data.
	 * @param cacheName
	 */
	public void setAliasCacheName(String cacheName) {
		this.aliasCacheName = cacheName;
	}

	/**
	 * Sets the (optional) module(s) where the ClassName class is defined, that
	 * will be loaded <code> (module,[module,..])</code>
	 * 
	 * @param module
	 * @see #getModule
	 */
	public void setModule(String module) {
		this.module = module;
	}

	/**
	 * Called to get the module(s) that are to be loaded
	 * 
	 * @see #setModule
	 * @return String
	 */
	public String getModule() {
		return module;
	}

	public String getPKey() {
		return this.pkKey;
	}

	public Class<?> getCacheClassType() {
		return this.cacheTypeClass;
	}
	
	/**
	 * This is an optional argument when defining the <code>CacheTypeMap</code>
	 * on the resource adapter.
	 * 
	 * @return Class<?>
	 */
	public Class<?> getCacheKeyClassType() {
		return pkCacheKeyJavaType;
	}	
	
	public ClassRegistry getClassRegistry() {
		return methodUtil;
	}

	/**
	 * Get the name of the configuration resource or file that should be used to
	 * configure a local cache manager
	 * 
	 * @return the name, and if indicated full path, of the file configuration that is used
	 *         to configure the cache container
	 * @see #setConfigurationFileNameForLocalCache(String)
	 */
	
	public String getConfigurationFileNameForLocalCache() {
		return configurationFileNameForLocalCache;
	}

	/**
	 * Set the name of the configuration that should be used to configure a
	 * local cache .
	 * 
	 * @param configurationFileName
	 *            the name, and the full path, to the configuration file that should be used to configure
	 *            the cacheContainer
	 * @see #getConfigurationFileNameForLocalCache()
	 */
	public void setConfigurationFileNameForLocalCache(
			String configurationFileName) {
		this.configurationFileNameForLocalCache = configurationFileName;
	}

	/**
	 * Get the JNDI Name of the cache.
	 * 
	 * @return JNDI Name of cache
	 */
	public String getCacheJndiName() {
		return cacheJndiName;
	}

	/**
	 * Set the JNDI name to a {@link Map cache} instance that should be used as
	 * this source.
	 * 
	 * @param jndiName
	 *            the JNDI name of the {@link Map cache} instance that should be
	 *            used
	 * @see #setCacheJndiName(String)
	 */
	public void setCacheJndiName(String jndiName) {
		this.cacheJndiName = jndiName;
	}
	
	@SuppressWarnings("rawtypes")
	public Cache<Object, Object> getCache(String cacheName) {
      if (cacheName == null) {
      	Assertion.isNotNull(cacheName, "Program Error: Cache Name is null");
      }

		return cacheManager.getCache(cacheName);
	}	


	public ClassLoader getClassLoader() {
		return this.cl;
	}
	
	protected Class<?> loadClass(String className) throws ResourceException {
		try {
			return Class.forName(className, true, getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new ResourceException(e);
		}
	}

	protected synchronized ClassLoader loadClasses() throws ResourceException {

		cl = null;
		// Thread.currentThread().getContextClassLoader();
		if (getModule() != null) {

			try {
				List<String> mods = StringUtil.getTokens(getModule(), ","); //$NON-NLS-1$
				for (String mod : mods) {
		
					Module x = Module.getContextModuleLoader().loadModule(
							ModuleIdentifier.create(mod));
					// the first entry must be the module associated with the
					// cache
					if (cl == null) {
						cl = x.getClassLoader();
					} else {
						throw new ResourceException("Unable to get classloader for " + mod);
					}
				}
			} catch (ModuleLoadException e) {
				throw new ResourceException(e);
			}

		} else {
			cl =  Thread.currentThread().getContextClassLoader();

		}

		/*
		 * Parsing based on format:  cacheName:className[;pkFieldName[:cacheKeyJavaType]]
		 * 
		 */
		
		List<String> parms = StringUtil.getTokens(getCacheTypeMap(), ";"); //$NON-NLS-1$
		String leftside = parms.get(0);
		List<String> cacheClassparm = StringUtil.getTokens(leftside, ":");
		
		if (cacheClassparm.size() != 2) {
			throw new InvalidPropertyException(UTIL.gs("TEIID25022"));
		}
		
		String cn = cacheClassparm.get(0);
		setCacheName(cn);
		String className = cacheClassparm.get(1);
		cacheTypeClass = loadClass(className);
		try {
			methodUtil.registerClass(cacheTypeClass);
		} catch (TranslatorException e1) {
			throw new ResourceException(e1);
		}
			
		if (parms.size() == 2) {
			String rightside = parms.get(1);
			List<String> pkKeyparm = StringUtil.getTokens(rightside, ":");
			pkKey = pkKeyparm.get(0);
			if (pkKeyparm.size() == 2) {
				String pktype = pkKeyparm.get(1);
				if (pktype != null) {
					pkCacheKeyJavaType = getPrimitiveClass(pktype);
				}
			}
		}		
		
		return cl;

	}

	protected synchronized InfinispanCacheRAConnection createCacheConnection() throws ResourceException {
		if (this.cacheManager == null) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(
						this.getClass().getClassLoader());
				loadClasses();
				
				InfinispanCacheRAConnection conn = new InfinispanCacheRAConnection(this);
				createCache();

						
				// if configured for materialization, initialize the 
				if (cacheNameProxy.getAliasCacheName() != null) {
					Map<Object,Object> aliasCache = (Map<Object, Object>) this.cacheManager.getCache(cacheNameProxy.getAliasCacheName());
					if (aliasCache == null) {
						throw new ResourceException(	
							InfinispanManagedConnectionFactory.UTIL
							.getString(
									"InfinispanManagedConnectionFactory.aliasCacheNotDefined", cacheNameProxy.getAliasCacheName())); //$NON-NLS-1$
					}
					cacheNameProxy.initializeAliasCache(aliasCache);
				}

			} catch (Exception e) {
				throw new ResourceException(e);
			} finally {
				Thread.currentThread().setContextClassLoader(cl);
			}
		}
		return new InfinispanCacheRAConnection(this);
	}

	private boolean determineConnectionType() {
		if (this.getConfigurationFileNameForLocalCache() != null) {
			return true;
		} 
		String jndiName = getCacheJndiName();
		if (jndiName != null && jndiName.trim().length() != 0) {
			return true; // to be determined later
		} 
		
		return false;
	}
	
	private void createCache() throws TranslatorException {
		if (cacheManager != null) return;

		if (getConfigurationFileNameForLocalCache() != null) {
			try {
				 DefaultCacheManager cc = new DefaultCacheManager(getConfigurationFileNameForLocalCache());
				
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

				cacheManager = cc;
				
				Configuration conf = cc.getCacheConfiguration(getCacheNameProxy().getPrimaryCacheAliasName());
				if (conf == null) {
					throw new TranslatorException("Program Error: cache " +  getCacheNameProxy().getPrimaryCacheAliasName() + " was not configured");
				}
				conf.module(getCacheClassType());
				
				if (getCacheNameProxy().getStageCacheAliasName() != null) {
					conf = cc.getCacheConfiguration(getCacheNameProxy().getStageCacheAliasName());
					if (conf == null) {
						throw new TranslatorException("Program Error: cache " +  getCacheNameProxy().getStageCacheAliasName() + " was not configured");
					}
					
					conf.module(getCacheClassType());
				}
			} catch (IOException e) {
				throw new TranslatorException(e);
			}
		} else {
			if (getCacheJndiName() != null) {
				try {
					cacheManager =  (EmbeddedCacheManager) performJNDICacheLookup(getCacheJndiName());
				} catch (TranslatorException e) {
					throw e;
				} catch (Exception e) {
					throw new TranslatorException(e);
				}
			} else if (cacheManager == null) {
				throw new TranslatorException("Program Error: DefaultCacheManager was not configured");
			}
		}

	}
	

	/* split out for testing purposes */
	protected Object performJNDICacheLookup(String jndiName) throws Exception {
		Context context = null;

		context = new InitialContext();
		final Object cache = context.lookup(jndiName);

		if (cache == null) {
			throw new TranslatorException(
					InfinispanManagedConnectionFactory.UTIL
							.getString(
									"InfinispanManagedConnectionFactory.unableToFindCacheUsingJNDI", jndiName)); //$NON-NLS-1$
		}

		return cache;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((configurationFileNameForLocalCache == null) ? 0
						: configurationFileNameForLocalCache.hashCode());
		result = prime * result
				+ ((cacheJndiName == null) ? 0 : cacheJndiName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InfinispanManagedConnectionFactory other = (InfinispanManagedConnectionFactory) obj;

		if (!checkEquals(this.configurationFileNameForLocalCache,
				other.configurationFileNameForLocalCache)) {
			return false;
		}
		if (!checkEquals(this.cacheJndiName, other.cacheJndiName)) {
			return false;
		}
		return false;

	}
	
	private Class<?> getPrimitiveClass(String className) throws ResourceException {
		if (className.contains(".")) {
			return loadClass(className);
		}
		if (className.equalsIgnoreCase("int")) {
			return int.class;
		}
		if (className.equalsIgnoreCase("long")) {
			return long.class;
		}
		if (className.equalsIgnoreCase("double")) {
			return double.class;
		}
		if (className.equalsIgnoreCase("short")) {
			return short.class;
		}
		if (className.equalsIgnoreCase("char")) {
			return char.class;
		}		
		if (className.equalsIgnoreCase("float")) {
			return float.class;
		}
		if (className.equalsIgnoreCase("boolean")) {
			return boolean.class;
		}
		
		return loadClass(className);
	}

	public void cleanUp() {
		methodUtil = null;
		cl = null;
		this.cacheManager= null;

	}
	
	public boolean isAlive() {
		if (cacheManager == null) return false;
		if (cacheManager.getStatus() == ComponentStatus.RUNNING) return true;
		
		return false;
	}
	
	protected String getVersion() {
		if (cacheManager == null) return "";

		return cacheManager.getCache(this.getCacheNameProxy().getPrimaryCacheKey()).getVersion();
	}
	
	
	/** used in testing */
	public void shutDownCache() {
		cleanUp();
	}
}