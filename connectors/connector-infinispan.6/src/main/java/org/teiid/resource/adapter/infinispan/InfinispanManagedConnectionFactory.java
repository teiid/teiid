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

import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ClassRegistry;

public class InfinispanManagedConnectionFactory extends
		BasicManagedConnectionFactory {
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);

	private static final String LOCAL_CACHE_CONNECTION = "org.teiid.resource.adapter.infinispan.local.LocalCacheConnection";
	private static final String REMOTE_CACHE_CONNECTION = "org.teiid.resource.adapter.infinispan.remote.RemoteCacheConnection";
	
	private static final long serialVersionUID = -9153717006234080627L;
	private String remoteServerList = null;
	private String configurationFileNameForLocalCache = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName = null;
	private String cacheTypeMapping = null;
	private ClassRegistry methodUtil = new ClassRegistry();
	private InfinispanCacheWrapper<?, ?> cacheWrapper = null;

	private String module;
	private String cacheName;
	private String pkKey;
	private Class<?> pkCacheKeyJavaType = null;
	private Class<?> cacheTypeClass = null; // cacheName ==> ClassType
	private ClassLoader cl;
	private String connectionType = null;

	@Override
	public BasicConnectionFactory<InfinispanCacheRAConnection> createConnectionFactory()
			throws ResourceException {
		if (this.cacheTypeMapping == null) {
			throw new InvalidPropertyException(
					UTIL.getString("InfinispanManagedConnectionFactory.cacheTypeMapNotSet")); //$NON-NLS-1$
		}

		if (remoteServerList == null
				&& configurationFileNameForLocalCache == null
				&& hotrodClientPropertiesFile == null && cacheJndiName == null) {
			throw new InvalidPropertyException(
					UTIL.getString("InfinispanManagedConnectionFactory.invalidServerConfiguration")); //$NON-NLS-1$	
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
	
	public String getCacheName() {
		return this.cacheName;
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
	 * Returns the <code>host:port[;host:port...]</code> list that identifies
	 * the remote servers to include in this cluster;
	 * 
	 * @return <code>host:port[;host:port...]</code> list
	 */
	public String getRemoteServerList() {
		return remoteServerList;
	}

	/**
	 * Set the list of remote servers that make up the Infinispan cluster. The
	 * servers must be Infinispan HotRod servers. The list must be in the
	 * appropriate format of <code>host:port[;host:port...]</code> that would be
	 * used when defining an Infinispan RemoteCacheManager instance. If
	 * the value is missing, <code>localhost:11311</code> is assumed.
	 * 
	 * @param remoteServerList
	 *            the server list in appropriate
	 *            <code>server:port;server2:port2</code> format.
	 */
	public void setRemoteServerList(String remoteServerList) {
		this.remoteServerList = remoteServerList;
	}

	/**
	 * Get the name of the configuration resource or file that should be used to
	 * configure a local cache manager
	 * 
	 * @return the name of the resource or file configuration that should be
	 *         passed to the cache container
	 * @see #setConfigurationFileNameForLocalCache(String)
	 * @deprecated
	 */
	
	public String getConfigurationFileNameForLocalCache() {
		return configurationFileNameForLocalCache;
	}

	/**
	 * Set the name of the configuration that should be used to configure a
	 * local cache .
	 * 
	 * @param configurationFileName
	 *            the name of the configuration file that should be used to load
	 *            the cacheContainer
	 * @see #getConfigurationFileNameForLocalCache()
	 * @deprecated
	 */
	public void setConfigurationFileNameForLocalCache(
			String configurationFileName) {
		this.configurationFileNameForLocalCache = configurationFileName;
	}

	/**
	 * Get the name of the HotRod client properties file that should be used to
	 * configure a RemoteCacheManager remoteCacheManager.
	 * 
	 * @return the name of the HotRod client properties file to be used to
	 *         configure RemoteCacheManager
	 * @see #setHotRodClientPropertiesFile(String)
	 */
	public String getHotRodClientPropertiesFile() {
		return hotrodClientPropertiesFile;
	}

	/**
	 * Set the name of the HotRod client properties file that should be used to
	 * configure a remoteCacheManager.
	 * 
	 * @param propertieFileName
	 *            the name of the HotRod client properties file that should be
	 *            used to configure the RemoteCacheManager
	 *            container.
	 * @see #getHotRodClientPropertiesFile()
	 */
	public void setHotRodClientPropertiesFile(String propertieFileName) {
		this.hotrodClientPropertiesFile = propertieFileName;
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
	
	protected InfinispanCacheWrapper<?,?> getCacheWrapper() {
		return cacheWrapper;
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
		
		cacheName = cacheClassparm.get(0);
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
		if (this.cacheWrapper == null) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(
						this.getClass().getClassLoader());
				loadClasses();
				
				Class<?> clz = null;
				if (getCacheJndiName() != null) {
					Object cacheMgr = performJNDICacheLookup(getCacheJndiName());
					boolean isHotRod = isHotRod(cacheMgr);
					if (isHotRod) {
						clz = loadClass(REMOTE_CACHE_CONNECTION);
					} else {
						clz = loadClass(LOCAL_CACHE_CONNECTION);
					}
					InfinispanCacheWrapper<?,?> cachewrapper  =  (InfinispanCacheWrapper<?, ?>) clz.newInstance();
					cachewrapper.init(this, cacheMgr);
					this.cacheWrapper =cachewrapper;

				} else {
					clz = loadClass(connectionType);
					InfinispanCacheWrapper<?,?> cachewrapper  =  (InfinispanCacheWrapper<?, ?>) clz.newInstance();
					cachewrapper.init(this);
					this.cacheWrapper =cachewrapper;
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
			this.connectionType =LOCAL_CACHE_CONNECTION;
			return true;
		} 
		String jndiName = getCacheJndiName();
		if (jndiName != null && jndiName.trim().length() != 0) {
			return true; // to be determined later
		} else if (this.getHotRodClientPropertiesFile() != null) {
			this.connectionType = REMOTE_CACHE_CONNECTION;
			return true;
		} else if (this.getRemoteServerList() != null
				&& !this.getRemoteServerList().isEmpty()) {
			this.connectionType = REMOTE_CACHE_CONNECTION;
			return true;
		}
		
		return false;
	}
	
	/** 
	 * If the CacheManager is the HotRod manager, then use the remote cache connection,
	 * otherwise, use the local cache connection
	 * @param cacheMgr
	 * @return boolean true if its the RemoteCacheManager used in HotRod
	 * @throws Exception
	 */
	protected boolean isHotRod(Object cacheMgr) throws Exception {
		if (cacheMgr.getClass().getName().contains("hotrod")) {
			return true;
		}
		return false;
	}
	

	/* split out for testing purposes */
	protected Object performJNDICacheLookup(String jndiName) throws Exception {
		Context context = null;

		context = new InitialContext();
		final Object cache = context.lookup(jndiName);

		if (cache == null) {
			throw new ResourceException(
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
				+ ((remoteServerList == null) ? 0 : remoteServerList.hashCode());
		result = prime
				* result
				+ ((configurationFileNameForLocalCache == null) ? 0
						: configurationFileNameForLocalCache.hashCode());
		result = prime
				* result
				+ ((hotrodClientPropertiesFile == null) ? 0
						: hotrodClientPropertiesFile.hashCode());
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

		if (!checkEquals(this.remoteServerList, other.remoteServerList)) {
			return false;
		}
		if (!checkEquals(this.configurationFileNameForLocalCache,
				other.configurationFileNameForLocalCache)) {
			return false;
		}
		if (!checkEquals(this.hotrodClientPropertiesFile,
				other.hotrodClientPropertiesFile)) {
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

	}
	
	
	/** used in testing */
	public void shutDown() {
		this.cacheWrapper.cleanUp();
	}
}
