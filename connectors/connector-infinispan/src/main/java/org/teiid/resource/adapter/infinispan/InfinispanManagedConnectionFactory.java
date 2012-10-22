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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

/**
 * The InfinispanManagedConnectionFactory exposes the Infinispan CacheManager in
 * order to obtain a cache by the {@link InfinispanConnectionImpl connection}.
 * <p>
 * Because Infinispan has various CacheManagers and they can be implemented
 * differently based on clustering, remote access, or just basic default cache
 * manager, it was easier to use reflections to get the cache, because they all
 * implement the same methods for cache access. And this makes the connector
 * logic simpler without having to know (or keep up with) which type of cache manager
 * is being used.
 * 
 * @author vhalbert
 * 
 */
public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -9153717006234080627L;
	private String remoteServerList=null;
	private String configurationFileNameForLocalCache = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName=null;
	private Map<String, Class<?>> typeMap = null;
	private String cacheTypes = null;
	
	private Object cacheManager;
	
	private Method getCacheMethod = null;
	private Method getCacheByNameMethod = null;
	private Method isAliveMethod = null;

	private String module;
	
	@Override
	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory() throws ResourceException {
		if (this.cacheTypes == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.cacheTypeMapNotSet")); //$NON-NLS-1$
		}
		
		if (remoteServerList == null && 
				configurationFileNameForLocalCache == null &&
				hotrodClientPropertiesFile == null &&
				cacheJndiName == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.invalidServerConfiguration")); //$NON-NLS-1$
			
		}
		
		createCacheContainer();

		ClassLoader cl = null;
		if (module != null) {
			Module m;
			try {
				m = Module.getCallerModuleLoader().loadModule(ModuleIdentifier.create(module));
			} catch (ModuleLoadException e) {
				throw new ResourceException(e);
			}
			cl = m.getClassLoader();
		} else {
			cl = Thread.currentThread().getContextClassLoader();
		}
		List<String> types = StringUtil.getTokens(this.cacheTypes, ","); //$NON-NLS-1$
		typeMap = new HashMap<String, Class<?>>();
		for (String type : types) {
			final List<String> mapped = StringUtil.getTokens(type, ":"); //$NON-NLS-1$
			final String cacheName = mapped.get(0);
			final String className = mapped.get(1);

			try {
				typeMap.put(cacheName, Class.forName(className, true, cl));
			} catch (ClassNotFoundException e) {
				throw new ResourceException(e);
			}
		}

		return new BasicConnectionFactory<InfinispanConnectionImpl>() {
			
			private static final long serialVersionUID = 2579916624625349535L;

			@Override
			public InfinispanConnectionImpl getConnection() throws ResourceException {
				return new InfinispanConnectionImpl(InfinispanManagedConnectionFactory.this);
			}
		};
	}	
	
	/**
	 * Get the <code>cacheName:ClassName[;cacheName:ClassName...]</code> cache type mappings.
	 * 
	 * @return <code>cacheName:ClassName[;cacheName:ClassName...]</code> cache type mappings
	 * @see #setCacheTypeMap(String)
	 */
	public String getCacheTypeMap() {
		return cacheTypes;
	}

	/**
	 * Set the cache type mapping <code>cacheName:ClassName[;cacheName:ClassName...]</code> that represent
	 * the root node class type for 1 or more caches available for access.
	 * 
	 * @param cacheTypeMap
	 *            the cache type mappings passed in the form of <code>cacheName:ClassName[;cacheName:ClassName...]</code>
	 * @see #getCacheTypeMap()
	 */
	public void setCacheTypeMap(
			String cacheTypeMap) {
		this.cacheTypes = cacheTypeMap;
	}  
	
	public void setModule(String module) {
		this.module = module;
	}
	
	public String getModule() {
		return module;
	}
	
	public Class<?> getCacheType(String cacheName) {
		return typeMap.get(cacheName);
	}
    
	/**
	 * Returns the <code>host:port[;host:port...]</code> list that identifies the remote servers
	 * to include in this cluster;
	 * @return <code>host:port[;host:port...]</code> list
	 */
   public String getRemoteServerList() {
        return remoteServerList;
    }

    /**
     * Set the list of remote servers that make up the Infinispan cluster. The servers must be Infinispan HotRod servers. The list
     * must be in the appropriate format of <code>host:port[;host:port...]</code> that would be used when defining an Infinispan
     * {@link RemoteCacheManager} instance. If the value is missing, <code>localhost:11311</code> is assumed.
     * 
     * @param remoteServerList the server list in appropriate <code>server:port;server2:port2</code> format.
     */
    public void setRemoteServerList( String remoteServerList ) {
        this.remoteServerList = remoteServerList;
    }
    
	/**
	 * Get the name of the configuration resource or file that should be used to
	 * configure a local {@link CacheContainer} using {@link DefaultCacheManager}.
	 * 
	 * @return the name of the resource or file configuration that should be
	 *         passed to the {@link CacheContainer}
	 * @see #setConfigurationFileNameForLocalCache(String)
	 */
	public String getConfigurationFileNameForLocalCache() {
		return configurationFileNameForLocalCache;
	}

	/**
	 * Set the name of the configuration that should be used to configure a
	 * local {@link Cache cache} using the {@link DefaultCacheManager}.
	 * 
	 * @param configurationFileName
	 *            the name of the configuration file that should be used to load
	 *            the {@link CacheContainer}
	 * @see #getConfigurationFileNameForLocalCache()
	 */
	public void setConfigurationFileNameForLocalCache(
			String configurationFileName) {
		this.configurationFileNameForLocalCache = configurationFileName;
	}  
	
	/**
	 * Get the name of the HotRod client properties file that should be used to
	 * configure a RemoteCacheManager remoteCacheManager.
	 * 
	 * @return the name of the HotRod client properties file to be
	 *        used to configure {@link RemoteCacheContainer}
	 * @see #setHotRodClientPropertiesFile(String)
	 */
	public String getHotRodClientPropertiesFile() {
		return hotrodClientPropertiesFile;
	}

	/**
	 * Set the name of the HotRod client properties file that should be used to configure a
	 *  {@link RemoteCacheManager remoteCacheManager}.
	 * 
	 * @param propertieFileName
	 *            the name of the HotRod client properties file that should be used to configure
	 *            the {@link RemoteCacheContainer remote} container.
	 * @see #getHotRodClientPropertiesFile()
	 */
	public void setHotRodClientPropertiesFile(
			String propertieFileName) {
		this.hotrodClientPropertiesFile = propertieFileName;
	}  	
	
	/**
	 * Get the JNDI Name of the cache.
	 * @return JNDI Name of cache
	 */
    public String getCacheJndiName() {
        return cacheJndiName;
    }

    /**
     * Set the JNDI name to a {@link Map cache} instance that should be used as this source.
     * 
     * @param jndiName the JNDI name of the {@link Map cache} instance that should be used
     * @see #setCacheJndiName(String)
     */
    public void setCacheJndiName( String jndiName ) {
        this.cacheJndiName = jndiName;
    }
    
    @SuppressWarnings("unchecked")
	protected Map<Object,Object> getCache(String cacheName) throws ResourceException {
    	if (cacheManager != null) {
    		if (cacheName == null) {    			
    			return (Map<Object, Object>) executeMethod(this.getCacheMethod, cacheManager, new Object[] {});
    		} else {   			
    			return (Map<Object, Object>) executeMethod(this.getCacheByNameMethod, cacheManager, new Object[] {cacheName});
    		}   		
    	}
    	
    	return null;

    }

    protected boolean isAlive() {
    	if (this.cacheManager != null) {
    		if (this.isAliveMethod != null) {
    			try {
					Object obj = executeMethod(this.isAliveMethod, cacheManager, null);
					return Boolean.getBoolean(obj.toString());
				} catch (ResourceException e) {
					return false;
				}
    		}
    	}
    	return false;
    }
    
    protected void createCacheContainer() throws ResourceException {
    	createLocalCacheContainer();
		if (this.cacheManager == null) {
			createRemoteCacheContainer();
		}
    	deriveMethods();	
    	
    }
    
	private void createRemoteCacheContainer() throws ResourceException {
		
		if (this.getHotRodClientPropertiesFile() != null) {
			File f = new File(this.getHotRodClientPropertiesFile());
			if (! f.exists()) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.clientPropertiesFileDoesNotExist", this.getHotRodClientPropertiesFile())); //$NON-NLS-1$

			}
			try {
				Properties props = PropertiesUtils.load(f.getAbsolutePath());
				RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);
				remoteCacheManager.start();
				remoteCacheManager.getCache();
				
				cacheManager = remoteCacheManager;
			} catch (MalformedURLException e) {
				throw new ResourceException(e);
			} catch (IOException e) {
				throw new ResourceException(e);
			}
			
			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using RemoteCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

		} else {
			if (this.getRemoteServerList() != null
					|| !this.getRemoteServerList().isEmpty()) {
				
				Properties props = new Properties();
				props.put("infinispan.client.hotrod.server_list", this.getRemoteServerList()); //$NON-NLS-1$
				RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);
				remoteCacheManager.start();
				
				cacheManager = remoteCacheManager;
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$
			}
		}

    }
	protected synchronized void createLocalCacheContainer() throws ResourceException {
		
	    Object cache = null;
		if (this.getConfigurationFileNameForLocalCache() != null) {	
			try {
				cacheManager = new DefaultCacheManager(
						this.getConfigurationFileNameForLocalCache());
			} catch (IOException e) {
				throw new ResourceException(e);
			}
			LogManager
					.logInfo(LogConstants.CTX_CONNECTOR,
							"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

		} 
		
	    String jndiName = getCacheJndiName();
	    if (jndiName != null && jndiName.trim().length() != 0) {
	        try {
	            Context context = null;
                try {
                    context = new InitialContext();
                } catch (NamingException err) {
                    throw new ResourceException(err);
                }
	            cache = context.lookup(jndiName);

	            if (cache == null) {
					throw new ResourceException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.unableToFindCacheUsingJNDI", jndiName)); //$NON-NLS-1$
	            } 	
	            
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using CacheContainer (obtained by JNDI: " + jndiName + " ==="); //$NON-NLS-1
	            
				cacheManager  = cache;
	        } catch (Exception err) {
	            if (err instanceof RuntimeException) throw (RuntimeException)err;
	            throw new ResourceException(err);
	        }
	    } 
    }	
	
	/**
	 * DeriveMethods is used to identify the methods to use based on the
	 * CacheManager being used.
	 * 
	 * @throws ResourceException
	 */
	private void deriveMethods() throws ResourceException {

		try {
			this.getCacheMethod = this.cacheManager.getClass()
					.getDeclaredMethod("getCache");  //$NON-NLS-1$
			this.getCacheByNameMethod = this.cacheManager.getClass()
					.getDeclaredMethod("getCache", String.class);  //$NON-NLS-1$
		} catch (SecurityException e1) {
			throw new ResourceException(e1);
		} catch (NoSuchMethodException e1) {
			throw new ResourceException(e1);
		}

		try {
			this.isAliveMethod = this.cacheManager.getClass().getMethod(
					"isStarted");  //$NON-NLS-1$

		} catch (SecurityException e) {
			throw new ResourceException(e);
		} catch (NoSuchMethodException e) {
			try {
				this.isAliveMethod = this.cacheManager.getClass()
						.getMethod("isDefaultRunning");  //$NON-NLS-1$
			} catch (SecurityException e1) {
				throw new ResourceException(e1);
			} catch (NoSuchMethodException e1) {
				// do nothing
			}
		}
	}
	
	/**
	 * Call to execute the method
	 * 
	 * @param m
	 *            is the method to execute
	 * @param api
	 *            is the object to execute the method on
	 * @param parms
	 *            are the parameters to pass when the method is executed
	 * @return Object return value
	 * @throws Exception
	 */
	private static Object executeMethod(Method m, Object api, Object[] parms)
			throws ResourceException {
		try {
			if (parms != null) {
				return m.invoke(api, parms);
			}
			return m.invoke(api, (Object[]) null);
		} catch (InvocationTargetException x) {
			x.printStackTrace();
			Throwable cause = x.getCause();
			System.err.format("invocation of %s failed: %s%n",
					"set" + m.getName(), cause.getMessage());
			LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling "
					+ m.getName() + ":" + cause.getMessage());
			throw new ResourceException(x.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new ResourceException(e.getMessage());
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((remoteServerList == null) ? 0 : remoteServerList.hashCode());
		result = prime * result + ((configurationFileNameForLocalCache == null) ? 0 : configurationFileNameForLocalCache.hashCode());
		result = prime * result + ((hotrodClientPropertiesFile == null) ? 0 : hotrodClientPropertiesFile.hashCode());
		result = prime * result + ((cacheJndiName == null) ? 0 : cacheJndiName.hashCode());	
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
		if (!checkEquals(this.configurationFileNameForLocalCache, other.configurationFileNameForLocalCache)) {
			return false;
		}
		if (!checkEquals(this.hotrodClientPropertiesFile, other.hotrodClientPropertiesFile)) {
			return false;
		}
		if (!checkEquals(this.cacheJndiName, other.cacheJndiName)) {
			return false;
		}
		return false;

	}	
	
}
