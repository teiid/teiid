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
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.Cache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private enum CACHE_TYPE {
		LOCAL_CONFIG_FILE,
		LOCAL_JNDI,
		REMOTE_SERVER_LISTS,
		REMOTE_HOT_ROD_PROPERTIES	
	}

	private static final long serialVersionUID = -9153717006234080627L;
	private String remoteServerList=null;
	private String configurationFileNameForLocalCache = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName=null;
	private Map<String, Class<?>> typeMap = null;
	private String cacheTypes = null;
	private BasicCacheContainer cacheContainer = null;
	private Map<String, String> pkMap;
	private String module;
	private CACHE_TYPE cacheType;
	
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

		determineCacheType();
		if (cacheType == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.invalidServerConfiguration")); //$NON-NLS-1$			
		}
		
		pkMap = new HashMap<String, String>();
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
		Map<String, Class<?>> tm = new HashMap<String, Class<?>>();
		for (String type : types) {
			List<String> mapped = StringUtil.getTokens(type, ":"); //$NON-NLS-1$
			if (mapped.size() != 2) {
				throw new InvalidPropertyException();
			}
			final String cacheName = mapped.get(0);
			String className = mapped.get(1);
			mapped = StringUtil.getTokens(className, ";"); //$NON-NLS-1$
			if (mapped.size() > 1) {
				className = mapped.get(0);
				pkMap.put(cacheName, mapped.get(1));
			}
			try {
				tm.put(cacheName, Class.forName(className, true, cl));
			} catch (ClassNotFoundException e) {
				throw new ResourceException(e);
			}
		}
		
		typeMap = Collections.unmodifiableMap(tm);
        
        InfinispanManagedConnectionFactory.this.createCacheContainer();

		return new BasicConnectionFactory<InfinispanConnectionImpl>() {
			
			private static final long serialVersionUID = 2579916624625349535L;

			@Override
			public InfinispanConnectionImpl getConnection() throws ResourceException {
				
				return new InfinispanConnectionImpl(InfinispanManagedConnectionFactory.this);
			}
		};
	}	
	
	public Map<String, String> getPkMap() {
		return pkMap;
	}
	
	/**
	 * Get the <code>cacheName:ClassName[,cacheName:ClassName...]</code> cache type mappings.
	 * 
	 * @return <code>cacheName:ClassName[,cacheName:ClassName...]</code> cache type mappings
	 * @see #setCacheTypeMap(String)
	 */
	public String getCacheTypeMap() {
		return cacheTypes;
	}

	/**
	 * Set the cache type mapping <code>cacheName:ClassName[,cacheName:ClassName...]</code> that represent
	 * the root node class type for 1 or more caches available for access.
	 * 
	 * @param cacheTypeMap
	 *            the cache type mappings passed in the form of <code>cacheName:ClassName[,cacheName:ClassName...]</code>
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
	
	public Map<String, Class<?>> getMapOfCacheTypes() {
		return this.typeMap;
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
    
    protected Map<Object,Object> getCache(String cacheName) {
    	if (cacheContainer != null) {
    		if (cacheName == null) {
    			return cacheContainer.getCache();
    		}
			return cacheContainer.getCache(cacheName);
    	}
    	return null;
    }
    
    

    protected boolean isAlive() {
    	if (this.cacheContainer != null) {
    		return true;
    	}
    	return false;
    }
    
    protected synchronized void createCacheContainer() throws ResourceException {
    	if (this.cacheContainer != null) return;
    	
    	switch (cacheType) {
		case LOCAL_JNDI:
			this.cacheContainer = createLocalCacheViaJNDI();
			break;
			
		case LOCAL_CONFIG_FILE:
			this.cacheContainer = createLocalCacheFromConfigFile();
			break;
			
		case REMOTE_HOT_ROD_PROPERTIES:
			this.cacheContainer = createRemoteCacheUsingHotRodClient();
			break;
			
		case REMOTE_SERVER_LISTS:
			this.cacheContainer = createRemoteCacheFromServerList();
			break;
			
		}

    }
    
    private void determineCacheType() {
    	if (this.getConfigurationFileNameForLocalCache() != null) {	
    		cacheType = CACHE_TYPE.LOCAL_CONFIG_FILE;
    	} else {
    	    String jndiName = getCacheJndiName();
    	    if (jndiName != null && jndiName.trim().length() != 0) {
    	    	cacheType = CACHE_TYPE.LOCAL_JNDI;
    	    } else if (this.getHotRodClientPropertiesFile() != null) {
    	    	cacheType = CACHE_TYPE.REMOTE_HOT_ROD_PROPERTIES;
    	    } else if (this.getRemoteServerList() != null
					&& !this.getRemoteServerList().isEmpty()) {
    	    	cacheType = CACHE_TYPE.REMOTE_SERVER_LISTS;
    	    }
    	}
    }
    
	private BasicCacheContainer createRemoteCacheUsingHotRodClient() throws ResourceException {
		
		File f = new File(this.getHotRodClientPropertiesFile());
		if (! f.exists()) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.clientPropertiesFileDoesNotExist", this.getHotRodClientPropertiesFile())); //$NON-NLS-1$

		}
		try {
			Properties props = PropertiesUtils.load(f.getAbsolutePath());
			RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);
			remoteCacheManager.start();
			remoteCacheManager.getCache();
			
			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using RemoteCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

			return remoteCacheManager;
		} catch (MalformedURLException e) {
			throw new ResourceException(e);
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		
    }
	
	private BasicCacheContainer createRemoteCacheFromServerList() throws ResourceException {
				
		Properties props = new Properties();
		props.put("infinispan.client.hotrod.server_list", this.getRemoteServerList()); //$NON-NLS-1$
        
        Collection args = new ArrayList();
        args.add((Object)props);
        
        RemoteCacheManager remoteCacheManager = BasicManagedConnectionFactory.getInstance(RemoteCacheManager.class, RemoteCacheManager.class.getName(), args, RemoteCacheManager.class);
        
 //       Object entity = ReflectionHelper.create(RemoteCacheManager.getClass.getName(), args, this.executionContext.getCommandContext().getVDBClassLoader());
		//RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);
 //       RemoteCacheManager remoteCacheManager = (RemoteCacheManager) entity;
		remoteCacheManager.start();
		
		LogManager
		.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

		return remoteCacheManager;
    }	
	
	private  BasicCacheContainer createLocalCacheViaJNDI() throws ResourceException {
		
	    Object cache = null;
		
	    String jndiName = getCacheJndiName();
        try {
            Context context = null;
            try {
                context = new InitialContext();
                cache = context.lookup(jndiName);
                
            } catch (NamingException err) {
            	throw new ResourceException(err);
            }          

            if (cache == null) {
				throw new ResourceException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.unableToFindCacheUsingJNDI", jndiName)); //$NON-NLS-1$
            } 	
            
			LogManager
			.logDetail(LogConstants.CTX_CONNECTOR,
					"=== Using CacheContainer (obtained by JNDI:", jndiName, "==="); //$NON-NLS-1 //$NON-NLS-2
        } catch (Exception err) {
            if (err instanceof RuntimeException) throw (RuntimeException)err;
            throw new ResourceException(err);
        }
        return (EmbeddedCacheManager) cache;
    }	
	
	private  BasicCacheContainer createLocalCacheFromConfigFile() throws ResourceException {
		try {
			BasicCacheContainer cc = new DefaultCacheManager(
					this.getConfigurationFileNameForLocalCache());

			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$
			
			return cc;
		} catch (IOException e) {
			throw new ResourceException(e);
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
