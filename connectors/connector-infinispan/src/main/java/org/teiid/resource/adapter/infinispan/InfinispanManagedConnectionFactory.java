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

import org.infinispan.client.hotrod.RemoteCacheManager;
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
	private Map<String, Class<?>> typeMap = null;  // cacheName ==> ClassType
	private String cacheTypes = null;
	
	private ContainerWrapper cacheContainer = null;
	private Map<String, String> pkMap; //  cacheName ==> pkey name
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
        
        
		return new BasicConnectionFactory<InfinispanConnectionImpl>() {
			
			private static final long serialVersionUID = 2579916624625349535L;

			@Override
			public InfinispanConnectionImpl getConnection() throws ResourceException {
				
				if (InfinispanManagedConnectionFactory.this.getCacheNameClassTypeMapping() == null) {
				
					InfinispanManagedConnectionFactory.this.loadClasses();
				}
				
				InfinispanManagedConnectionFactory.this.createCacheContainer();
				
				return new InfinispanConnectionImpl(InfinispanManagedConnectionFactory.this);
			}
		};
			

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

	/**
	 * Sets the (optional) module(s) where the ClassName class is defined, that will be loaded <code> (module,[module,..])</code>
	 * @param module
	 * @see #getModule
	 */
	public void setModule(String module) {
		this.module = module;
	}
	
	/**
	 * Called to get the module(s) that are to be loaded
	 * @see #setModule
	 * @return
	 */
	public String getModule() {
		return module;
	}
	
	public String getPkMap(String cacheName) {
		return pkMap.get(cacheName);
	}

	public void setPkMap(Map<String, String> mapOfPKs) {
		pkMap = mapOfPKs;
	}	
	
	public Map<String, Class<?>> getCacheNameClassTypeMapping() {
		return this.typeMap;
	}
	
	public void setCacheNameClassTypeMapping(Map<String, Class<?>> cacheType) {
		this.typeMap = cacheType;
	}
	
	public Class<?> getCacheType(String cacheName) {
		return this.typeMap.get(cacheName);
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
	 * configure a local {@link EmbeddedCacheManager} using {@link DefaultCacheManager}.
	 * 
	 * @return the name of the resource or file configuration that should be
	 *         passed to the cache container
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
	 *            the name of the configuration file that should be used to load
	 *            the cacheContainer
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
    
	protected synchronized void loadClasses()  throws ResourceException {
		
		ClassLoader cl = null;
		if (getModule() != null) {
  
                Module m = null;
                try {
            		List<String> mods = StringUtil.getTokens(getModule(), ","); //$NON-NLS-1$
            		for (String mod : mods) {   	
            			Module x = Module.getCallerModuleLoader().loadModule(ModuleIdentifier.create(mod));
            			// the first entry must be the module associated with the cache
            			if (m == null) m = x;
            		}
                } catch (ModuleLoadException e) {
                    throw new ResourceException(e);
                }
                cl = m.getClassLoader();

		} else {
			cl = Thread.currentThread().getContextClassLoader();
		}
	
		List<String> types = StringUtil.getTokens(getCacheTypeMap(), ","); //$NON-NLS-1$

		Map<String, String> pkMap = new HashMap<String, String>(types.size());
		Map<String, Class<?>> tm = new HashMap<String, Class<?>>(types.size());

		for (String type : types) {
			List<String> mapped = StringUtil.getTokens(type, ":"); //$NON-NLS-1$
			if (mapped.size() != 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.invalidServerConfiguration"));
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
		
		setCacheNameClassTypeMapping(Collections.unmodifiableMap(tm));
		setPkMap(Collections.unmodifiableMap(pkMap));

	}
   
    
    protected synchronized void createCacheContainer() throws ResourceException {
    	if (this.cacheContainer != null) return;
    	
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    	
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
    	
    	} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
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
    
	private ContainerWrapper createRemoteCacheUsingHotRodClient() throws ResourceException {
		
		File f = new File(this.getHotRodClientPropertiesFile());
		if (! f.exists()) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.clientPropertiesFileDoesNotExist", this.getHotRodClientPropertiesFile())); //$NON-NLS-1$

		}
		try {
			Properties props = PropertiesUtils.load(f.getAbsolutePath());
			final RemoteCacheManager remoteCacheManager = new RemoteCacheManager(props);
			remoteCacheManager.start();
			remoteCacheManager.getCache();
			
			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using RemoteCacheManager (loaded by configuration) ==="); //$NON-NLS-1$
			
			return new ContainerWrapper() {
				
				@Override
				public Map<Object,Object> getCache(String cacheName) {
			   		if (cacheName == null) {
		    			return remoteCacheManager.getCache();
		    		}
					return remoteCacheManager.getCache(cacheName);		
				}	
				public void cleanUp() {
					remoteCacheManager.stop();
				}
			};
		} catch (MalformedURLException e) {
			throw new ResourceException(e);
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		
    }
	
	private ContainerWrapper createRemoteCacheFromServerList() throws ResourceException {
				
		Properties props = new Properties();
		props.put("infinispan.client.hotrod.server_list", this.getRemoteServerList()); //$NON-NLS-1$
        
        Collection args = new ArrayList();
        args.add((Object)props);
        
        final RemoteCacheManager remoteCacheManager = BasicManagedConnectionFactory.getInstance(RemoteCacheManager.class, RemoteCacheManager.class.getName(), args, RemoteCacheManager.class);
        
 		remoteCacheManager.start();
		
		LogManager
		.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$
		
		return new ContainerWrapper() {
			
			@Override
			public Map<Object,Object> getCache(String cacheName) {
		   		if (cacheName == null) {
	    			return remoteCacheManager.getCache();
	    		}
				return remoteCacheManager.getCache(cacheName);		
			}	
			public void cleanUp() {
				remoteCacheManager.stop();
			}
		};
    }	
	
	private  ContainerWrapper createLocalCacheViaJNDI() throws ResourceException {

	    String jndiName = getCacheJndiName();
        try {                
                final Object cache = performJNDICacheLookup(jndiName);
                
    			LogManager
    			.logDetail(LogConstants.CTX_CONNECTOR,
    					"=== Using CacheContainer (obtained by JNDI:", jndiName, "==="); //$NON-NLS-1 //$NON-NLS-2

    			return new ContainerWrapper() {
    				
    				@Override
    				public Map<Object,Object> getCache(String cacheName) {
    			   		if (cacheName == null) {
    		    			return ((EmbeddedCacheManager) cache).getCache();
    		    		}
    					return ((EmbeddedCacheManager) cache).getCache(cacheName);		
    				}	
    				public void cleanUp() {
    					((EmbeddedCacheManager) cache).stop();
    				}
    			};   			        

        } catch (ResourceException re) {
        	throw re;
        } catch (Exception err) {
            if (err instanceof RuntimeException) throw (RuntimeException)err;
            throw new ResourceException(err);
        }
        
    }	
	
	/* split out for testing purposes */
	Object performJNDICacheLookup(String jndiName) throws Exception {
        Context context = null;

        context = new InitialContext();
        final Object cache = context.lookup(jndiName);        

        if (cache == null) {
			throw new ResourceException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.unableToFindCacheUsingJNDI", jndiName)); //$NON-NLS-1$
        } 	

        return cache;
	}
	
	private  ContainerWrapper createLocalCacheFromConfigFile() throws ResourceException {
		try {
			final DefaultCacheManager cc = new DefaultCacheManager(
					this.getConfigurationFileNameForLocalCache());

			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$
			
			return new ContainerWrapper() {
				
				@Override
				public Map<Object,Object> getCache(String cacheName) {
			   		if (cacheName == null) {
		    			return cc.getCache();
		    		}
					return cc.getCache(cacheName);		
				}		
				public void cleanUp() {
					cc.stop();
				}
			};
			
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
	
	
	public void cleanUp() {
		
		typeMap = null;		
		if (cacheContainer != null) cacheContainer.cleanUp();
		pkMap = null;

	}

	
}	
	
	interface ContainerWrapper {
		public Map<Object,Object> getCache(String cacheName);
		public void cleanUp();
	}
	


