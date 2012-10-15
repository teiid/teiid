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
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -9153717006234080627L;
	private String remoteServerList=null;
	private String configurationFileNameForLocalCache = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName=null;
	@SuppressWarnings("rawtypes")
	private Map<String, Class> typeMap = null;
	private String cacheTypes = null;
	private BasicCacheContainer cacheContainer = null;

	
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
	@SuppressWarnings("rawtypes")
	public void setCacheTypeMap(
			String cacheTypeMap) {
		if (this.cacheTypes == cacheTypeMap
				|| this.cacheTypes != null
				&& this.cacheTypes.equals(cacheTypeMap))
			return; // unchanged
		this.cacheTypes = cacheTypeMap;
		
		List<String> types = StringUtil.getTokens(this.cacheTypes, ",");
		typeMap = new HashMap<String,Class>(types.size());
		for(String type : types) {
			final List<String> mapped = StringUtil.getTokens(type, ":");
			final String cacheName = mapped.get(0);
			final String className = mapped.get(1);

			try {
				typeMap.put(cacheName, Class.forName(className));
			} catch (ClassNotFoundException e) {
				throw new TeiidRuntimeException("Class hasn't been added to the classpath: "+ e.getMessage());//$NON-NLS-1$
			}
			
		}
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
        if (this.remoteServerList == remoteServerList || this.remoteServerList != null
            && this.remoteServerList.equals(remoteServerList)) return; // unchanged
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
		if (this.configurationFileNameForLocalCache == configurationFileName
				|| this.configurationFileNameForLocalCache != null
				&& this.configurationFileNameForLocalCache.equals(configurationFileName))
			return; // unchanged
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
		if (this.hotrodClientPropertiesFile == propertieFileName
				|| this.hotrodClientPropertiesFile != null
				&& this.hotrodClientPropertiesFile.equals(propertieFileName))
			return; // unchanged
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

    
    
    protected BasicCacheContainer getCacheManager() {
    	return  this.cacheContainer;
    }
  
    
    protected void createCacheContainer() throws ResourceException {
    		this.cacheContainer=this.getLocalCacheContainer();
    		if (this.cacheContainer == null) {
    			this.cacheContainer= createRemoteCacheContainer();
    		}
    }
    
	private RemoteCacheManager createRemoteCacheContainer() throws ResourceException {
		
		RemoteCacheManager container = null;
		if (this.getHotRodClientPropertiesFile() != null) {
			File f = new File(this.getHotRodClientPropertiesFile());
			if (! f.exists()) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.getString("InfinispanManagedConnectionFactory.clientPropertiesFileDoesNotExist", this.getHotRodClientPropertiesFile())); //$NON-NLS-1$

			}
			try {
				Properties props = PropertiesUtils.load(f.getAbsolutePath());
				container = new RemoteCacheManager(props);
				container.start();
				container.getCache();
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
					|| !this.getRemoteServerList().isEmpty()) { //$NON-NLS-1$
				
				Properties props = new Properties();
				props.put("infinispan.client.hotrod.server_list", this.getRemoteServerList());
				container = new RemoteCacheManager(props);
				container.start();
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$
			}
		}

		return container;
    }
	protected BasicCacheContainer getLocalCacheContainer() throws ResourceException {
	  	
	    Object cache = null;
		if (this.getConfigurationFileNameForLocalCache() != null) {
			
			DefaultCacheManager container;
			try {
				container = new DefaultCacheManager(
						this.getConfigurationFileNameForLocalCache());
			} catch (IOException e) {
				throw new ResourceException(e);
			}
			LogManager
					.logInfo(LogConstants.CTX_CONNECTOR,
							"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$
			return container;

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
	    				throw new ResourceException("Unable to find cache using JNDI: " + jndiName);  //$NON-NLS-1$	            	
	            } 	
	            
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using CacheContainer (obtained by JNDI: " + jndiName + " ==="); //$NON-NLS-1$

	            
	           return   (BasicCacheContainer) cache;
	        } catch (Exception err) {
	            if (err instanceof RuntimeException) throw (RuntimeException)err;
	            throw new ResourceException(err);
	        }
	    } 
	    return null;
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
