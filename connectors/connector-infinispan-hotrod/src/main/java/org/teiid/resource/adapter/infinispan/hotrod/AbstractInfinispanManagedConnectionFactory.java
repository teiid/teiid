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
package org.teiid.resource.adapter.infinispan.hotrod;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanPlugin;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.Version;


public abstract class AbstractInfinispanManagedConnectionFactory extends
		BasicManagedConnectionFactory {

	/**
	 */
	private static final long serialVersionUID = -4791974803005018658L;

	private enum CACHE_TYPE {
		USE_JNDI, REMOTE_SERVER_LISTS, REMOTE_HOT_ROD_PROPERTIES
	}
	
	private String remoteServerList = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName = null;
	private Class<?> cacheTypeClass = null; // cacheName ==> ClassType
	private String cacheTypes = null;
	@SuppressWarnings("rawtypes")
	protected Map<String, BaseMarshaller> messageMarshallerMap = null;
	private ClassRegistry methodUtil = new ClassRegistry();
	
	private String protobufDefFile = null;
	private String messageMarshallers = null;
	private String messageDescriptor = null;
	
	private boolean usingAnnotations = false;
	
	private RemoteCacheManager cacheContainer = null;
	private String stagingCacheName;
	private String aliasCacheName;
	private String pkKey;
	private Class<?> pkCacheKeyJavaType = null;
	private CACHE_TYPE cacheType;
	private String module;
	private ClassLoader cl;
	private CacheNameProxy cacheNameProxy;
	private Version version = null;



	@Override
	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
			throws ResourceException {
		
		// if all the properties are null,then its assumed the pojo has the protobuf annotations for indexing columns
		if (protobufDefFile == null && messageMarshallers == null && messageDescriptor == null) {
			usingAnnotations = true;
		} else {
			// if any of the following properties are specified, all 3 must be specified
			if (protobufDefFile == null) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25030));
			}
	
			if (messageMarshallers == null) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25029));
			}
			
			if (messageDescriptor == null) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25020));
			}
		}
		
		if (this.cacheTypes == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25021));
		}

		if (remoteServerList == null
				&& hotrodClientPropertiesFile == null && cacheJndiName == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25023));
		}

		determineCacheType();
		if (cacheType == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25022));
		}
		
		/*
		 * the creation of the cacheContainer has to be done within the
		 * call to get the connection so that the classloader is driven
		 * from the caller.
		 */
		return new BasicConnectionFactory<InfinispanConnectionImpl>() {

			private static final long serialVersionUID = 1L;

			@Override
			public InfinispanConnectionImpl getConnection()
					throws ResourceException {
				
				AbstractInfinispanManagedConnectionFactory.this.createCacheContainer();

				return new InfinispanConnectionImpl(AbstractInfinispanManagedConnectionFactory.this);
			}
		};

	}

	public String getCacheName() {
		// return the cacheName that is mapped as the alias
		return cacheNameProxy.getPrimaryCacheAliasName();
	}
	
	public String getCacheStagingName() {
		return cacheNameProxy.getStageCacheAliasName();
	}	
	
	@SuppressWarnings("rawtypes")
	public RemoteCache getCache(String cacheName) {
      if (cacheName == null) {
      	Assertion.isNotNull(cacheName, "Program Error: Cache Name is null");
      }

       return cacheContainer.getCache(cacheName);
      
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
		return cacheTypes;
	}

	/**
	 * Set the cache type mapping
	 * <code>cacheName:className[;pkFieldName[:cacheJavaType]]</code> that represent
	 * the root node class type for an available cache for access.
	 * The following is how the string parsed:
	 * <li>cacheNam = is the name used to retrieve the named cache
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
		this.cacheTypes = cacheTypeMap;
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
	
	/**
	 * Get the Protobuf Definition File Name
	 * 
	 * @return Name of the Protobuf Definition File
	 * @see #setProtobufDefinitionFile(String)
	 */
	public String getProtobufDefinitionFile() {
		return protobufDefFile;
	}

	/**
	 * Set the Google Protobuf Definition File name that describes the objects to be serialized.
	 * 
	 * @param protobufDefFile
	 *            the file name of the protobuf definition file to use
	 * @see #getProtobufDefinitionFile()
	 */
	public void setProtobufDefinitionFile(String protobufDefFile) {
		this.protobufDefFile = protobufDefFile;
	}

	/**
	 * Get the Message Marshaller class names
	 * 
	 * @return String comma delimited, class names of Message Marshallers
	 * @see #setMessageMarshallers(String)
	 */
	public String getMessageMarshallers() {
		return messageMarshallers;
	}

	/**
	 * Set the Protobin Marshallers pojoClassName:marshallerClassName[,pojoClassName:marshallerClassName,..]
	 * 
	 * @param messageMarshallers
	 *            the class names of the marshallers to use
	 * @see #getMessageMarshallers()
	 */
	public void setMessageMarshallers(String messageMarshallers) {
		this.messageMarshallers = messageMarshallers;
	}
	
	/**
	 * Get the Message descriptor class name for the root object in cache
	 * 
	 * @return Message Descriptor name
	 * @see #setMessageDescriptor(String)
	 */
	public String getMessageDescriptor() {
		return messageDescriptor;
	}

	/**
	 * Set the name of the Message Descriptor
	 * 
	 * @param messageDescriptor
	 *            the name of the message descriptor
	 * @see #getMessageDescriptor()
	 */
	public void setMessageDescriptor(String messageDescriptor) {
		this.messageDescriptor = messageDescriptor;
	}	
	
	public String getStagingCacheName() {
		return this.stagingCacheName;
	}

	/**
	 * An option to configure the staging cache name to use when using JDG to materialize data.
	 * @param cacheName
	 */
	protected void setStagingCacheName(String cacheName) {
		this.stagingCacheName = cacheName;
	}
	
	protected String getAliasCacheName() {
		return this.aliasCacheName;
	}
	
	/**
	 * An option to configure the alias cache name to use when using JDG to materialize data.
	 * @param cacheName
	 */
	protected void setAliasCacheName(String cacheName) {
		this.aliasCacheName = cacheName;
	}	

	public String getPk() {
		return pkKey;
	}
	
	/**
	 * This is an optional argument when defining the <code>CacheTypeMap</code>
	 * on the resouce adapter.
	 * 
	 * @return Class<?>
	 */
	public Class<?> getCacheKeyClassType() {
		return pkCacheKeyJavaType;
	}

	/**
	 * Return the Class that identifies the cache object 
	 * @return Class<?>
	 */
	public Class<?> getCacheClassType() {
		return this.cacheTypeClass;
	}

	public void setCacheClassTypeClass(Class<?> classCacheType) {
		this.cacheTypeClass = classCacheType;
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
	 * used when defining an Infinispan remote cache manager instance. If
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
	 * Get the name of the HotRod client properties file that should be used to
	 * configure a remoteCacheManager.
	 * 
	 * @return the name of the HotRod client properties file to be used to
	 *         configure remote cache manager
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
	 *            used to configure the remote cache manager
	 *            
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
	
	/** 
	 * Call to set the name of the cache to access when calling getCache
	 * @param cacheName
	 * @throws ResourceException 
	 */
	protected void setCacheName(String cacheName) throws ResourceException {
		if (getAliasCacheName() != null && getStagingCacheName() != null) {
			cacheNameProxy = new CacheNameProxy(cacheName, getStagingCacheName(),getAliasCacheName() );
			
		} else if (getStagingCacheName() != null || getAliasCacheName() != null)  {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25011));

		} else {
			cacheNameProxy = new CacheNameProxy(cacheName);
		}
	}
	
	public CacheNameProxy getCacheNameProxy() {
		return cacheNameProxy;
	}

	public boolean isAlive() {
		return this.cacheContainer != null;
	}
	
	protected RemoteCacheManager getCacheContainer() {
		return this.cacheContainer;
	}
	
	protected void setCacheContainer(RemoteCacheManager rcm) {
		this.cacheContainer = rcm;
	}
	
	abstract protected SerializationContext getContext();
	
	protected ClassLoader getClassLoader() {
		return this.cl;
	}
	
	protected Class<?> loadClass(String className) throws ResourceException {
		try {
			return Class.forName(className, false, getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new ResourceException(e);
		}
	}
	
	@SuppressWarnings("rawtypes")
	protected synchronized ClassLoader loadClasses() throws ResourceException {

		cl = null;

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
					}
				}
			} catch (ModuleLoadException e) {
				throw new ResourceException(e);
			}

		} 
		
		if (cl == null) {
			cl = Thread.currentThread().getContextClassLoader();
		}
		
			
		/*
		 * Parsing based on format:  cacheName:className[;pkFieldName[:cacheKeyJavaType]]
		 * 
		 */
		
		List<String> parms = StringUtil.getTokens(getCacheTypeMap(), ";"); //$NON-NLS-1$
		String leftside = parms.get(0);
		List<String> cacheClassparm = StringUtil.getTokens(leftside, ":");
		
		if (cacheClassparm.size() != 2) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25022));
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
					pkCacheKeyJavaType = loadClass(pktype);
				}
			}
		}
		
		if (!this.usingAnnotations) {
	
			List<String> marshallers = StringUtil.getTokens(this.getMessageMarshallers(), ","); //$NON-NLS-1$
			
			Map<String, BaseMarshaller> mmp = new HashMap<String, BaseMarshaller>(marshallers.size());
			
			for (String mm : marshallers) {
				
				List<String> marshallMap = StringUtil.getTokens(mm, ":"); //$NON-NLS-1$
				if (marshallMap.size() != 2) {
					throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25031, new Object[] {mm}));
				}
				final String clzName = marshallMap.get(0);
				final String m = marshallMap.get(1);
	
				try {
					Object bmi = (loadClass(m)).newInstance();
					Class ci = loadClass(clzName);
	
					mmp.put(clzName, (BaseMarshaller) bmi); 	
	
					methodUtil.registerClass(ci);
			
				} catch (InstantiationException e) {
					throw new ResourceException(e);
				} catch (IllegalAccessException e) {	
					throw new ResourceException(e);
				} catch (TranslatorException e) {
					throw new ResourceException(e);
				}
			
			}
			
			messageMarshallerMap=Collections.unmodifiableMap(mmp);
		}
		
		return cl;

	}

	protected synchronized void createCacheContainer() throws ResourceException {
		if (getCacheContainer() != null)
			return;
		
		RemoteCacheManager cc = null;


		ClassLoader lcl = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(
					this.getClass().getClassLoader());
		
			ClassLoader classLoader = loadClasses();

			switch (cacheType) {
			case USE_JNDI:
				cc = getRemoteCacheFromJNDI(this.getCacheJndiName(), classLoader);
				break;
	
			case REMOTE_HOT_ROD_PROPERTIES:
				cc = createRemoteCacheFromProperties(classLoader);
				break;
	
			case REMOTE_SERVER_LISTS:
				cc = createRemoteCacheFromServerList(classLoader);
				break;
	
			}

			setCacheContainer(cc);

			registerWithCacheManager(getContext(), cc, classLoader, this.usingAnnotations);
			
			// if configured for materialization, initialize the cacheNameProxy
			if (cacheNameProxy.getAliasCacheName() != null) {
				RemoteCache aliasCache = cc.getCache(cacheNameProxy.getAliasCacheName());
				if (aliasCache == null) {
					throw new ResourceException(	
							InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25010, new Object[] {cacheNameProxy.getAliasCacheName()}));
				}
				cacheNameProxy.initializeAliasCache(aliasCache);
			}

		
		} finally {
			Thread.currentThread().setContextClassLoader(lcl);
		}
		
		
	}	

	private void determineCacheType() {
		String jndiName = getCacheJndiName();
		if (jndiName != null && jndiName.trim().length() != 0) {
			cacheType = CACHE_TYPE.USE_JNDI;
		} else if (this.getHotRodClientPropertiesFile() != null) {
			cacheType = CACHE_TYPE.REMOTE_HOT_ROD_PROPERTIES;
		} else if (this.getRemoteServerList() != null
				&& !this.getRemoteServerList().isEmpty()) {
			cacheType = CACHE_TYPE.REMOTE_SERVER_LISTS;
		}
	}

	protected abstract RemoteCacheManager createRemoteCacheFromProperties(
			ClassLoader classLoader) throws ResourceException;
	
	protected abstract RemoteCacheManager createRemoteCacheFromServerList(
			ClassLoader classLoader) throws ResourceException;

	
	private RemoteCacheManager getRemoteCacheFromJNDI(
			String jndiName, ClassLoader classLoader) throws ResourceException {

		Object cache = null;
		try {
			Context context = new InitialContext();
			cache = context.lookup(jndiName);
		} catch (Exception err) {
			if (err instanceof RuntimeException)
				throw (RuntimeException) err;
			throw new ResourceException(err);
		}

		if (cache == null) {
			throw new ResourceException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25025, jndiName));
		}
		
		
		if (cache instanceof RemoteCacheManager) {
			LogManager.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded from JNDI " + jndiName + ") ==="); //$NON-NLS-1$

			return cacheContainer;
		}

		throw new ResourceException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25026, cacheContainer.getClass().getName()));

	}
	
	protected void registerWithCacheManager(SerializationContext ctx, RemoteCacheManager cc, ClassLoader cl, boolean useAnnotations) throws ResourceException {
		
	}
	
	public Version getVersion() throws TranslatorException {
		RemoteCache rc = this.getCache(this.getCacheNameProxy().getPrimaryCacheKey());
		return Version.getVersion(rc.getProtocolVersion());

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+  (protobufDefFile.hashCode());
		result = prime
				* result
				+ ((remoteServerList == null) ? 0 : remoteServerList.hashCode());
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
		AbstractInfinispanManagedConnectionFactory other = (AbstractInfinispanManagedConnectionFactory) obj;

		if (!checkEquals(this.remoteServerList, other.remoteServerList)) {
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

	public void cleanUp() {

		messageMarshallerMap = null;
		cacheType = null;
		cacheContainer = null;
		cl = null;
		methodUtil.cleanUp();

	}
}