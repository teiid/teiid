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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.hotrod.schema.AnnotationSchema;
import org.teiid.resource.adapter.infinispan.hotrod.schema.ProtobufSchema;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanPlugin;
import org.teiid.translator.infinispan.hotrod.ProtobufDataTypeManager;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.util.Version;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);

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
	private ClassRegistry methodUtil = new ClassRegistry(new ProtobufDataTypeManager());
	
	private String protobufDefFile = null;
	private String messageMarshallers = null;
	private String messageDescriptor = null;
	
	private String childClasses= null;
	
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
	private InfinispanSchemaDefinition cacheSchemaConfigurator;

	@Override
	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
			throws ResourceException {
		
		// if all the properties are null,then its assumed the pojo has the protobuf annotations for indexing columns
		if (protobufDefFile == null && messageMarshallers == null && messageDescriptor == null) {
			usingAnnotations = true;
			
			cacheSchemaConfigurator = new AnnotationSchema();
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
			cacheSchemaConfigurator = new ProtobufSchema();
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
				
				InfinispanManagedConnectionFactory.this.createCacheContainer();

				return new InfinispanConnectionImpl(InfinispanManagedConnectionFactory.this);
			}
		};

	}
	
	public InfinispanSchemaDefinition getCacheSchemaConfigurator() {
		return cacheSchemaConfigurator;
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
	 * Get the <code>cacheName:className[:pkFieldName[:cacheJavaType]]</code> cache
	 * type mappings.
	 * 
	 * @return <code>cacheName:className[:pkFieldName[:cacheJavaType]]</code> cache
	 *         type mappings
	 * @see #setCacheTypeMap(String)
	 */
	public String getCacheTypeMap() {
		return cacheTypes;
	}

	// 	 TEIID-4582 support all colon separators
	/**
	 * Set the cache type mapping
	 * <code>cacheName:className[:pkFieldName[:cacheJavaType]]</code> or 
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
	 *            <code>cacheName:className[:pkFieldName[:cacheJavaType]]</code>
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
	
   /**
    * Returns a comma separated list of child class names that are
    * registered in the JDG schema
    * 
    * @return childClasses
    */
   public String getChildClasses() {
           return childClasses;
   }

   /**
    * Sets a comma separated list of class names to register in the JDG schema
    * 
    * @param childClasses Sets childClasses to the specified value.
    */
   public void setChildClasses(String childClasses) {
           this.childClasses = childClasses;
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
	
    public boolean configuredUsingAnnotations() {
    	return this.usingAnnotations;
    }

	public boolean isAlive() {
		return this.cacheContainer != null;
	}
	
	public RemoteCacheManager getCacheContainer() {
		return this.cacheContainer;
	}
	
	protected void setCacheContainer(RemoteCacheManager rcm) {
		this.cacheContainer = rcm;
	}
	

	public SerializationContext getContext() {
		return ProtoStreamMarshaller.getSerializationContext(this
				.getCacheContainer());
	}
	
	public ClassLoader getClassLoader() {
		return this.cl;
	}
	
	public Class<?> loadClass(String className) throws ResourceException {
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
		String cacheName = null;
		String className = null;
		String pkFieldName = null;
		String cacheKeyJavaType = null;
		
		if (getCacheTypeMap().contains(";")) {
			List<String> p = StringUtil.getTokens(getCacheTypeMap(), ";"); //$NON-NLS-1$
			String leftside = p.get(0);
			List<String> cacheClassparm = StringUtil.getTokens(leftside, ":");
						
			if (cacheClassparm.size() != 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25022));
			}
			
			cacheName = cacheClassparm.get(0);
			className = cacheClassparm.get(1);
			
			if (p.size() == 2) {
				String rightside = p.get(1);
				List<String> pkKeyparm = StringUtil.getTokens(rightside, ":");
				pkFieldName = pkKeyparm.get(0);
				if (pkKeyparm.size() == 2) {
					cacheKeyJavaType = pkKeyparm.get(1);
				}
			}

		} else {
			List<String> parms = StringUtil.getTokens(getCacheTypeMap(), ":"); //$NON-NLS-1$
			if (parms.size() < 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25022));
			}
			
			cacheName = parms.get(0);
			className = parms.get(1);
			
			if (parms.size() > 2) {
				pkFieldName = parms.get(2);
				if (parms.size() == 4) {
					cacheKeyJavaType = parms.get(3);
				}
			}
			
		}
		setCacheName(cacheName);
		cacheTypeClass = loadClass(className);
	
		methodUtil.registerClass(cacheTypeClass);
		
		if (pkFieldName != null) pkKey = pkFieldName;
		if (cacheKeyJavaType != null) pkCacheKeyJavaType = getPrimitiveClass(cacheKeyJavaType);
		
		cacheSchemaConfigurator.initialize(this, methodUtil);

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

			registerWithCacheManager();
			
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

	protected RemoteCacheManager createRemoteCacheFromProperties(
			ClassLoader classLoader) throws ResourceException {
		File f = new File(this.getHotRodClientPropertiesFile());
		if (!f.exists()) {
			throw new InvalidPropertyException(
					InfinispanManagedConnectionFactory.UTIL.getString(
							"clientPropertiesFileDoesNotExist",
							f.getAbsoluteFile()));

		}
		try {
			Properties props = PropertiesUtils.load(f.getAbsolutePath());

			LogManager
					.logInfo(
							LogConstants.CTX_CONNECTOR,
							"=== Using RemoteCacheManager (created from properties file " + f.getAbsolutePath() + ") ==="); //$NON-NLS-1$

			return createRemoteCache(props, classLoader);

		} catch (Exception err) {
			throw new ResourceException(err);
		}

	}
	
	protected  RemoteCacheManager createRemoteCacheFromServerList(
			ClassLoader classLoader) throws ResourceException {
		Properties props = new Properties();
		props.put(
				"infinispan.client.hotrod.server_list", this.getRemoteServerList()); //$NON-NLS-1$

		LogManager.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

		return createRemoteCache(props, classLoader);
	}
	
	private RemoteCacheManager createRemoteCache(Properties props,
			ClassLoader classLoader) throws ResourceException {
		RemoteCacheManager remoteCacheManager;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.marshaller(new ProtoStreamMarshaller());
			cb.withProperties(props);
			if (classLoader != null)
				cb.classLoader(classLoader);
			remoteCacheManager = new RemoteCacheManager(cb.build(), true);

		} catch (Exception err) {
			throw new ResourceException(err);
		}

		return remoteCacheManager;

	}	

	
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
	
	protected void registerWithCacheManager() throws ResourceException {
		this.cacheSchemaConfigurator.registerSchema(this);
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
		InfinispanManagedConnectionFactory other = (InfinispanManagedConnectionFactory) obj;

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

		cacheType = null;
		cacheContainer = null;
		cl = null;
		methodUtil.cleanUp();

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
	
}

