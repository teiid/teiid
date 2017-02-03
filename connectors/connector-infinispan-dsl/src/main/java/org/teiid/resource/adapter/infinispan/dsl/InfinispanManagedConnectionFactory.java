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
package org.teiid.resource.adapter.infinispan.dsl;

import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.adapter.infinispan.dsl.schema.AnnotationSchema;
import org.teiid.resource.adapter.infinispan.dsl.schema.ProtobufSchema;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;



public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);

	/**
	 */
	private static final long serialVersionUID = -4791974803005018658L;

	enum CACHE_TYPE {
		USE_JNDI, REMOTE_SERVER_LISTS, REMOTE_HOT_ROD_PROPERTIES
	}
	
	private String remoteServerList = null;
	private String hotrodClientPropertiesFile = null;
	private String cacheJndiName = null;
	private Class<?> cacheTypeClass = null; // cacheName ==> ClassType
	private String cacheTypes = null;
	private ClassRegistry methodUtil = new ClassRegistry();
	
	private String protobufDefFile = null;
	private String messageMarshallers = null;
	private String messageDescriptor = null;
	
	private String childClasses= null;
	
	private boolean usingAnnotations = false;
	
	private String stagingCacheName;
	private String aliasCacheName;
	private String pkKey;
	private Class<?> pkCacheKeyJavaType = null;
	private CACHE_TYPE cacheType;
	private String module;
	private ClassLoader cl;
	private CacheNameProxy cacheNameProxy;
	private InfinispanSchemaDefinition cacheSchemaConfigurator;
	private boolean initialized = false;
	
	/* properties for JDG authentication, used for materialization use cases or when only a single user account it used */
	private String authUserName = null;
	private String authPassword = null;
	private String authServerName = null;
	private String authSASLMechanism = null;
	private String authApplicationRealm = null;
	private String adminUserName = null;
	private String adminPassword = null;
	
	@Override
	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
			throws ResourceException {
		
		validation();
		
		return new InfinispanConnectionFactory(this);

	}
	
	class InfinispanConnectionFactory extends BasicConnectionFactory<InfinispanConnectionImpl>{

		InfinispanManagedConnectionFactory factory;
		
		public InfinispanConnectionFactory(InfinispanManagedConnectionFactory IMfactory) {
			factory = IMfactory;
		}
		private static final long serialVersionUID = 3802635158148246427L;
		@Override
		public InfinispanConnectionImpl getConnection()
				throws ResourceException {
			
			InfinispanManagedConnectionFactory.this.loadClasses();

			return new InfinispanConnectionImpl(factory);
		}

	}
	
	 synchronized void validation() throws ResourceException {
		if (initialized) return;
		
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
		
		if ((adminUserName != null && adminPassword == null) || (adminUserName == null && adminPassword != null)) {
			throw new InvalidPropertyException("AdminUserName and AdminPassword must be specfied");
		} else if (adminUserName != null && adminPassword != null && authApplicationRealm == null) {
			throw new InvalidPropertyException("AuthApplicationRealm must be specfied");
		}

		if ((authUserName != null && authPassword == null) || (authUserName == null && authPassword != null)) {
			throw new InvalidPropertyException("AuthUserName and AuthPassword must be specfied");
		} else if (authUserName != null && authPassword != null && authApplicationRealm == null) {
			throw new InvalidPropertyException("AuthApplicationRealm must be specfied");
		}

		if ((authServerName != null && authSASLMechanism == null)
				|| (authServerName == null && authSASLMechanism != null)) {
			throw new InvalidPropertyException("AuthServerName and AuthSASMechanism must be specfied");
		}
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
	 * select name, email, id from Person
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
	 * Returns a comma separated list of child class names that are registered
	 * in the JDG schema
	 * 
	 * @return childClasses
	 */
	public String getChildClasses() {
		return childClasses;
	}

	/**
	 * Sets a comma separated list of class names to register in the JDG schema
	 * 
	 * @param childClasses
	 *            Sets childClasses to the specified value.
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
	
	public String getAuthUserName() {
		return authUserName;
	}

	public void setAuthUserName(String username) {
		this.authUserName = username;
	}

	public String getAuthPassword() {
		return authPassword;
	}

	public void setAuthPassword(String password) {
		this.authPassword = password;
	}
	
	public String getAuthServerName() {
		return authServerName;
	}

	public void setAuthServerName(String authServerName) {
		this.authServerName = authServerName;
	}

	public String getAuthSASLMechanism() {
		return authSASLMechanism;
	}

	public void setAuthSASLMechanism(String authSASLMechanism) {
		this.authSASLMechanism = authSASLMechanism;
	}

	public String getAuthApplicationRealm() {
		return authApplicationRealm;
	}

	public void setAuthApplicationRealm(String authApplicationRealm) {
		this.authApplicationRealm = authApplicationRealm;
	}

	public String getAdminUserName() {
		return adminUserName;
	}

	public void setAdminUserName(String adminUserName) {
		this.adminUserName = adminUserName;
	}

	public String getAdminPassword() {
		return adminPassword;
	}

	public void setAdminPassword(String adminPassword) {
		this.adminPassword = adminPassword;
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

    public CACHE_TYPE getCacheType() {
    	return cacheType;
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
		if (initialized) {
			return this.getClassLoader();
		}
	
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
	
		methodUtil.registerClass(cacheTypeClass);
			
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
		
		cacheSchemaConfigurator.initialize(this, methodUtil);
		
		initialized = true;

		return cl;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+  ((protobufDefFile == null) ? 0 : protobufDefFile.hashCode());
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
		if (this.remoteServerList == null) {
			if (other.remoteServerList != null) {
				return false;
			}
		} else if (!checkEquals(this.remoteServerList, other.remoteServerList)) {
				return false;
		}
		if (this.hotrodClientPropertiesFile == null) {
			if (other.hotrodClientPropertiesFile != null) {
				return false;
			}
		} else if (!checkEquals(this.hotrodClientPropertiesFile,
				other.hotrodClientPropertiesFile)) {
			return false;
		}
		if (this.cacheJndiName == null) {
			if (other.cacheJndiName != null) {
				return false;
			}
		} else if (!checkEquals(this.cacheJndiName, other.cacheJndiName)) {
			return false;
		}
		if (this.protobufDefFile == null) {
			if (other.protobufDefFile != null) {
				return false;
			}
		} else if (!checkEquals(this.protobufDefFile, other.protobufDefFile)) {
			return false;
		}
		return false;

	}

	public void cleanUp() {

		cacheType = null;
		cl = null;
		methodUtil.cleanUp();

	}
	
}

