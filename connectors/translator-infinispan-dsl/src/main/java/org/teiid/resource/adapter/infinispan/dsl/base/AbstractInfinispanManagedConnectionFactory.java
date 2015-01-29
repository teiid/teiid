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
package org.teiid.resource.adapter.infinispan.dsl.base;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.ClassRegistry;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;


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
	private Map<String, Class<?>> typeMap = null; // cacheName ==> ClassType
	private String cacheTypes = null;
	@SuppressWarnings("rawtypes")
	private Map<String, BaseMarshaller> messageMarshallerMap = null;
	private ClassRegistry methodUtil = new ClassRegistry();
	
	private String protobufDefFile = null;
	private String messageMarshallers = null;
	private String messageDescriptor = null;
	
	private RemoteCacheManager cacheContainer = null;
	private Map<String, String> pkMap; // cacheName ==> pkey name
	private CACHE_TYPE cacheType;
	private String module;
	private ClassLoader cl;


	@Override
	public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
			throws ResourceException {
		
		if (protobufDefFile == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25030));
		}

		if (messageMarshallers == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25029));
		}
		
		if (messageDescriptor == null) {
			throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25020));
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

	/**
	 * Get the <code>cacheName:ClassName[,cacheName:ClassName...]</code> cache
	 * type mappings.
	 * 
	 * @return <code>cacheName:ClassName[,cacheName:ClassName...]</code> cache
	 *         type mappings
	 * @see #setCacheTypeMap(String)
	 */
	public String getCacheTypeMap() {
		return cacheTypes;
	}

	/**
	 * Set the cache type mapping
	 * <code>cacheName:ClassName[,cacheName:ClassName...]</code> that represent
	 * the root node class type for 1 or more caches available for access.
	 * 
	 * @param cacheTypeMap
	 *            the cache type mappings passed in the form of
	 *            <code>cacheName:ClassName[,cacheName:ClassName...]</code>
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
	 * Set the Protobin Marshallers classname[,classname,..]
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
			return Class.forName(className, true, getClassLoader());
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

		} else {
			cl = this.getClass().getClassLoader();
		}
		
		List<String> types = StringUtil.getTokens(getCacheTypeMap(), ","); //$NON-NLS-1$

		Map<String, String> pkMap = new HashMap<String, String>(types.size());
		Map<String, Class<?>> tm = new HashMap<String, Class<?>>(types.size());

		for (String type : types) {
			List<String> mapped = StringUtil.getTokens(type, ":"); //$NON-NLS-1$
			if (mapped.size() != 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25022));
			}
			final String cacheName = mapped.get(0);
			String className = mapped.get(1);
			mapped = StringUtil.getTokens(className, ";"); //$NON-NLS-1$
			if (mapped.size() > 1) {
				className = mapped.get(0);
				pkMap.put(cacheName, mapped.get(1));
			}
			tm.put(cacheName, loadClass(className)); 

		}

		List<String> marshallers = StringUtil.getTokens(this.getMessageMarshallers(), ","); //$NON-NLS-1$
		
		Map<String, BaseMarshaller> mmp = new HashMap<String, BaseMarshaller>(marshallers.size());
		
		for (String mm : marshallers) {
			
			List<String> mapped = StringUtil.getTokens(mm, ":"); //$NON-NLS-1$
			if (mapped.size() != 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25031, new Object[] {mm}));
			}
			final String className = mapped.get(0);
			final String m = mapped.get(1);

			try {
				Object bmi = (loadClass(m)).newInstance();
				Class ci = loadClass(className);

				mmp.put(className, (BaseMarshaller) bmi); 	

				methodUtil.registerClass(ci);
		
			} catch (InstantiationException e) {
				throw new ResourceException(e);
			} catch (IllegalAccessException e) {	
				throw new ResourceException(e);
			} catch (TranslatorException e) {
				throw new ResourceException(e);
			}
		
		}
		
		setCacheNameClassTypeMapping(Collections.unmodifiableMap(tm));
		setPkMap(Collections.unmodifiableMap(pkMap));
		
		messageMarshallerMap=Collections.unmodifiableMap(mmp);

		return cl;

	}

	protected synchronized void createCacheContainer() throws ResourceException {
		if (getCacheContainer() != null)
			return;
		
		RemoteCacheManager cc = null;

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(
					this.getClass().getClassLoader());
		
			ClassLoader classLoader = loadClasses();

			switch (cacheType) {
			case USE_JNDI:
				cc = createRemoteCacheWrapperFromJNDI(this.getCacheJndiName(), classLoader);
				break;
	
			case REMOTE_HOT_ROD_PROPERTIES:
				cc = createRemoteCacheWrapperFromProperties(classLoader);
				break;
	
			case REMOTE_SERVER_LISTS:
				cc = createRemoteCacheWrapperFromServerList(classLoader);
				break;
	
			}

			setCacheContainer(cc);
			registerMarshallers(getContext(), classLoader);

		
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
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

	protected abstract RemoteCacheManager createRemoteCacheWrapperFromProperties(
			ClassLoader classLoader) throws ResourceException;
	
	protected abstract RemoteCacheManager createRemoteCacheWrapperFromServerList(
			ClassLoader classLoader) throws ResourceException;

	
	private RemoteCacheManager createRemoteCacheWrapperFromJNDI(
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void registerMarshallers(SerializationContext ctx, ClassLoader cl) throws ResourceException {

		try {
			FileDescriptorSource fds = new FileDescriptorSource();
			fds.addProtoFile("protofile", cl.getResourceAsStream(getProtobufDefinitionFile() ) );
			
			ctx.registerProtoFiles( fds );

			List<Class<?>> registeredClasses = methodUtil.getRegisteredClasses();
			for (Class clz:registeredClasses) {
				BaseMarshaller m = messageMarshallerMap.get(clz.getName());
				ctx.registerMarshaller(m);				
			}

		} catch (IOException e) {
			throw new ResourceException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25032), e);
		} 
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
		typeMap = null;
		cacheContainer = null;
		pkMap = null;
		cl = null;

	}
}
