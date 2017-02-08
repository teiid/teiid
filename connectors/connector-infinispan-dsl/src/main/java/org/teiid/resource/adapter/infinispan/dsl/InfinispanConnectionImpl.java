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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanDSLConnection;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;
import org.teiid.translator.object.DDLHandler;
import org.teiid.translator.object.SearchType;
import org.teiid.translator.object.Version;


/** 
 * Represents a connection to an Infinispan cache container. The <code>cacheName</code> that is specified will dictate the
 * cache to be accessed in the container.
 * 
 */
public class InfinispanConnectionImpl extends BasicConnection implements InfinispanDSLConnection{ 
	
	InfinispanManagedConnectionFactory config = null;
	
	private RemoteCacheManager cacheContainer = null;
	
	private SerializationContext context = null;
	
	private boolean adminUsage = false;


	public InfinispanConnectionImpl(InfinispanManagedConnectionFactory config)  throws ResourceException {
		this.config = config;
		
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan Connection has been newly created "); //$NON-NLS-1$
	}	

	@Override
	public Version getVersion() throws TranslatorException {
		RemoteCache rc = this.getCache(config.getCacheNameProxy().getPrimaryCacheKey());
		return Version.getVersion(rc.getProtocolVersion());
	}
	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		config = null;
		context = null;
		if (cacheContainer != null)
			cacheContainer.stop();
		
		cacheContainer=null;
	}
	
	@Override
	public void cleanUp() {
		if (cacheContainer != null) {
			cacheContainer.stop();
		}
		cacheContainer = null;
	}

	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	@Override
	public boolean isAlive() {
		if (config == null || cacheContainer == null) return false;
		boolean alive = false;
		try {
			 this.getCache();
			 alive = true;
		} catch (Throwable t) {
			
		}
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Infinispan Remote Cache Connection is alive:", alive); //$NON-NLS-1$
		return alive;
	}	

	@Override
	public Class<?> getCacheClassType() throws TranslatorException {		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "=== GetType for cache :", getCacheName(),  "==="); //$NON-NLS-1$ //$NON-NLS-2$

		Class<?> type = config.getCacheClassType();
		if (type != null) {
			return type;
		}
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25040, getCacheName()));

	}
	
	@Override
	public Class<?> getCacheKeyClassType()  {
		return config.getCacheKeyClassType();
	}
	

	@SuppressWarnings({ "rawtypes"})
	@Override
	public RemoteCache getCache() throws TranslatorException {
		return getTargetCache();
	}

	/**
	 * {@inheritDoc}
	 * Called to return the descriptor based on the root class
	 */
	@Override
	public Descriptor getDescriptor()
			throws TranslatorException {
		
		return getDescriptor(config.getCacheClassType());

	}
	
	@Override
	public Descriptor getDescriptor(Class<?> clz)
			throws TranslatorException {
		this.adminUsage = true;
		// check that its been created.
		this.getCacheContainer();
		
		return config.getCacheSchemaConfigurator().getDecriptor(config, this, clz);

	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		
		return Search.getQueryFactory(getCache());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#add(java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void add(Object key, Object value) throws TranslatorException {
		getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key) throws TranslatorException {
		return getCache().remove(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void update(Object key, Object value) throws TranslatorException {
		getCache().replace(key, value);
	}
	private RemoteCache getTargetCache() throws TranslatorException {
		final String cacheName = getTargetCacheName();
		
	    if (cacheName == null) {
	       Assertion.isNotNull(cacheName, "Program Error: Cache Name is null");
	    }

	    return getCacheContainer().getCache(cacheName);
	}

	private String getTargetCacheName() {
		try {
			if (getDDLHandler().isStagingTarget()) {
				return config.getCacheNameProxy().getStageCacheAliasName(this);
			}
			return config.getCacheNameProxy().getPrimaryCacheAliasName(this);
		} catch (TranslatorException te) {
			throw new RuntimeException(te);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getClassRegistry()
	 */
	@Override
	public org.teiid.translator.object.ClassRegistry getClassRegistry() {
		return config.getClassRegistry();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key) throws TranslatorException {
		return getCache().get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getAll()
	 */
	@Override
	public Collection<Object> getAll() throws TranslatorException {
		DSLSearch s = (DSLSearch) getSearchType();
		
		return s.getAll();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getDDLHandler()
	 */
	@Override
	public DDLHandler getDDLHandler() {
		return config.getCacheNameProxy().getDDLHandler();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getPkField()
	 */
	@Override
	public String getPkField() {
		return config.getPk();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName() {
		return getTargetCacheName();
	}

	/**
	 * {@inheritDoc}
	 * @throws ResourceException 
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public RemoteCache getCache(String cacheName) throws TranslatorException {
		return getCacheContainer().getCache(cacheName);

	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {
		getCache(cacheName).clear();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getSearchType()
	 */
	@Override
	public SearchType getSearchType() {
		return new DSLSearch(this);
	}
	
	/**
	* Call to determine if the JDG cache is configured using annotation (or using protobuf and marsharllers).
	* @return true if annotations are used
	*/

	@Override
	 public boolean configuredUsingAnnotations() {
	        return config.configuredUsingAnnotations();
	}

	@Override
	public boolean configuredForMaterialization() {
		return (config.getStagingCacheName() != null);
	}
	
	private synchronized RemoteCacheManager getCacheContainer() throws TranslatorException {
		if (this.cacheContainer != null) return this.cacheContainer;
		
		try {
			createCacheContainer();
		} catch (ResourceException e) {
			// TODO Auto-generated catch block
			throw new TranslatorException(e);
		}
		return this.cacheContainer;				
	}
	
	protected synchronized void createCacheContainer() throws ResourceException {

		RemoteCacheManager cc = null;


		ClassLoader lcl = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(
					config.getRAClassLoader());
			
			switch (config.getCacheType()) {
			case USE_JNDI:
				cc = getRemoteCacheFromJNDI(config.getCacheJndiName());
				break;

			case REMOTE_HOT_ROD_PROPERTIES:
				cc = createRemoteCacheFromProperties();
				break;

			case REMOTE_SERVER_LISTS:
				cc = createRemoteCacheFromServerList();
				break;

			}

			this.cacheContainer = cc;

			this.context = ProtoStreamMarshaller.getSerializationContext(this.cacheContainer);

			// if configured for materialization, initialize the cacheNameProxy
			if (config.getCacheNameProxy().getAliasCacheName() != null) {
				RemoteCache aliasCache = cc.getCache(config.getCacheNameProxy().getAliasCacheName());
				if (aliasCache == null) {
					throw new ResourceException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25010,
							new Object[] { config.getCacheNameProxy().getAliasCacheName() }));
				}
			}

			config.getCacheSchemaConfigurator().registerSchema(config, this);

			
		} finally {
			Thread.currentThread().setContextClassLoader(lcl);
		}
		
	}
	
	private RemoteCacheManager getRemoteCacheFromJNDI(
			String jndiName) throws ResourceException {

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
			return (RemoteCacheManager) cache;
		}

		throw new ResourceException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25026, cache.getClass().getName()));

	}
	
	protected RemoteCacheManager createRemoteCacheFromProperties() throws ResourceException {
		File f = new File(config.getHotRodClientPropertiesFile());
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

			return createRemoteCache(props, config.getRemoteServerList());

		} catch (ResourceException re) {
			throw re;
		} catch (Exception err) {
			throw new ResourceException(err);
		}

	}
	
	protected  RemoteCacheManager createRemoteCacheFromServerList() throws ResourceException {

		LogManager.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

		return createRemoteCache(null, config.getRemoteServerList());
	}

	
	private RemoteCacheManager createRemoteCache(Properties props, String serverList) throws ResourceException {
		RemoteCacheManager remoteCacheManager;
		try {
			
			ProtoStreamMarshaller pm = (ProtoStreamMarshaller) ReflectionHelper.create("org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller", null, config.getRAClassLoader());
			ConfigurationBuilder cb = (ConfigurationBuilder) ReflectionHelper.create("org.infinispan.client.hotrod.configuration.ConfigurationBuilder", null, config.getRAClassLoader());
			cb.marshaller(pm);
			
			if (props != null) {
				cb.withProperties(props);
			}
			
			if (serverList != null) {
				cb.addServers(serverList);
			}
			
			if (config.getAuthSASLMechanism() != null) {
				
				if (this.adminUsage) {
					cb.security().authentication().enable()
					.serverName(config.getAuthServerName())
					.saslMechanism(config.getAuthSASLMechanism())
					.callbackHandler(new TeiidCallBackHandler(config.getAdminUserName(), config.getAuthApplicationRealm(), config.getAdminPassword().toCharArray()));

				} else if (config.getAuthUserName()!= null) {
					cb.security().authentication().enable()
						.serverName(config.getAuthServerName())
						.saslMechanism(config.getAuthSASLMechanism())
						.callbackHandler(new TeiidCallBackHandler(config.getAuthUserName(), config.getAuthApplicationRealm(), config.getAuthPassword().toCharArray()));
					
				} else {
					Subject subject = ConnectionContext.getSubject();

					if (subject != null) {

						cb.security().authentication().enable()
							.serverName(config.getAuthServerName())
							.saslMechanism(config.getAuthSASLMechanism())
							.clientSubject(subject)
							.callbackHandler(new TeiidCallBackHandler());

					}


				}
			}
			
			Collection<Object> ctors = new ArrayList<Object>(2);
			ctors.add(cb.build());
			ctors.add(true);

			remoteCacheManager = (RemoteCacheManager) ReflectionHelper.create("org.infinispan.client.hotrod.RemoteCacheManager", ctors, config.getRAClassLoader());

		} catch (Exception err) {
			throw new ResourceException(err);
		}

		return remoteCacheManager;

	}
	
	public SerializationContext getContext() {
		return context;
	}
	
}

class TeiidCallBackHandler implements CallbackHandler {
	   final private String username;
	   final private char[] password;
	   final private String realm;

	   public TeiidCallBackHandler (String username, String realm, char[] password) {
	      this.username = username;
	      this.password = password;
	      this.realm = realm;
	   }
	   public TeiidCallBackHandler () {	   
	      this.username = System.getProperty("sasl.username");
	      this.password = System.getProperty("sasl.password").toCharArray();
	      this.realm = System.getProperty("sasl.realm");
	   }

	   @Override
	   public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
	      for (Callback callback : callbacks) {
	         if (callback instanceof NameCallback) {
	            NameCallback nameCallback = (NameCallback) callback;
	            nameCallback.setName(username);
	         } else if (callback instanceof PasswordCallback) {
	            PasswordCallback passwordCallback = (PasswordCallback) callback;
	            passwordCallback.setPassword(password);
	         } else if (callback instanceof AuthorizeCallback) {
	            AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
	            authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(
	                  authorizeCallback.getAuthorizationID()));
	         } else if (callback instanceof RealmCallback) {
	            RealmCallback realmCallback = (RealmCallback) callback;
	            realmCallback.setText(realm);
	         } else {
	            throw new UnsupportedCallbackException(callback);
	         }
	      }
	   }
	}

