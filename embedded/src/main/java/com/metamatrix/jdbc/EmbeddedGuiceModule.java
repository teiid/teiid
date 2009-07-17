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

package com.metamatrix.jdbc;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jboss.cache.Cache;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.process.DQPCore;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.jboss.JBossCacheFactory;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.JMXUtil;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.embedded.services.EmbeddedDataService;
import com.metamatrix.dqp.embedded.services.EmbeddedMetadataService;
import com.metamatrix.dqp.embedded.services.EmbeddedTransactionService;
import com.metamatrix.dqp.embedded.services.EmbeddedVDBService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.authorization.service.AdminAuthorizationPolicyProvider;
import com.metamatrix.platform.security.authorization.service.AuthorizationServiceImpl;
import com.metamatrix.platform.security.membership.service.MembershipServiceImpl;
import com.metamatrix.platform.security.session.service.SessionServiceImpl;

public class EmbeddedGuiceModule extends AbstractModule implements DQPConfigSource{
	
	private Properties props;
	private URL bootstrapURL;
	private JMXUtil jmx;
	Injector injector;
	
	public EmbeddedGuiceModule(URL bootstrapURL, Properties props, JMXUtil jmxUtil) {
		this.bootstrapURL = bootstrapURL;
		this.props = props;
		this.jmx = jmxUtil;
	}
	
	@Override
	protected void configure() {
		bind(LogConfiguration.class).toProvider(LogConfigurationProvider.class).in(Scopes.SINGLETON);		
		bind(LogListener.class).toProvider(LogListernerProvider.class).in(Scopes.SINGLETON);  
		
		bind(URL.class).annotatedWith(Names.named("BootstrapURL")).toInstance(bootstrapURL); //$NON-NLS-1$
		bind(Properties.class).annotatedWith(Names.named("DQPProperties")).toInstance(this.props); //$NON-NLS-1$
		bind(JMXUtil.class).annotatedWith(Names.named("jmx")).toInstance(this.jmx); //$NON-NLS-1$

		InetAddress address = resolveHostAddress(props.getProperty(DQPEmbeddedProperties.BIND_ADDRESS));
		bind(InetAddress.class).annotatedWith(Names.named(DQPEmbeddedProperties.HOST_ADDRESS)).toInstance(address);
		this.props.put(DQPEmbeddedProperties.HOST_ADDRESS, address);

		bind(Cache.class).toProvider(CacheProvider.class).in(Scopes.SINGLETON);
		bind(CacheFactory.class).to(JBossCacheFactory.class).in(Scopes.SINGLETON);
		
		bind(DQPContextCache.class).in(Scopes.SINGLETON);
		bind(DQPCore.class).in(Scopes.SINGLETON);
		bind(new TypeLiteral<Collection<AuthorizationPolicy>>(){}).annotatedWith(Names.named("AdminRoles")).toProvider(AdminAuthorizationPolicyProvider.class).in(Scopes.SINGLETON); //$NON-NLS-1$
		
		configureServices();
		
		// this needs to be removed.
		binder().requestStaticInjection(LogManager.class);
	}
	
	private InetAddress resolveHostAddress(String bindAddress) {
		try {
			if (bindAddress == null) {
				return NetUtils.getInstance().getInetAddress();
			}
			return NetUtils.resolveHostByName(bindAddress);	
		} catch (UnknownHostException e) {
			throw new MetaMatrixRuntimeException("Failed to resolve the bind address"); //$NON-NLS-1$
		}
	}
	
	private void configureServices() {
		Map<String, Class<? extends ApplicationService>> defaults = getDefaultServiceClasses();
		for(int i=0; i<DQPServiceNames.ALL_SERVICES.length; i++) {
			final String serviceName = DQPServiceNames.ALL_SERVICES[i];
			String className = this.props.getProperty("service."+serviceName+".classname"); //$NON-NLS-1$ //$NON-NLS-2$
			Class clazz = defaults.get(serviceName);
			if (clazz != null && className != null) {
				try {
					clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
				} catch (ClassNotFoundException e) {
					throw new MetaMatrixRuntimeException(e);
				}
			}
			if (clazz != null) {
				bind(DQPServiceNames.ALL_SERVICE_CLASSES[i]).to(clazz).in(Scopes.SINGLETON);
			}
		}		
	}
	
	private Map<String, Class<? extends ApplicationService>> getDefaultServiceClasses() {
		boolean useTxn = PropertiesUtils.getBooleanProperty(props, TransactionService.TRANSACTIONS_ENABLED, true);
		
		Map<String, Class<? extends ApplicationService>> result = new HashMap<String, Class<? extends ApplicationService>>();
		result.put(DQPServiceNames.CONFIGURATION_SERVICE, EmbeddedConfigurationService.class);
		result.put(DQPServiceNames.BUFFER_SERVICE, EmbeddedBufferService.class);
		result.put(DQPServiceNames.VDB_SERVICE, EmbeddedVDBService.class);
		result.put(DQPServiceNames.METADATA_SERVICE, EmbeddedMetadataService.class);
		result.put(DQPServiceNames.DATA_SERVICE, EmbeddedDataService.class);
		if (useTxn) {
			result.put(DQPServiceNames.TRANSACTION_SERVICE, EmbeddedTransactionService.class);
		}
		result.put(DQPServiceNames.SESSION_SERVICE, SessionServiceImpl.class);
		result.put(DQPServiceNames.MEMBERSHIP_SERVICE, MembershipServiceImpl.class);
		result.put(DQPServiceNames.AUTHORIZATION_SERVICE, AuthorizationServiceImpl.class);
		return result;
	}

	public void setInjector(Injector injector) {
		this.injector = injector;
	}
	
	@Override
	public Properties getProperties() {
		return this.props;
	}

	@Override
	public ApplicationService getServiceInstance(Class<? extends ApplicationService> type) {
		return this.injector.getInstance(type);
	}	
}

