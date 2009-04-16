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

import java.util.Properties;

import org.jboss.cache.Cache;
import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.jboss.JBossCacheFactory;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.dqp.embedded.EmbeddedConfigSource;

public class EmbeddedGuiceModule extends AbstractModule {
	
	private Properties props;
	
	public EmbeddedGuiceModule(Properties props) {
		this.props = props;
	}
	
	@Override
	protected void configure() {
				
		bind(Cache.class).toProvider(CacheProvider.class).in(Scopes.SINGLETON);
		bind(CacheFactory.class).to(JBossCacheFactory.class).in(Scopes.SINGLETON);
		bindConstant().annotatedWith(Names.named("HostName")).to("embedded"); //$NON-NLS-1$ //$NON-NLS-2$
		bindConstant().annotatedWith(Names.named("ProcessName")).to("embedded"); //$NON-NLS-1$ //$NON-NLS-2$
		bind(DQPContextCache.class).in(Scopes.SINGLETON);
		bind(DQPConfigSource.class).toInstance(new EmbeddedConfigSource(this.props));
		
		bind(LogConfiguration.class).toProvider(LogConfigurationProvider.class).in(Scopes.SINGLETON);		
		bind(LogListener.class).toProvider(LogListernerProvider.class).in(Scopes.SINGLETON);  
		
		
		// this needs to be removed.
		binder().requestStaticInjection(LogManager.class);
	}
	
    
}

