package com.metamatrix.jdbc;

import org.jboss.cache.Cache;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.jboss.JBossCacheFactory;

public class EmbeddedGuiceModule extends AbstractModule {

	@Override
	protected void configure() {
		
		bind(Cache.class).toProvider(CacheProvider.class).in(Scopes.SINGLETON);
		bind(CacheFactory.class).to(JBossCacheFactory.class).in(Scopes.SINGLETON);
		
		// currently this is setup in embedded buffer service - needs to move in here.
		//bind(BufferManager.class).toProvider(BufferManagerProvider.class).in(Scopes.SINGLETON);		
	}
	
}

