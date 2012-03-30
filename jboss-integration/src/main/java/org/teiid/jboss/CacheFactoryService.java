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
package org.teiid.jboss;

import org.infinispan.manager.CacheContainer;
import org.jboss.modules.Module;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.jboss.JBossCacheFactory;

class CacheFactoryService implements Service<CacheFactory> {
	protected InjectedValue<CacheContainer> cacheContainerInjector = new InjectedValue<CacheContainer>();
	private String cacheName;
	private CacheFactory cacheFactory;
	
	public CacheFactoryService(String cacheName){
		this.cacheName = cacheName;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		CacheContainer cc = cacheContainerInjector.getValue();
		if (cc != null) {
			this.cacheFactory = new JBossCacheFactory(this.cacheName, cc, Module.getCallerModule().getClassLoader());
		}
		else {
			this.cacheFactory = new DefaultCacheFactory();
		}
	}

	@Override
	public void stop(StopContext context) {
		this.cacheFactory.destroy();
		this.cacheFactory = null;
	}

	@Override
	public CacheFactory getValue() throws IllegalStateException, IllegalArgumentException {
		return this.cacheFactory;
	}
}
