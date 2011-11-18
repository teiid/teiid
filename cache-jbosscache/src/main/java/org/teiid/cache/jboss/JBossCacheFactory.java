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

package org.teiid.cache.jboss;

import java.io.Serializable;

import org.infinispan.manager.CacheContainer;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.core.TeiidRuntimeException;


public class JBossCacheFactory implements CacheFactory, Serializable{
	private static final long serialVersionUID = -2767452034178675653L;
	private transient org.infinispan.Cache cacheStore;
	private volatile boolean destroyed = false;
	

	public JBossCacheFactory(String name, Object cm) throws Exception {
		CacheContainer cachemanager = (CacheContainer)cm;
		this.cacheStore = cachemanager.getCache(name);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Cache get(String location, CacheConfiguration config) {
		if (!destroyed) {
			return new JBossCache(this.cacheStore, config.getLocation());	
		}
		throw new TeiidRuntimeException("Cache system has been shutdown"); //$NON-NLS-1$
	}
	
	public void destroy() {
		this.destroyed = true;		
	}	
	
	public void stop() {
		destroy();
	}
	
	@Override
	public boolean isReplicated() {
		return true;
	}
}
