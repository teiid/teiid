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
package org.teiid.dqp.internal.cache;

import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

@Singleton
public class DQPContextCache {
	private static enum Scope {REQUEST,SESSION,SERVICE,VDB,GLOBAL;}
	
	private Cache cache;
	private String processIdentifier;
	
	@Inject
	public DQPContextCache(@Named("DQPProperties") Properties props, CacheFactory cacheFactory) {
		this.cache = cacheFactory.get(Cache.Type.SCOPED_CACHE, new CacheConfiguration(Policy.LRU, 600, 10000));
		this.processIdentifier = props.getProperty(DQPEmbeddedProperties.PROCESSNAME);
	}
	
	public Cache getGlobalScopedCache() {
		return this.cache.addChild(Scope.GLOBAL.name());
	}

	public void shutdown() {
		this.cache.removeChild(this.processIdentifier);
	}
	
	public Cache getRequestScopedCache(String request) {
		Cache processCache = this.cache.addChild(this.processIdentifier);
		Cache scopeNode = processCache.addChild(Scope.REQUEST.name());
		return scopeNode.addChild(request.toString());
	}

	public void removeRequestScopedCache(String request) {
		Cache processCache = this.cache.getChild(this.processIdentifier);
		if (processCache != null) {
			Cache scopeNode = processCache.getChild(Scope.REQUEST.name());
			if (scopeNode != null) {
				scopeNode.removeChild(request.toString());
			}
		}
	}
	
	public Cache getServiceScopedCache(String serviceId) {
		Cache processCache = this.cache.addChild(this.processIdentifier);
		Cache scopeNode = processCache.addChild(Scope.SERVICE.name());
		return scopeNode.addChild(serviceId);
	}
	
	public void removeServiceScopedCache(String serviceId) {
		Cache processCache = this.cache.getChild(this.processIdentifier);
		if (processCache != null) {
			Cache scopeNode = processCache.addChild(Scope.SERVICE.name());
			if (scopeNode != null) {
				scopeNode.removeChild(serviceId);
			}
		}
	}

	public Cache getSessionScopedCache(String session) {
		Cache scopeNode = this.cache.addChild(Scope.SESSION.name());
		return scopeNode.addChild(session);
	}

	public void removeSessionScopedCache(String session) {
		Cache scopeNode = this.cache.addChild(Scope.SESSION.name());
		if (scopeNode != null) {
			scopeNode.removeChild(session);
		}
	}

	public Cache getVDBScopedCache(String vdbName, String vdbVersion) {
		Cache scopeNode = this.cache.addChild(Scope.VDB.name());
		String id = vdbName+"-"+vdbVersion; //$NON-NLS-1$
		return scopeNode.addChild(id.toUpperCase());
	}

	public void removeVDBScopedCache(String vdbName, String vdbVersion) {
		Cache scopeNode = this.cache.addChild(Scope.VDB.name());
		if (scopeNode != null) {
			String id = vdbName+"-"+vdbVersion; //$NON-NLS-1$
			scopeNode.removeChild(id.toUpperCase());
		}
	}
}
