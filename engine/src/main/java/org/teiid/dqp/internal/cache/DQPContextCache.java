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

import java.io.Serializable;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.common.log.LogManager;

public class DQPContextCache implements Serializable{
	private static final long serialVersionUID = 6958846566556640186L;

	private static enum Scope {REQUEST,SESSION,SERVICE,VDB,GLOBAL;}
	
	private Cache cache;
	private String processIdentifier;
	
	// called by mc
	public void setCacheFactory(CacheFactory cacheFactory) {
		this.cache = cacheFactory.get(Cache.Type.SCOPED_CACHE, new CacheConfiguration(Policy.LRU, 600, 10000));
	}
	//called by mc
	public void setProcessName(String name) {
		this.processIdentifier = name;
	}
	
	public Cache getGlobalScopedCache() {
		return this.cache.addChild(Scope.GLOBAL.name());
	}

	public void stop() {
		try {
			this.cache.removeChild(this.processIdentifier);
		} catch(IllegalStateException e) {
			LogManager.logWarning(com.metamatrix.common.util.LogConstants.CTX_DQP, e, e.getMessage());
		}
	}
	
	public Cache getRequestScopedCache(String request) {
		Cache processCache = this.cache.addChild(this.processIdentifier);
		Cache scopeNode = processCache.addChild(Scope.REQUEST.name());
		return scopeNode.addChild(request.toString());
	}

	public void removeRequestScopedCache(String request) {
		try {
			Cache processCache = this.cache.getChild(this.processIdentifier);
			if (processCache != null) {
				Cache scopeNode = processCache.getChild(Scope.REQUEST.name());
				if (scopeNode != null) {
					scopeNode.removeChild(request.toString());
				}
			}
		} catch(IllegalStateException e) {
			LogManager.logWarning(com.metamatrix.common.util.LogConstants.CTX_DQP, e, e.getMessage());
		}
	}
	
	public Cache getServiceScopedCache(String serviceId) {
		Cache processCache = this.cache.addChild(this.processIdentifier);
		Cache scopeNode = processCache.addChild(Scope.SERVICE.name());
		return scopeNode.addChild(serviceId);
	}
	
	public void removeServiceScopedCache(String serviceId) {
		try {
			Cache processCache = this.cache.getChild(this.processIdentifier);
			if (processCache != null) {
				Cache scopeNode = processCache.addChild(Scope.SERVICE.name());
				if (scopeNode != null) {
					scopeNode.removeChild(serviceId);
				}
			}
		} catch(IllegalStateException e) {
			LogManager.logWarning(com.metamatrix.common.util.LogConstants.CTX_DQP, e, e.getMessage());
		}
	}

	public Cache getSessionScopedCache(String session) {
		Cache scopeNode = this.cache.addChild(Scope.SESSION.name());
		return scopeNode.addChild(session);
	}

	public void removeSessionScopedCache(String session) {
		try {
			Cache scopeNode = this.cache.addChild(Scope.SESSION.name());
			if (scopeNode != null) {
				scopeNode.removeChild(session);
			}
		} catch(IllegalStateException e) {
			LogManager.logWarning(com.metamatrix.common.util.LogConstants.CTX_DQP, e, e.getMessage());
		}
	}

	public Cache getVDBScopedCache(String vdbName, int vdbVersion) {
		Cache scopeNode = this.cache.addChild(Scope.VDB.name());
		String id = vdbName+"-"+vdbVersion; //$NON-NLS-1$
		return scopeNode.addChild(id.toUpperCase());
	}

	public void removeVDBScopedCache(String vdbName, int vdbVersion) {
		try {
			Cache scopeNode = this.cache.addChild(Scope.VDB.name());
			if (scopeNode != null) {
				String id = vdbName+"-"+vdbVersion; //$NON-NLS-1$
				scopeNode.removeChild(id.toUpperCase());
			}
		} catch(IllegalStateException e) {
			LogManager.logWarning(com.metamatrix.common.util.LogConstants.CTX_DQP, e, e.getMessage());
		}
	}
}
