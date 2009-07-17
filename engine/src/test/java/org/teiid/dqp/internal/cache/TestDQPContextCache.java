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

import junit.framework.TestCase;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.FakeCache.FakeCacheFactory;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;


public class TestDQPContextCache extends TestCase {

	DQPContextCache cacheContext = null;
	
	@Override
	protected void setUp() throws Exception {
		Properties p = new Properties();
		p.setProperty(DQPEmbeddedProperties.PROCESSNAME, "host-process"); //$NON-NLS-1$
		cacheContext = new DQPContextCache(p, new FakeCacheFactory());
			
	}
	
	private DQPWorkContext getContext() {
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("MyVDB"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(1), "foo")); //$NON-NLS-1$
        return workContext;
	}

	// killing a request scope does not kill the session scope
	public void testRequestScope() {
		DQPWorkContext context = getContext();
		Cache cache = this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString());
		cache.put("key", "request-value"); //$NON-NLS-1$ //$NON-NLS-2$

		cache = this.cacheContext.getSessionScopedCache(context.getSessionToken().getSessionIDValue());		
		cache.put("key", "session-value"); //$NON-NLS-1$ //$NON-NLS-2$
	
		assertEquals("request-value", this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString()).get("key")); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals("session-value", this.cacheContext.getSessionScopedCache(context.getSessionToken().getSessionIDValue()).get("key")); //$NON-NLS-1$ //$NON-NLS-2$
	
		// close the request
		this.cacheContext.removeRequestScopedCache(context.getRequestID(12L).toString());
		
		assertNull(this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString()).get("key")); //$NON-NLS-1$ 
		assertEquals("session-value", this.cacheContext.getSessionScopedCache(context.getSessionToken().getSessionIDValue()).get("key")); //$NON-NLS-1$ //$NON-NLS-2$
	}
		
	
	public void testServiceScope() {
		DQPWorkContext context = getContext();
		Cache cache = this.cacheContext.getServiceScopedCache("my-connector"); //$NON-NLS-1$
		cache.put("key", "service-value"); //$NON-NLS-1$ //$NON-NLS-2$
	
		assertEquals("service-value", this.cacheContext.getServiceScopedCache("my-connector").get("key")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		this.cacheContext.removeServiceScopedCache("my-Connector"); //$NON-NLS-1$
		
		assertNull(this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString()).get("key")); //$NON-NLS-1$
	}	
	
	public void testGlobalScope() {
		DQPWorkContext context = getContext();
		Cache cache = this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString());
		cache.put("key", "request-value"); //$NON-NLS-1$ //$NON-NLS-2$

		cache = this.cacheContext.getGlobalScopedCache();		
		cache.put("key", "global-value"); //$NON-NLS-1$ //$NON-NLS-2$

		assertEquals("request-value", this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString()).get("key")); //$NON-NLS-1$ //$NON-NLS-2$
		assertEquals("global-value", this.cacheContext.getGlobalScopedCache().get("key")); //$NON-NLS-1$ //$NON-NLS-2$
		
		this.cacheContext.shutdown();
		
		assertNull(this.cacheContext.getRequestScopedCache(context.getRequestID(12L).toString()).get("key")); //$NON-NLS-1$
		// global only dies when the engine is shutdown
		assertEquals("global-value", this.cacheContext.getGlobalScopedCache().get("key")); //$NON-NLS-1$ //$NON-NLS-2$
	}	
}
