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
package org.teiid.dqp.internal.process;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.cache.Cachable;
import org.teiid.cache.Cache;
import org.teiid.common.buffer.BufferManager;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.parser.ParseInfo;


@SuppressWarnings("nls")
public class TestSessionAwareCache {
	
	@Test
	public void testSessionSpecfic() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>();
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		cache.put(id, Determinism.SESSION_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(0)).prepare((Cache)anyObject(), (BufferManager)anyObject());
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(0)).restore((Cache)anyObject(), (BufferManager)anyObject());
		
		assertTrue(result==c);
	}
	
	@Test
	public void testUserSpecfic() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>();
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);
				
		cache.put(id, Determinism.USER_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(1)).prepare((Cache)anyObject(), (BufferManager)anyObject());
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(1)).restore((Cache)anyObject(), (BufferManager)anyObject());		
		
		assertTrue(result==c);
	}
	
	@Test
	public void testNoScope() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>();
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);		
		
		cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(1)).prepare((Cache)anyObject(), (BufferManager)anyObject());
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(1)).restore((Cache)anyObject(), (BufferManager)anyObject());		
		
		assertTrue(result==c);
	}
	
	@Test
	public void testVDBRemoval() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>();
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((Cache)anyObject(), (BufferManager)anyObject())).toReturn(true);		
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);
		
		Object c = cache.get(id);
		
		assertTrue(result==c);
		
		cache.clearForVDB("vdb-name", 1);
		
		assertNull(cache.get(id));
	}

	
	public static DQPWorkContext buildWorkContext() {
		DQPWorkContext workContext = new DQPWorkContext();
		SessionMetadata session = new SessionMetadata();
		workContext.setSession(session);
		session.setVDBName("vdb-name"); //$NON-NLS-1$
		session.setVDBVersion(1); 
		session.setSessionId(String.valueOf(1));
		session.setUserName("foo"); //$NON-NLS-1$
		return workContext;
	}
}
