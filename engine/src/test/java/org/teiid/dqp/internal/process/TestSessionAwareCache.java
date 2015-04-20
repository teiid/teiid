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

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.cache.Cachable;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.parser.ParseInfo;


@SuppressWarnings("nls")
public class TestSessionAwareCache {
	
	@Test
	public void testSessionSpecfic() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		cache.put(id, Determinism.SESSION_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(0)).prepare((BufferManager)anyObject());
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(0)).restore((BufferManager)anyObject());
		
		assertTrue(result==c);
	}
	
	@Test
	public void testUserSpecfic() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((BufferManager)anyObject())).toReturn(true);
				
		cache.put(id, Determinism.USER_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(1)).prepare((BufferManager)anyObject());
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(1)).restore((BufferManager)anyObject());		
		
		assertTrue(result==c);
	}
	
	@Test
	public void testNoScope() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((BufferManager)anyObject())).toReturn(true);		
		
		cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);
		
		// make sure that in the case of session specific; we do not call prepare
		// as session is local only call for distributed
		Mockito.verify(result, times(1)).prepare((BufferManager)anyObject());
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Object c = cache.get(id);
		
		Mockito.verify(result, times(1)).restore((BufferManager)anyObject());		
		
		assertTrue(result==c);
	}
	
	@Test
	public void testVDBRemoval() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((BufferManager)anyObject())).toReturn(true);		
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);
		
		Object c = cache.get(id);
		
		assertTrue(result==c);
		
		cache.clearForVDB("vdb-name", 1);
		
		assertNull(cache.get(id));
	}
	
	@Test public void testRemove() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		Mockito.stub(result.prepare((BufferManager)anyObject())).toReturn(true);
		Mockito.stub(result.restore((BufferManager)anyObject())).toReturn(true);		
		
		id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);
		
		Object c = cache.get(id);
		
		assertTrue(result==c);
		
		assertTrue(cache.remove(id, Determinism.VDB_DETERMINISTIC) != null);
		assertNull(cache.get(id));
		
		//session scope
		cache.put(id, Determinism.SESSION_DETERMINISTIC, result, null);
		assertTrue(cache.get(id) != null);
		assertTrue(cache.remove(id, Determinism.SESSION_DETERMINISTIC) != null);
		assertNull(cache.get(id));
	}
	
	@Test public void testTtl() {
		
		SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
		
		CacheID id = new CacheID(buildWorkContext(), new ParseInfo(), "SELECT * FROM FOO");
		
		Cachable result = Mockito.mock(Cachable.class);
		
		//make sure defaults are returned
		assertNull(cache.computeTtl(id, result, null));
		assertEquals(Long.valueOf(1), cache.computeTtl(id, result, 1l));
		
		AccessInfo ai = new AccessInfo();
		Mockito.stub(result.getAccessInfo()).toReturn(ai);
		
		Table t = new Table();
		t.setProperty(DataModifiable.DATA_TTL, "2");
		ai.addAccessedObject(t);
		
		assertEquals(Long.valueOf(2), cache.computeTtl(id, result, null));
		
		Table t1 = new Table();
		Schema s = new Schema();
		t1.setParent(s);
		s.setProperty(DataModifiable.DATA_TTL, "0");
		ai.addAccessedObject(t1);
		
		//ensure that the min and the parent are used
		assertEquals(Long.valueOf(0), cache.computeTtl(id, result, null));
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
