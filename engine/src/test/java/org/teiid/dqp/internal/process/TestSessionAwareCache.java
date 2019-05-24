/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

        cache.clearForVDB("vdb-name", "1");

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
        assertEquals(Long.valueOf(1), cache.computeTtl(id, result, 1L));

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

    @Test public void testClear() {

        SessionAwareCache<Cachable> cache = new SessionAwareCache<Cachable>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);

        DQPWorkContext context = buildWorkContext();
        CacheID id = new CacheID(context, new ParseInfo(), "SELECT * FROM FOO");

        Cachable result = Mockito.mock(Cachable.class);
        Mockito.stub(result.prepare((BufferManager)anyObject())).toReturn(true);
        Mockito.stub(result.restore((BufferManager)anyObject())).toReturn(true);

        id = new CacheID(context, new ParseInfo(), "SELECT * FROM FOO");
        cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);

        id = new CacheID(context, new ParseInfo(), "SELECT * FROM FOO1");
        cache.put(id, Determinism.VDB_DETERMINISTIC, result, null);

        assertEquals(2, cache.getTotalCacheEntries());

        cache.clearForVDB(context.getVdbName(), context.getVdbVersion());

        assertEquals(0, cache.getTotalCacheEntries());
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
