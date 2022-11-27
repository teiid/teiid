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

package org.teiid.common.buffer;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.sql.symbol.ElementSymbol;

@SuppressWarnings({"nls", "unchecked"})
public class TestSTree {

    @Test public void testRemoveAll() throws TeiidComponentException {
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        ElementSymbol e2 = new ElementSymbol("y");
        e2.setType(String.class);
        List<ElementSymbol> elements = Arrays.asList(e1, e2);
        STree map = bm.createSTree(elements, "1", 1);

        for (int i = 20000; i > 0; i--) {
            assertNull(map.insert(Arrays.asList(i, String.valueOf(i)), InsertMode.NEW, -1));
            assertEquals(20000 - i + 1, map.getRowCount());
        }

        for (int i = 20000; i > 0; i--) {
            assertNotNull(String.valueOf(i), map.remove(Arrays.asList(i)));
        }

        assertEquals(0, map.getRowCount());
        assertNull(map.insert(Arrays.asList(1, String.valueOf(1)), InsertMode.NEW, -1));
    }

    @Test public void testUnOrderedInsert() throws TeiidComponentException {
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setProcessorBatchSize(16);

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        List elements = Arrays.asList(e1);
        STree map = bm.createSTree(elements, "1", 1);

        int size = (1<<16)+(1<<4)+1;
        int logSize = map.getExpectedHeight(size);

        for (int i = 0; i < size; i++) {
            assertNull(map.insert(Arrays.asList(i), InsertMode.NEW, logSize));
            assertEquals(i + 1, map.getRowCount());
        }
        assertTrue(5 >= map.getHeight());
    }

    @Test public void testOrderedInsert() throws TeiidComponentException {
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setProcessorBatchSize(4);

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        List<ElementSymbol> elements = Arrays.asList(e1);
        STree map = bm.createSTree(elements, "1", 1);

        int size = (1<<16)+(1<<4)+1;

        for (int i = 0; i < size; i++) {
            assertNull(map.insert(Arrays.asList(i), InsertMode.ORDERED, size));
            assertEquals(i + 1, map.getRowCount());
        }

        assertEquals(4, map.getHeight());

        for (int i = 0; i < size; i++) {
            assertNotNull(map.remove(Arrays.asList(i)));
        }

    }

    @Test public void testStorageWrites() throws TeiidComponentException {
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setProcessorBatchSize(32);
        bm.setMaxReserveKB(0);//force all to disk
        BufferFrontedFileStoreCache fsc =(BufferFrontedFileStoreCache)bm.getCache();
        fsc.setMaxStorageObjectSize(1 << 19);
        fsc.setMemoryBufferSpace(1 << 19);
        fsc.initialize();
        bm.initialize();

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(String.class);
        List<ElementSymbol> elements = Arrays.asList(e1);
        STree map = bm.createSTree(elements, "1", 1);

        int size = 1000;

        for (int i = 0; i < size; i++) {
            assertNull(map.insert(Arrays.asList(new String(new byte[1000])), InsertMode.ORDERED, size));
            assertEquals(i + 1, map.getRowCount());
        }

        for (int i = 0; i < size; i++) {
            assertNotNull(map.remove(Arrays.asList(new String(new byte[1000]))));
        }

        assertEquals(0, map.getRowCount());
        assertEquals(0, bm.getActiveBatchBytes());

        map.remove();

        assertEquals(0, bm.getActiveBatchBytes());
    }

    @Test public void testSearch() throws TeiidComponentException, TeiidProcessingException {
        //due to buffering changes we need to hold this in memory directly rather than serialize it out as that will lead to GC overhead errors
        BufferManagerImpl bm = BufferManagerFactory.getTestBufferManager(Integer.MAX_VALUE, 1);

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        ElementSymbol e2 = new ElementSymbol("x");
        e2.setType(Integer.class);
        List elements = Arrays.asList(e1, e2);
        STree map = bm.createSTree(elements, "1", 2);

        int size = 1<<16;
        for (int i = 0; i < size; i++) {
            assertNull(map.insert(Arrays.asList(i, i), InsertMode.NEW, -1));
            assertEquals(i + 1, map.getRowCount());
        }
        map.compact();
        for (int i = 0; i < size; i++) {
            TupleBrowser tb = new TupleBrowser(map, new CollectionTupleSource(Collections.singletonList(Arrays.asList(i)).iterator()), true);
            assertNotNull(tb.nextTuple());
            assertNull(tb.nextTuple());
        }
    }

    @Test public void testSearchWithRepeated() throws TeiidComponentException, TeiidProcessingException {
        //due to buffering changes we need to hold this in memory directly rather than serialize it out as that will lead to GC overhead errors
        BufferManagerImpl bm = BufferManagerFactory.getTestBufferManager(Integer.MAX_VALUE, 1);

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        ElementSymbol e2 = new ElementSymbol("x");
        e2.setType(Integer.class);
        List<ElementSymbol> elements = Arrays.asList(e1, e2);
        STree map = bm.createSTree(elements, "1", 2);

        int size = 1<<16;
        for (int i = 0; i < size; i++) {
            assertNull(map.insert(Arrays.asList(i, i*2), InsertMode.NEW, -1));
            assertNull(map.insert(Arrays.asList(i, i*2+1), InsertMode.NEW, -1));
            assertEquals((i + 1) * 2, map.getRowCount());
        }
        map.compact();
        for (int i = 0; i < size; i++) {
            TupleBrowser tb = new TupleBrowser(map, new CollectionTupleSource(Collections.singletonList(Arrays.asList(i)).iterator()), true);
            for (int j = 0; j < 2; j++) {
                assertNotNull(tb.nextTuple());
            }
            assertNull(tb.nextTuple());
        }
    }

    @Test public void testTupleBrowserRemove() throws Exception {
        BufferManagerImpl bm = BufferManagerFactory.getTestBufferManager(1, 1);

        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Integer.class);
        ElementSymbol e2 = new ElementSymbol("x");
        e2.setType(Integer.class);
        List<ElementSymbol> elements = Arrays.asList(e1, e2);
        STree map = bm.createSTree(elements, "1", 2);
        map.insert(Arrays.asList(1, 1), InsertMode.NEW, -1);
        TupleBrowser tb = new TupleBrowser(map, new CollectionTupleSource(Collections.singletonList(Arrays.asList(1)).iterator()), true, false);
        assertNotNull(tb.nextTuple());
        tb.removed();
        assertEquals(Integer.valueOf(0), tb.getValueCount());
    }

}
