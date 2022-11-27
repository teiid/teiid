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

package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.WeakReference;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;

public class TestBufferFrontedFileStoreCache {

    private BufferFrontedFileStoreCache cache;

    private static class SimpleSerializer implements Serializer<Integer> {
        @Override
        public Integer deserialize(ObjectInput ois)
                throws IOException, ClassNotFoundException {
            Integer result = ois.readInt();
            for (int i = 0; i < result; i++) {
                assertEquals(i, ois.readInt());
            }
            return result;
        }

        @Override
        public Long getId() {
            return 1L;
        }

        @Override
        public void serialize(Integer obj, ObjectOutput oos)
                throws IOException {
            oos.writeInt(obj);
            for (int i = 0; i < obj; i++) {
                oos.writeInt(i);
            }
        }

        @Override
        public boolean useSoftCache() {
            return false;
        }

        @Override
        public String describe(Integer obj) {
            return null;
        }
    }

    @After public void teardown() {
        if (this.cache != null) {
            cache.shutdown();
        }
    }

    @Test public void testAddGetMultiBlock() throws Exception {
        cache = createLayeredCache(1 << 26, 1 << 26, true);

        CacheEntry ce = new CacheEntry(2L);
        Serializer<Integer> s = new SimpleSerializer();
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(2);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);
        ce = get(cache, 2L, s);
        assertEquals(cacheObject, ce.getObject());

        //test something that exceeds the direct inode data blocks
        ce = new CacheEntry(3L);
        cacheObject = Integer.valueOf(80000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        ce = get(cache, 3L, s);
        assertEquals(cacheObject, ce.getObject());

        //repeat the test to ensure proper cleanup
        ce = new CacheEntry(4L);
        cacheObject = Integer.valueOf(60000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        ce = get(cache, 4L, s);
        assertEquals(cacheObject, ce.getObject());

        cache.removeCacheGroup(1L);

        assertEquals(0, cache.getDataBlocksInUse());
        assertEquals(0, cache.getInodesInUse());

        //test something that exceeds the indirect data blocks
        ce = new CacheEntry(3L);
        cache.createCacheGroup(s.getId());
        cacheObject = Integer.valueOf(5000000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        ce = get(cache, 3L, s);
        assertEquals(cacheObject, ce.getObject());

        cache.removeCacheGroup(1L);

        assertEquals(0, cache.getDataBlocksInUse());
        assertEquals(0, cache.getInodesInUse());

        //test something that exceeds the allowable object size
        ce = new CacheEntry(3L);
        cache.createCacheGroup(s.getId());
        cacheObject = Integer.valueOf(500000000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        ce = get(cache, 3L, s);
        assertNull(ce);

        cache.removeCacheGroup(1L);

        assertEquals(0, cache.getDataBlocksInUse());
        assertEquals(0, cache.getInodesInUse());
    }

    @Test public void testMultipleAdds() throws Exception {
        cache = createLayeredCache(1 << 18, 1 << 18, true);

        Serializer<Integer> s = new SimpleSerializer() {
            @Override
            public void serialize(Integer obj, ObjectOutput oos)
                    throws IOException {
                throw new IOException();
            }
        };
        CacheEntry ce = new CacheEntry(new CacheKey(31L, 0, 0), 1000000, null, null, false);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(50000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        assertTrue(cache.add(ce, s));

        s = new SimpleSerializer();
        assertTrue(cache.add(ce, s));

        assertNotNull(get(cache, ce.getId(), s));
    }

    private static CacheEntry get(BufferFrontedFileStoreCache cache, Long oid,
            Serializer<Integer> s) throws TeiidComponentException {
        PhysicalInfo o = cache.lockForLoad(oid, s);
        CacheEntry ce = cache.get(o, oid, new WeakReference<Serializer<?>>(s));
        cache.unlockForLoad(o);
        return ce;
    }

    @Test public void testEviction() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, true);
        assertEquals(3, cache.getMaxMemoryBlocks());

        CacheEntry ce = new CacheEntry(2L);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        ce.setSerializer(ref);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(5000);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        ce = new CacheEntry(3L);
        ce.setSerializer(ref);
        cacheObject = Integer.valueOf(5001);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);

        assertTrue(cache.getDataBlocksInUse() < 4);
        assertTrue(cache.getInodesInUse() < 2);

        ce = get(cache, 2L, s);
        assertEquals(Integer.valueOf(5000), ce.getObject());

        ce = get(cache, 3L, s);
        assertEquals(Integer.valueOf(5001), ce.getObject());
    }

    @Test public void testEvictionFails() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, false);
        BufferManagerImpl bmi = Mockito.mock(BufferManagerImpl.class);
        cache.setBufferManager(bmi);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        cache.createCacheGroup(s.getId());

        for (int i = 0; i < 3; i++) {
            add(cache, s, ref, i);
        }
        Mockito.verify(bmi, Mockito.atLeastOnce()).invalidCacheGroup(Long.valueOf(1));
    }

    private void add(BufferFrontedFileStoreCache cache, Serializer<Integer> s,
            WeakReference<? extends Serializer<?>> ref, int i) {
        CacheEntry ce = new CacheEntry(Long.valueOf(i));
        ce.setSerializer(ref);
        Integer cacheObject = Integer.valueOf(5000 + i);
        ce.setObject(cacheObject);
        cache.addToCacheGroup(s.getId(), ce.getId());
        cache.add(ce, s);
    }

    private static BufferFrontedFileStoreCache createLayeredCache(int bufferSpace, int objectSize, boolean memStorage) throws TeiidComponentException {
        BufferFrontedFileStoreCache fsc = new BufferFrontedFileStoreCache();
        fsc.cleanerRunning.set(true); //prevent async affects
        fsc.setMemoryBufferSpace(bufferSpace);
        fsc.setMaxStorageObjectSize(objectSize);
        fsc.setDirect(false);
        if (memStorage) {
            SplittableStorageManager ssm = new SplittableStorageManager(new MemoryStorageManager());
            ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
            fsc.setStorageManager(ssm);
        } else {
            StorageManager sm = new StorageManager() {

                @Override
                public void initialize() throws TeiidComponentException {

                }

                @Override
                public FileStore createFileStore(String name) {
                    return new FileStore() {

                        @Override
                        public void setLength(long length) throws IOException {
                            throw new OutOfDiskException(null);
                        }

                        @Override
                        protected void removeDirect() {

                        }

                        @Override
                        protected int readWrite(long fileOffset, byte[] b, int offSet, int length,
                                boolean write) throws IOException {
                            return 0;
                        }

                        @Override
                        public long getLength() {
                            return 0;
                        }
                    };
                }

                @Override
                public long getMaxStorageSpace() {
                    return -1;
                }
            };
            fsc.setStorageManager(sm);
        }
        fsc.initialize();
        return fsc;
    }

    @Test public void testSizeIndex() throws Exception {
        PhysicalInfo info = new PhysicalInfo(1L, 1L, -1, 0, 0);
        info.setSize(1<<13);
        assertEquals(0, info.sizeIndex);

        info = new PhysicalInfo(1L, 1L, -1, 0, 0);
        info.setSize(1 + (1<<13));
        assertEquals(1, info.sizeIndex);

        info = new PhysicalInfo(1L, 1L, -1, 0, 0);
        info.setSize(2 + (1<<15));
        assertEquals(3, info.sizeIndex);
    }

    @Test(expected=Exception.class) public void testSizeChanged() throws Exception {
        PhysicalInfo info = new PhysicalInfo(1L, 1L, -1, 0, 0);
        info.setSize(1<<13);
        assertEquals(0, info.sizeIndex);

        info.setSize(1 + (1<<13));
    }

    @Test public void testDefragTruncateEmpty() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, true);
        cache.setMinDefrag(10000000);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(5000);

        for (int i = 0; i < 4; i++) {
            CacheEntry ce = new CacheEntry((long)i);
            ce.setSerializer(ref);
            ce.setObject(cacheObject);

            cache.addToCacheGroup(s.getId(), ce.getId());
            cache.add(ce, s);
        }
        assertEquals(98304, cache.getDiskUsage());
        for (int i = 0; i < 4; i++) {
            cache.remove(1L, (long)i);
        }
        assertEquals(98304, cache.getDiskUsage());
        cache.setMinDefrag(0);
        cache.defragTask.run();
        assertEquals(98304, cache.getDiskUsage());
        cache.setTruncateInterval(1);
        cache.defragTask.run();
        assertEquals(0, cache.getDiskUsage());
    }

    @Test public void testDefragTruncate() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, true);
        cache.setMinDefrag(10000000);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(5000);

        for (int i = 0; i < 30; i++) {
            CacheEntry ce = new CacheEntry((long)i);
            ce.setSerializer(ref);
            ce.setObject(cacheObject);

            cache.addToCacheGroup(s.getId(), ce.getId());
            cache.add(ce, s);
        }
        assertEquals(950272, cache.getDiskUsage());
        for (int i = 0; i < 25; i++) {
            cache.remove(1L, (long)i);
        }
        assertEquals(950272, cache.getDiskUsage());
        cache.setMinDefrag(0);
        cache.setTruncateInterval(1);
        cache.defragTask.run();
        assertEquals(622592, cache.getDiskUsage());
        cache.defragTask.run();
        assertEquals(262144, cache.getDiskUsage());
        cache.defragTask.run();
        assertEquals(131072, cache.getDiskUsage());
        cache.defragTask.run();
        //we've reached a stable size
        assertEquals(131072, cache.getDiskUsage());
    }

    @Test public void testDefragTruncateCompact() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, true);
        cache.setCompactBufferFiles(true);
        cache.setTruncateInterval(1);
        cache.setMinDefrag(10000000);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(5000);

        for (int i = 0; i < 30; i++) {
            CacheEntry ce = new CacheEntry((long)i);
            ce.setSerializer(ref);
            ce.setObject(cacheObject);

            cache.addToCacheGroup(s.getId(), ce.getId());
            cache.add(ce, s);
        }
        assertEquals(950272, cache.getDiskUsage());
        for (int i = 0; i < 25; i++) {
            cache.remove(1L, (long)i);
        }
        assertEquals(950272, cache.getDiskUsage());
        cache.setMinDefrag(0);
        cache.setTruncateInterval(1);
        cache.defragTask.run();
        assertEquals(131072, cache.getDiskUsage());
        cache.defragTask.run();
        //we've reached a stable size
        assertEquals(131072, cache.getDiskUsage());
    }

    @Test public void testDefragMin() throws Exception {
        cache = createLayeredCache(1<<15, 1<<15, true);
        cache.setMinDefrag(10000000);
        Serializer<Integer> s = new SimpleSerializer();
        WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
        cache.createCacheGroup(s.getId());
        Integer cacheObject = Integer.valueOf(5000);

        for (int i = 0; i < 100; i++) {
            CacheEntry ce = new CacheEntry((long)i);
            ce.setSerializer(ref);
            ce.setObject(cacheObject);

            cache.addToCacheGroup(s.getId(), ce.getId());
            cache.add(ce, s);
        }
        assertEquals(3244032, cache.getDiskUsage());
        for (int i = 0; i < 90; i++) {
            cache.remove(1L, (long)i);
        }
        assertEquals(3244032, cache.getDiskUsage());
        cache.setMinDefrag(5000);
        cache.setTruncateInterval(1);
        cache.defragTask.run();
        assertEquals(1802240, cache.getDiskUsage());
        cache.defragTask.run();
        assertEquals(1114112, cache.getDiskUsage());
        cache.defragTask.run();
        assertEquals(655360, cache.getDiskUsage());
        cache.defragTask.run();
        //we've reached a stable size
        assertEquals(655360, cache.getDiskUsage());
    }

    @Test public void testLargeMax() throws TeiidComponentException {
        createLayeredCache(1 << 20, 1 << 30, false);
    }

    @Test public void testNonAlignedMaxBlocks() throws TeiidComponentException {
        BufferFrontedFileStoreCache bf = createLayeredCache(1 << 20, 8000000, false);
        assertEquals(974, bf.getMaxMemoryBlocks());
    }

}
