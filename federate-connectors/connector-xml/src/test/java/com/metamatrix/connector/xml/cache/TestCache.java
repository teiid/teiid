/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.xml.cache;


import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.exception.ConnectorException;

/**
 * 
 * Tests the document cache with performance caching off.  Documents are not maintained
 * in the cache past when the query that originated them needs them in order to satisfy
 * joins in the query.
 *
 */
public class TestCache extends TestCase {

    private static final String CACHE_LOC = UnitTestUtil.getTestScratchPath() + File.separator + "cache"; //$NON-NLS-1$ //$NON-NLS-2$
    /**
     * 
     */
    
    private final int CACHE_SIZE = 512;
    private final int CACHE_TIMEOUT = 0;
    private final int LOGGING_LEVEL = 1;
    
    public TestCache(String arg0) {
        super(arg0);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestCache("testCacheSetup"));
        suite.addTest(new TestCache("testAddToCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testRemoveFromCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testOverloadMemoryCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testOverloadFileCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testFetchFromMemoryCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testFetchFromFileCache")); //$NON-NLS-1$
        suite.addTest(new TestCache("testCacheUncacheable")); //$NON-NLS-1$
        suite.addTest(new TestCache("testFetchOlderObject")); //$NON-NLS-1$
        suite.addTest(new TestCache("testCacheStraightToFile")); //$NON-NLS-1$
        suite.addTest(new TestCache("testCacheToFileTooBig")); //$NON-NLS-1$
        suite.addTest(new TestCache("testCleanerShutdown")); //$NON-NLS-1$
        //suite.addTest(new CacheTest("testMultithreaded")); //$NON-NLS-1$
        return suite;        
    }
    
    public void setUp() {
        File cacheFile = new File(CACHE_LOC);
        if(!cacheFile.exists()) {
            cacheFile.mkdirs();
        }
    }
    
    
    public void testCacheSetup() {
        try{
        IDocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestCacheSetup"); //$NON-NLS-1$
        assertNotNull(cache);
        }catch(Throwable t){
            fail(t.getMessage());
        }
    }
    
    public void testAddToCache() throws ConnectorException {
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestAddToCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache("foo1", newString, newString.length(), "foo"); //$NON-NLS-1$
        
        assertEquals("Memory cache size is wrong", cache.getCurrentMemoryCacheSize(), newString.length());
        assertEquals("File cache size is wrong", cache.getCurrentFileCacheSize(), 0);
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    
    public void testOverloadMemoryCache() throws Exception {
        final int maxSize = 18;
        final String id1 = "foo1"; //$NON-NLS-1$
        final String id2 = "foo2"; //$NON-NLS-1$
        final String dir = CACHE_LOC;
        

        DocumentCache cache = new DocumentCache(maxSize, maxSize, dir, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestOverloadMemoryCache"); //$NON-NLS-1$
        String string1 = new String("blah, blah, blah"); //$NON-NLS-1$
        String string2 = new String("blab, blab, blab"); //$NON-NLS-1$
        cache.addToCache(id1, string1, string1.length(), "foo1");
        cache.addToCache(id2, string2, string2.length(), "foo2");
        
        assertEquals("Memory cache size is wrong", string2.length(), cache.getCurrentMemoryCacheSize());
        assertEquals("File cache size is wrong", string1.length(), cache.getCurrentFileCacheSize());
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    public void testOverloadFileCache() throws Exception {
        
    	final int maxSize = 18;
        final String id1 = "foo1"; //$NON-NLS-1$
        final String id2 = "foo2"; //$NON-NLS-1$
        final String id3 = "foo3"; //$NON-NLS-1$
        final String dir = CACHE_LOC;

        DocumentCache cache = new DocumentCache(maxSize, maxSize, dir, 20000, EnvironmentUtility.createStdoutLogger(6), "TestOverloadFileCache"); //$NON-NLS-1$
        String string1 = new String("blah, blah, blah"); //$NON-NLS-1$
        String string2 = new String("blab, blab, blab"); //$NON-NLS-1$
        String string3 = new String("slab, slab, slab"); //$NON-NLS-1$
        try{
        cache.addToCache(id1, string1, string1.length(), "foo1");
        cache.addToCache(id2, string2, string2.length(), "foo2");
        }catch(Throwable t){
        	fail(t.getMessage());
        }
        try{
        	cache.addToCache(id3, string3, string3.length(), "foo3");
        }catch(ConnectorException e){
        	assertEquals("Failed to cache to file.  Increase the file cache size", e.getMessage());
        }
        assertEquals("Memory cache size is wrong",  string2.length(), cache.getCurrentMemoryCacheSize());
        assertEquals("File cache size is wrong", string1.length(), cache.getCurrentFileCacheSize());        
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id3, null));   
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    public void tearDown()  {
        try {
            File cacheDir = new File(CACHE_LOC);
            File[] files = cacheDir.listFiles();
            for(int i = 0; i < files.length; i++) {
                files[i].delete();
            }
        } catch (NullPointerException npe) {
            //ignore
        }
    }
    
    
    public void testFetchFromMemoryCache() throws Exception {
        final int timeout = 1000;
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestFetchFromCache"); //$NON-NLS-1$
        final String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        final String cacheKey = "foo1";
        cache.addToCache(cacheKey, newString, newString.length(), "foo"); //$NON-NLS-1$
        cache.dumpCache();
        Object newObject = cache.fetchObject(cacheKey, "foo");
        assertEquals("Memory cache size is wrong", cache.getCurrentMemoryCacheSize(), newString.length());
        assertEquals("File cache size is wrong", cache.getCurrentFileCacheSize(), 0);
        assertEquals("Got the wrong value out of the cache", ((String)newObject), newString);
        cache.dumpCache();
        Thread.sleep(timeout + 1000);
        cache.dumpCache();
        assertNotNull("Should have been able to fetch this expired object", cache.fetchObject(cacheKey, "foo"));
        assertNull("Got an object from the cache that should have expired", cache.fetchObject(cacheKey, "bar"));
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    public void testFetchFromFileCache() throws Exception {
        final int timeout = 1000;
        DocumentCache cache = new DocumentCache(0, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestFetchFromCache"); //$NON-NLS-1$
        final String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        final String cacheKey = "foo1";
        cache.addToCache(cacheKey, newString, newString.length(), "foo"); //$NON-NLS-1$
        cache.dumpCache();
        Object newObject = cache.fetchObject(cacheKey, "foo");
        assertEquals("Got the wrong value out of the cache", newString, ((String)newObject));
        assertEquals("Memory cache size is wrong", 0, cache.getCurrentMemoryCacheSize());
        assertEquals("File cache size is wrong", newString.length(), cache.getCurrentFileCacheSize());
        
        cache.dumpCache();
        Thread.sleep(timeout + 1000);
        cache.dumpCache();
        Object expObj = cache.fetchObject(cacheKey, null);
        assertNull("Got an object from the cache that should have expired", expObj);
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    public void testCacheUncacheable() {
        try{
        //what happens if an object can't go in either cache?
        final int memoryCache = 10;
        final int fileCache = 10;
        final String tooBigString = "this is more than 10";
        final String cacheKey = "foo";
        DocumentCache cache = new DocumentCache(memoryCache, fileCache, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestCacheUncacheachable");
        cache.addToCache(cacheKey, tooBigString, tooBigString.length(), "foo");
        cache.dumpCache();
        assertEmpty(cache);
        cache.shutdownCleaner();
        }catch(Throwable t){
        	fail(t.getMessage());
        }

    }
    
    private void assertEmpty(DocumentCache cache) {
        assertZero("Cache count is not zero", cache.getCacheCount());
        assertZero("Memory cache size is not zero", cache.getCurrentMemoryCacheSize());
        assertZero("File cache size is not zero", cache.getCurrentFileCacheSize());                     
    }

    private void assertZero(String message, int item) {
        assertEquals(message, 0, item);
    }

    public void testCacheStraightToFile() throws ConnectorException {
        //what happens if we skip the memory cache?
        DocumentCache cache = new DocumentCache(0, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestCacheStraightFromFile"); //$NON-NLS-1$
        final String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        final String cacheKey = "foo1";
        cache.addToCache(cacheKey, newString, newString.length(), "foo"); //$NON-NLS-1$
        cache.dumpCache();
        assertZero("Memory cache size is not zero", cache.getCurrentMemoryCacheSize());
        assertEquals("File cache size is wrong", cache.getCurrentFileCacheSize(), newString.length());
        cache.shutdownCleaner();
    }
    
    public void testCacheToFileTooBig() throws ConnectorException {
        //what happens if we try to send something to the file cache
        //but its too big?
        final int memoryCache = 0;
        final int fileCache = 10;
        final String tooBigString = "this is more than 10";
        DocumentCache cache = new DocumentCache(memoryCache, fileCache, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestCacheToFileTooBig"); //$NON-NLS-1$
        final String cacheKey = "foo1";
        cache.addToCache(cacheKey, tooBigString, tooBigString.length(), "foo"); //$NON-NLS-1$
        cache.dumpCache();
        assertZero("Memory cache size is not zero", cache.getCurrentMemoryCacheSize());
        assertZero("File cache size is not zero", cache.getCurrentFileCacheSize());
        cache.shutdownCleaner();
    }
    
    public void testCleanerShutdown() throws Exception {
        DocumentCache cache = new DocumentCache(0, CACHE_SIZE, CACHE_LOC, 8000, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestCleanerShutdown"); //$NON-NLS-1$
        final String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        final String cacheKey = "foo1";
        cache.addToCache(cacheKey, newString, newString.length(), "foo"); //$NON-NLS-1$
        cache.dumpCache();
        cache.shutdownCleaner(true);
        //Thread.sleep(100000);
        cache.dumpCache();
        assertZero("Memory cache size is not equal to zero.",  cache.getCurrentMemoryCacheSize());
        assertZero("File cache size is not equal to zero.", cache.getCurrentFileCacheSize());
    }
    
    
    public void testFetchOlderObject() throws ConnectorException {
        final String id1 = "foo1"; //$NON-NLS-1$
        final String id2 = "foo2"; //$NON-NLS-1$
        final String id3 = "foo3"; //$NON-NLS-1$
        
        IDocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, 500, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        String string1 = new String("blah, blah, blah"); //$NON-NLS-1$
        String string2 = new String("blab, blab, blab"); //$NON-NLS-1$
        String string3 = new String("slab, slab, slab"); //$NON-NLS-1$
        cache.addToCache(id1, string1, string1.length(), "foo");
		cache.addToCache(id2, string2, string2.length(), "foo");
        cache.addToCache(id3, string3, string3.length(), "foo");
        Object obj1 = cache.fetchObject(id1, null);
        assertEquals("Got the wrong value out of the cache", string1, ((String) obj1));
        cache.shutdownCleaner();
    }
    
    public void testRemoveFromCache() throws Exception {
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestRemoveFromCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.release(id, "foo");
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id, "foo"));
        cache.dumpCache();
        cache.shutdownCleaner();
    }
    
    /*public void testMultithreaded() throws Exception {
        final String id1 = "foo1"; //$NON-NLS-1$
        final String id2 = "foo2"; //$NON-NLS-1$
        final String id3 = "foo3"; //$NON-NLS-1$
        
        ArrayList IDs = new ArrayList(3);
        IDs.add(id1);
        IDs.add(id2);
        IDs.add(id3);
        
        
        String string1 = new String("blah, blah, blah"); //$NON-NLS-1$
        String string2 = new String("blab, blab, blab"); //$NON-NLS-1$
        String string3 = new String("slab, slab, slab"); //$NON-NLS-1$

        IDocumentCache cache = new DocumentCache(16, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        cache.addToCache(id1, string1, string1.length(), "foo");
        cache.addToCache(id2, string2, string2.length(), "foo");
        cache.addToCache(id3, string3, string3.length(), "foo");

        IDocumentCache cache2 = new DocumentCache(16, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        cache2.addToCache(id1, string1, string1.length(), "foo");
        cache2.addToCache(id2, string2, string2.length(), "foo");
        cache2.addToCache(id3, string3, string3.length(), "foo");

        IDocumentCache cache3 = new DocumentCache(16, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        cache3.addToCache(id1, string1, string1.length(), "foo");
        cache3.addToCache(id2, string2, string2.length(), "foo");
        cache3.addToCache(id3, string3, string3.length(), "foo");

        IDocumentCache cache4 = new DocumentCache(16, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        cache4.addToCache(id1, string1, string1.length(), "foo");
        cache4.addToCache(id2, string2, string2.length(), "foo");
        cache4.addToCache(id3, string3, string3.length(), "foo");

        IDocumentCache cache5 = new DocumentCache(16, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(6), "TestFetchOlderObject"); //$NON-NLS-1$
        cache5.addToCache(id1, string1, string1.length(), "foo");
        cache5.addToCache(id2, string2, string2.length(), "foo");
        cache5.addToCache(id3, string3, string3.length(), "foo");

    	Fetcher F = new Fetcher(cache, IDs);
        Fetcher F2 = new Fetcher(cache, IDs);
        Fetcher F3 = new Fetcher(cache, IDs);
        Fetcher F4 = new Fetcher(cache, IDs);
        Fetcher F5 = new Fetcher(cache, IDs);
        F.start();
        F2.start();
        F3.start();
        F4.start();
        F5.start();
        F.join();
        F2.join();
        F3.join();
        F4.join();
        F5.join();
        cache.shutdownCleaner();
        cache2.shutdownCleaner();
        cache3.shutdownCleaner();
        cache4.shutdownCleaner();
        cache5.shutdownCleaner();
    }
    
    public class Fetcher extends Thread {
        
        IDocumentCache cache;
        ArrayList IDs;
        
        public Fetcher(IDocumentCache cache, ArrayList IDs) {
            this.cache = cache;
            this.IDs = IDs;
        }

        public void run() {
            for(int i = 0; i < 100; i++) {
                cache.fetchObject((String)IDs.get(0), null);
                cache.fetchObject((String)IDs.get(1), null);
                cache.fetchObject((String)IDs.get(2), null);
            }            
        }
        
        
    }*/

    //TODO: synchronization tests
}