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

package com.metamatrix.connector.xml.cache;

import java.io.File;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * 
 * Tests the document cache with performance caching on.  Performance caching holds the documents
 * in the cache for a specified time in order to give the user a performance increase by reducing
 * the number of network requests made to slow changing services.
 *
 */
public class TestPerformanceCache extends TestCase {

    private static final String CACHE_LOC = UnitTestUtil.getTestScratchPath() + File.separator + "cache"; //$NON-NLS-1$ //$NON-NLS-2$
    /**
     * 
     */
    
    private final int CACHE_SIZE = 512;
    private final int CACHE_TIMEOUT = 60000;
    private final int LOGGING_LEVEL = 1;
    
    public TestPerformanceCache() {
        super();
    }

    /**
     * @param arg0
     */
    public TestPerformanceCache(String arg0) {
        super(arg0);
    }
    
    public void setUp() {
        File cacheFile = new File(CACHE_LOC);
        if(!cacheFile.exists()) {
            cacheFile.mkdirs();
        }
    }
    
    public void testAddToCache() throws Exception {
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestAddToCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache("foo1", newString, newString.length(), "foo"); //$NON-NLS-1$
        
        assertEquals("Memory cache size is wrong", cache.getCurrentMemoryCacheSize(), newString.length());
        assertEquals("File cache size is wrong", cache.getCurrentFileCacheSize(), 0);
        cache.dumpCache();
        cache.clearCache();
    }
    
    /**
     * In this test we allow the item to expire, but we do not release our reference to it.  I this scenario we
     * should till be able to get it if we pass the correct id, queryID and partID.  We should not get it with
     * the correct id and queryID that is different from what we created it with.
     * @throws Exception
     */
    public void testExpireFromCacheNotReleased() throws Exception {
        final int timeout = 1000;
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestExpireFromCacheNotReleased"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id, "foo"));
        Thread.sleep(timeout * 2);
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id, "goo"));
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.dumpCache();
        cache.clearCache();
    }


    /**
     * In this test we release our reference to the cache object, but don't wait long enough for it to expire.
     * Because performance cache is on, we should till find it.
     * @throws Exception
     */
    public void testRemoveFromCache() throws Exception {
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "testRemoveFromCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.release(id, "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "baz"));
        cache.dumpCache();
        cache.clearCache();
    }
    
    /**
     * In this test we allow the item to expire, but we do not release our reference to it, assert is still
     * accessible,release it, and assert it is no longer accessible.
     * @throws Exception
     */
    public void testExpireFromCacheNotReleasedAndRemove() throws Exception {
        final int timeout = 1000;
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestExpireFromCacheNotReleased"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id, "foo"));
        Thread.sleep(timeout * 2);
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.release(id, "foo");
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id,  "foo"));
        cache.dumpCache();
        cache.clearCache();
    }
}
