/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.cache;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.cdk.api.EnvironmentUtility;

/**
 * 
 * Tests the document cache with performance caching on.  Performance caching holds the documents
 * in the cache for a specified time in order to give the user a performance increase by reducing
 * the number of network requests made to slow changing services.
 *
 */
public class TestPerformanceCache extends TestCase {

    private static final String CACHE_LOC = System.getProperty("user.dir") + File.separator + "cache"; //$NON-NLS-1$ //$NON-NLS-2$
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
    
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestPerformanceCache("testCacheSetup"));
        suite.addTest(new TestPerformanceCache("testAddToCache")); //$NON-NLS-1$
        suite.addTest(new TestPerformanceCache("testExpireFromCacheNotReleased")); //$NON-NLS-1$
        suite.addTest(new TestPerformanceCache("testRemoveFromCache")); //$NON-NLS-1$
        suite.addTest(new TestPerformanceCache("testExpireFromCacheNotReleasedAndRemove")); //$NON-NLS-1$
        /*        suite.addTest(new CacheTest("testOverloadMemoryCache")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testOverloadFileCache")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testFetchFromMemoryCache")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testFetchFromFileCache")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testCacheUncacheable")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testFetchOlderObject")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testCacheStraightToFile")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testCacheToFileTooBig")); //$NON-NLS-1$
        suite.addTest(new CacheTest("testCleanerShutdown")); //$NON-NLS-1$
*/      return suite;        
    }
    
    public void setUp() {
        File cacheFile = new File(CACHE_LOC);
        if(!cacheFile.exists()) {
            cacheFile.mkdirs();
        }
    }
    
    
    public void testCacheSetup() {
        try{
        IDocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestPerformanceCacheSetup"); //$NON-NLS-1$
        assertNotNull(cache);
        }catch(Throwable t){
            assertTrue("Exception occurred: " + t.getMessage(), false);
        }
    }
    
    public void testAddToCache() {
        try{
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestAddToCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache("foo1", newString, newString.length(), "foo"); //$NON-NLS-1$
        
        assertEquals("Memory cache size is wrong", cache.getCurrentMemoryCacheSize(), newString.length());
        assertEquals("File cache size is wrong", cache.getCurrentFileCacheSize(), 0);
        cache.dumpCache();
        cache.clearCache();
        }catch(Throwable t){
            assertTrue("Exception occurred: " + t.getMessage(), false);
        }
    }
    
    /**
     * In this test we allow the item to expire, but we do not release our reference to it.  I this scenario we
     * should till be able to get it if we pass the correct id, queryID and partID.  We should not get it with
     * the correct id and queryID that is different from what we created it with.
     * @throws Exception
     */
    public void testExpireFromCacheNotReleased() throws Exception {
        try{
        final int timeout = 10000;
        final long sleepySleepy = 1000;
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestExpireFromCacheNotReleased"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id, "foo"));
        long current = System.currentTimeMillis();
        while(System.currentTimeMillis() < (current + timeout)){};   
        Thread.sleep(sleepySleepy);
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id, "goo"));
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.dumpCache();
        cache.clearCache();
        }catch(Throwable t){
            assertTrue("Exception occurred: " + t.getMessage(), false);
        }
    }


    /**
     * In this test we release our reference to the cache object, but don't wait long enough for it to expire.
     * Because performance cache is on, we should till find it.
     * @throws Exception
     */
    public void testRemoveFromCache() throws Exception {
        try{
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, CACHE_TIMEOUT, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "testRemoveFromCache"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.release(id, "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "baz"));
        cache.dumpCache();
        cache.clearCache();
        }catch(Throwable t){
            assertTrue("Exception occurred: " + t.getMessage(), false);
        }
    }
    
    /**
     * In this test we allow the item to expire, but we do not release our reference to it, assert is still
     * accessible,release it, and assert it is no longer accessible.
     * @throws Exception
     */
    public void testExpireFromCacheNotReleasedAndRemove() throws Exception {
        try{
        final int timeout = 10000;
        final long sleepySleepy = 1000;
        final String id = "foo1"; //$NON-NLS-1$
        DocumentCache cache = new DocumentCache(CACHE_SIZE, CACHE_SIZE, CACHE_LOC, timeout, EnvironmentUtility.createStdoutLogger(LOGGING_LEVEL), "TestExpireFromCacheNotReleased"); //$NON-NLS-1$
        String newString = new String("blah, blah, blah"); //$NON-NLS-1$
        cache.addToCache(id, newString, newString.length(), "foo");
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id, "foo"));
        long current = System.currentTimeMillis();
        while(System.currentTimeMillis() < (current + timeout)){};   
        Thread.sleep(sleepySleepy);
        assertNotNull("Didn't find an object in the cache we should have.", cache.fetchObject(id,  "foo"));
        cache.release(id, "foo");
        assertNull("Found an object in the cache we shouldn't have.", cache.fetchObject(id,  "foo"));
        cache.dumpCache();
        cache.clearCache();
        }catch(Throwable t){
            assertTrue("Exception occurred: " + t.getMessage(), false);
        }
    }
}
