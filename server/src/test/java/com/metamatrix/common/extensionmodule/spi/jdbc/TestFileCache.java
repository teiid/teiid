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

package com.metamatrix.common.extensionmodule.spi.jdbc;

import junit.framework.TestCase;


/** 
 * Tests <code>FileCache</code>
 * @since 4.2
 */
public class TestFileCache extends TestCase {

    private FileCache fileCache;
    
    
    public TestFileCache(String name) {
        super(name);
    }
    
    public void setUp() {
        fileCache = new FileCache();
    }
    
    
    public void testSetAndGet() {
        byte[] bytes1 = new byte[] {1};
        byte[] bytes2 = new byte[] {2};
        byte[] bytes3 = new byte[] {3};
        
        
        fileCache.addTypeToCache("JAR File"); //$NON-NLS-1$
        fileCache.put("key1", 1, bytes1, "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$
        fileCache.put("key2", 2, bytes2, "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$
        fileCache.put("key2", 3, bytes3, "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(bytes1, fileCache.getBytes("key1")); //$NON-NLS-1$ 
        assertEquals(1, fileCache.getChecksum("key1")); //$NON-NLS-1$ 
        
        assertEquals(bytes3, fileCache.getBytes("key2")); //$NON-NLS-1$
        assertEquals(3, fileCache.getChecksum("key2")); //$NON-NLS-1$
    }

       
    public void testFileTypes() {
        //add file of wrong type
        fileCache.addTypeToCache("JAR File"); //$NON-NLS-1$
        byte[] bytes1 = new byte[100];
        byte[] bytes2 = new byte[100];
        fileCache.put("key1", 1, bytes1, "foo"); //$NON-NLS-1$ //$NON-NLS-2$        
        assertNull(fileCache.getBytes("key1")); //$NON-NLS-1$
        assertEquals(FileCache.UNKNOWN_CHECKSUM, fileCache.getChecksum("key1")); //$NON-NLS-1$
               
        
        //add types, add files of right types
        fileCache = new FileCache();
        fileCache.addTypeToCache("type1"); //$NON-NLS-1$
        fileCache.addTypeToCache("type2"); //$NON-NLS-1$
        fileCache.put("key1", 1, bytes1, "type1"); //$NON-NLS-1$ //$NON-NLS-2$
        fileCache.put("key2", 2, bytes2, "type2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(bytes1, fileCache.getBytes("key1")); //$NON-NLS-1$
        assertEquals(1, fileCache.getChecksum("key1")); //$NON-NLS-1$
        assertEquals(bytes2, fileCache.getBytes("key2")); //$NON-NLS-1$
        assertEquals(2, fileCache.getChecksum("key2")); //$NON-NLS-1$
    }

    
    
    /**
     * Tests whether objects are cleared by garbage collection.
     * Because FileCache uses SoftReferences, objects are not cleared
     * unless the system comes close to using all of its memory.
     * TODO: how to test this case? 
     * 
     * @since 4.2
     */
    public void testReplacement() {
        byte[] bytes1 = new byte[100];
        byte[] bytes2 = new byte[100];
        
        fileCache.addTypeToCache("JAR File"); //$NON-NLS-1$
        fileCache.put("key1", 1, bytes1, "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$   
        fileCache.put("key2", 2, bytes2, "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$
        for (int i=0; i<10; i++) {
            System.gc();
        }
        
        //values should be in the cache, because there is still a reference to them
        assertEquals(bytes1, fileCache.getBytes("key1")); //$NON-NLS-1$ 
        assertEquals(bytes2, fileCache.getBytes("key2")); //$NON-NLS-1$ 
        assertEquals(2, fileCache.size());
        
        
        //remove references to the objects
        bytes1 = null;
        bytes2 = null;
        for (int i=0; i<10; i++) {
            System.gc();
        }
        
        //values should should be in the cache, as long as system is not close to out of memory 
        assertNotNull(fileCache.getBytes("key1")); //$NON-NLS-1$ 
        assertNotNull(fileCache.getBytes("key2")); //$NON-NLS-1$ 
        assertEquals(2, fileCache.size());
    }
    
    
}
