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

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/** 
 * Cache of files (byte[]s) and their checksums, by filename.
 * The values in the cache are contained in SoftReferences, 
 * so they may be removed by the garbage collector when necessary.
 * Other than that, there is no cache replacement.
 * @since 4.2
 */
public class FileCache {
    
    
    /**
     * Set of Strings: file types to cache. 
     */
    private static Set typesToCache = new HashSet();
    
    
    public static final long UNKNOWN_CHECKSUM = -1; 

    
    /**
     * Map of <String filename> -> <FileCacheElement>
     */
    private Map map = new HashMap();
    
    
    public void addTypeToCache(String type) {
        typesToCache.add(type);
    }
    
    
    
    
    public long getChecksum(String filename) {
         FileCacheElement element = (FileCacheElement) map.get(filename);
         if (element == null) {
             return UNKNOWN_CHECKSUM;
         }
         
         return element.getChecksum();         
    }
    
    
    public byte[] getBytes(String filename) {
        FileCacheElement element = (FileCacheElement) map.get(filename);
        if (element == null) {
            return null;
        }
        
        Reference reference = element.getBytesReference();
        byte[] bytes = (byte[]) reference.get();
        
        //remove key from the map if byte[] has been gc'd
        if (bytes == null) {
            map.remove(filename);
        }        

        
        return bytes;   
    }
    
    
    public void put(String filename, long checksum, byte[] bytes, String type) {
        if (! typesToCache.contains(type)) {
            return;
        }
        
        
        FileCacheElement element = new FileCacheElement(filename, checksum, bytes);        
        map.put(filename, element);
    }
    
 
    
    public int size() {
        return map.size();
    }
    
    
    
    /**
     * Simple dataholder class. 
     * @since 4.2
     */    
    private static class FileCacheElement {
        private String filename;
        private long checksum;
        private Reference bytesReference;
        
        public FileCacheElement(String filename, long checksum, byte[] bytes) {
            this.filename = filename;
            this.checksum = checksum;
            this.bytesReference = new SoftReference(bytes);
        }
        
        public String getFilename() {
            return filename;
        }
        public long getChecksum() {
            return checksum;
        }
        public Reference getBytesReference() {
            return bytesReference;
        } 
    }
    
}

