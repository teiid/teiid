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

package com.metamatrix.common.buffer.impl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.core.util.UnitTestUtil;
public class TestFileStorageManager {
		
	public FileStorageManager getStorageManager(Integer maxFileSize, Integer openFiles, String dir) throws MetaMatrixComponentException {
        Properties resourceProps = new Properties();
        FileStorageManager sm = new FileStorageManager();
        sm.setStorageDirectory(UnitTestUtil.getTestScratchPath() + (dir != null ? File.separator + dir : "")); //$NON-NLS-1$
        if (maxFileSize != null) {
        	sm.setMaxFileSizeDirect(maxFileSize);
        }
        if (openFiles != null) {
        	sm.setMaxOpenFiles(openFiles);
        }
        sm.initialize(resourceProps);
        return sm;
	}
    
    @Test public void testAddGetBatch1() throws Exception {
        StorageManager sm = getStorageManager(null, null, null);        
        String tsID = "local,1:0";     //$NON-NLS-1$
        // Add one batch
        FileStore store = sm.createFileStore(tsID);
        writeBytes(store);
        // Get that batch
        store.remove();
    }
            
    @Test public void testCreatesSpillFiles() throws Exception {
        FileStorageManager sm = getStorageManager(1024, null, null); // 1KB
        String tsID = "local,1:0";     //$NON-NLS-1$
        // Add one batch
        FileStore store = sm.createFileStore(tsID);
        writeBytes(store);
        
        Map<File, RandomAccessFile> cache = sm.getFileCache();
        assertEquals(1, cache.size());

        writeBytes(store);
        
        assertEquals(2, cache.size());
        
        store.remove();
        
        assertEquals(0, cache.size());
    }

    static Random r = new Random();
    
	private void writeBytes(FileStore store)
			throws MetaMatrixComponentException {
		byte[] bytes = new byte[2048];
        r.nextBytes(bytes);
        long start = store.write(bytes, 0, bytes.length);
        byte[] bytesRead = new byte[2048];        
        store.readFully(start, bytesRead, 0, bytesRead.length);
        assertTrue(Arrays.equals(bytes, bytesRead));
	}
    
}
