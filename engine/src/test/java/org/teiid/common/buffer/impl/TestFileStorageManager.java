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

package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestFileStorageManager {
		
	public FileStorageManager getStorageManager(Integer openFiles, String dir) throws TeiidComponentException {
        FileStorageManager sm = new FileStorageManager();
        sm.setStorageDirectory(UnitTestUtil.getTestScratchPath() + (dir != null ? File.separator + dir : "")); //$NON-NLS-1$
        if (openFiles != null) {
        	sm.setMaxOpenFiles(openFiles);
        }
        sm.initialize();
        return sm;
	}
    
    @Test public void testWrite() throws Exception {
        FileStorageManager sm = getStorageManager(null, null);        
        String tsID = "0";     //$NON-NLS-1$
        FileStore store = sm.createFileStore(tsID);
        writeBytes(store);
        assertEquals(2048, sm.getUsedBufferSpace());
        store.remove();
        assertEquals(0, sm.getUsedBufferSpace());
    }
    
    @Test public void testPositionalWrite() throws Exception {
        FileStorageManager sm = getStorageManager(null, null);        
        String tsID = "0";     //$NON-NLS-1$
        FileStore store = sm.createFileStore(tsID);
        byte[] expectedBytes = writeBytes(store, 2048);
        assertEquals(4096, sm.getUsedBufferSpace());
        
        writeBytes(store, 4096);
        assertEquals(6144, sm.getUsedBufferSpace());
        
        byte[] bytesRead = new byte[2048];        
        store.readFully(2048, bytesRead, 0, bytesRead.length);
        
        assertArrayEquals(expectedBytes, bytesRead);
        
        store.remove();
        assertEquals(0, sm.getUsedBufferSpace());
    }
            
    @Test(expected=IOException.class) public void testMaxSpace() throws Exception {
    	FileStorageManager sm = getStorageManager(null, null); 
    	sm.setMaxBufferSpace(1);
        String tsID = "0";     //$NON-NLS-1$
        // Add one batch
        FileStore store = sm.createFileStore(tsID);
        writeBytes(store);
    }
    
    @Test public void testFlush() throws Exception {
    	FileStorageManager sm = getStorageManager(null, null);
    	FileStore store = sm.createFileStore("0");
    	FileStoreOutputStream fsos = store.createOutputStream(2);
    	fsos.write(new byte[3]);
    	fsos.write(1);
    	fsos.flush();
    	assertEquals(0, fsos.getCount());
    }

    static Random r = new Random();
    
	static void writeBytes(FileStore store) throws IOException {
		writeBytes(store, store.getLength());
	}

	static byte[] writeBytes(FileStore store, long start)
			throws IOException {
		byte[] bytes = new byte[2048];
        r.nextBytes(bytes);
        store.write(start, bytes, 0, bytes.length);
        byte[] bytesRead = new byte[2048];        
        store.readFully(start, bytesRead, 0, bytesRead.length);
        assertTrue(Arrays.equals(bytes, bytesRead));
        return bytes;
	}
    
    @Test public void testWritingMultipleFiles() throws Exception {
    	FileStorageManager sm = getStorageManager(null, null); 
        String tsID = "0";     //$NON-NLS-1$
        // Add one batch
        FileStore store = sm.createFileStore(tsID);
        String contentOrig = new String("some file content this will stored in same tmp file with another");
        OutputStream out = store.createOutputStream();
        out.write(contentOrig.getBytes(), 0, contentOrig.getBytes().length);
        out.close();

        out = store.createOutputStream();
        long start = store.getLength();
        byte[] bytesOrig = new byte[2048];
        r.nextBytes(bytesOrig);
        out.write(bytesOrig, 0, 2048);
        
        byte[] readContent = new byte[2048];
        InputStream in = store.createInputStream(0, contentOrig.getBytes().length);        
    	int c = in.read(readContent, 0, 3000);
       	assertEquals(contentOrig, new String(readContent, 0, c));       	
       	c = in.read(readContent, 0, 3000);
       	assertEquals(-1, c);
       	in.close();
        
        in = store.createInputStream(start, 2048);
        c = in.read(readContent, 0, 3000);
        assertTrue(Arrays.equals(bytesOrig, readContent));
       	c = in.read(readContent, 0, 3000);
       	assertEquals(-1, c);
       	in.close();        
    }	
	
}
