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

import org.junit.Test;
import org.teiid.common.buffer.FileStore;

public class TestSplittableStorageManager {
	
    @Test public void testCreatesSpillFiles() throws Exception {
    	MemoryStorageManager msm = new MemoryStorageManager();
        SplittableStorageManager ssm = new SplittableStorageManager(msm);
        ssm.setMaxFileSizeDirect(2048);
        String tsID = "0";     //$NON-NLS-1$
        // Add one batch
        FileStore store = ssm.createFileStore(tsID);
        TestFileStorageManager.writeBytes(store);
        
        assertEquals(1, msm.getCreated());

        TestFileStorageManager.writeBytes(store);
        
        assertEquals(2, msm.getCreated());
        
        store.remove();
        
        assertEquals(2, msm.getRemoved());
    }

}
