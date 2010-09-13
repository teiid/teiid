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

package org.teiid.dqp.service.buffer;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.services.BufferServiceImpl;

@SuppressWarnings("nls")
public class TestLocalBufferService {

    @Test public void testCheckMemPropertyGotSet() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(true);
        
        svc.start();
        // all the properties are set
        assertTrue("Not Directory", svc.getBufferDirectory().isDirectory()); //$NON-NLS-1$
        assertTrue("does not exist", svc.getBufferDirectory().exists()); //$NON-NLS-1$
        assertTrue("does not end with one", svc.getBufferDirectory().getParent().endsWith("1")); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(svc.isUseDisk());
        
        BufferManagerImpl mgr = (BufferManagerImpl) svc.getBufferManager();
        assertTrue(((FileStorageManager)mgr.getStorageManager()).getDirectory().endsWith(svc.getBufferDirectory().getName()));
    }

    @Test public void testCheckMemPropertyGotSet2() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(false);
        svc.start();
        
        // all the properties are set
        assertFalse(svc.isUseDisk());
    }
    
}
