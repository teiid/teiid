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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
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
        
        BufferManagerImpl mgr = svc.getBufferManager();
        SplittableStorageManager ssm = (SplittableStorageManager)((BufferFrontedFileStoreCache)mgr.getCache()).getStorageManager();
        assertTrue(((FileStorageManager)ssm.getStorageManager()).getDirectory().endsWith(svc.getBufferDirectory().getName()));
    }

    @Test public void testCheckMemPropertyGotSet2() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(false);
        svc.start();
        
        // all the properties are set
        assertFalse(svc.isUseDisk());
    }
    
    @Test public void testSchemaSize() throws Exception {
    	//82 strings of Total Length 2515 charcacters
    	//11 Dates
    	//1 Long
    	//1 short
    	//20 bigdecimal with 671 total integers in them.
    	List<Expression> schema = new ArrayList<Expression>();
    	for (int i = 0; i <82; i++) {
    		schema.add(new Constant(null, DataTypeManager.DefaultDataClasses.STRING));
    	}
    	for (int i = 0; i <11; i++) {
    		schema.add(new Constant(null, DataTypeManager.DefaultDataClasses.DATE));
    	}
    	schema.add(new Constant(null, DataTypeManager.DefaultDataClasses.LONG));
    	schema.add(new Constant(null, DataTypeManager.DefaultDataClasses.SHORT));
    	for (int i = 0; i <20; i++) {
    		schema.add(new Constant(null, DataTypeManager.DefaultDataClasses.BIG_DECIMAL));
    	}
    	
    	BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(false);
        svc.start();
        
        BufferManager mgr = svc.getBufferManager();
        assertEquals(3364096, mgr.getSchemaSize(schema));
        assertEquals(128, mgr.getProcessorBatchSize(schema));
    }
    
}
