/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.service.buffer;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.services.BufferServiceImpl;

@SuppressWarnings("nls")
public class TestLocalBufferService {

    @Test public void testCheckMemPropertyGotSet() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(true);
        svc.setInlineLobs(false);
        svc.start();
        // all the properties are set
        assertTrue("Not Directory", svc.getBufferDirectory().isDirectory()); //$NON-NLS-1$
        assertTrue("does not exist", svc.getBufferDirectory().exists()); //$NON-NLS-1$
        assertTrue("does not end with one", svc.getBufferDirectory().getParent().endsWith("1")); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(svc.isUseDisk());
        assertFalse(svc.isInlineLobs());
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

    @Test
    public void testStateTransfer() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(true);
        svc.start();

        BufferManager mgr = svc.getBufferManager();
        List<ElementSymbol> schema = new ArrayList<ElementSymbol>(2);
        ElementSymbol es = new ElementSymbol("x"); //$NON-NLS-1$
        es.setType(DataTypeManager.getDataTypeClass(DefaultDataTypes.STRING));
        schema.add(es);

        ElementSymbol es2 = new ElementSymbol("y"); //$NON-NLS-1$
        es2.setType(DataTypeManager.getDataTypeClass(DefaultDataTypes.INTEGER));
        schema.add(es2);

        TupleBuffer buffer = mgr.createTupleBuffer(schema, "cached", TupleSourceType.FINAL); //$NON-NLS-1$
        buffer.setBatchSize(50);
        buffer.setId("state_id");

        for (int batch=0; batch<3; batch++) {
            for (int row = 0; row < 50; row++) {
                int val = (batch*50)+row;
                buffer.addTuple(Arrays.asList(new Object[] {"String"+val, new Integer(val)}));
            }
        }
        buffer.close();
        mgr.distributeTupleBuffer(buffer.getId(), buffer);

        FileOutputStream fo = new FileOutputStream(UnitTestUtil.getTestScratchPath()+"/teiid/statetest");
        ((BufferManagerImpl)mgr).getState(buffer.getId(), fo);
        fo.close();
        svc.stop();

        // now read back
        BufferServiceImpl svc2 = new BufferServiceImpl();
        svc2.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/2");
        svc2.setUseDisk(true);
        svc2.start();

        BufferManagerImpl mgr2 = svc2.getBufferManager();
        FileInputStream fis = new FileInputStream(UnitTestUtil.getTestScratchPath()+"/teiid/statetest");
        mgr2.setState(buffer.getId(), fis);
        fis.close();

        String id = "state_id";
        buffer = mgr2.getTupleBuffer(id);
        for (int batch=0; batch<3; batch++) {
            TupleBatch tb = buffer.getBatch((batch*50)+1);
            List[] rows = tb.getAllTuples();
            for (int row = 0; row < 50; row++) {
                int val = (batch*50)+row;
                assertEquals("String"+val, rows[row].get(0));
                assertEquals(val, rows[row].get(1));
            }
        }
        svc2.stop();
    }

    @Test public void testUseDiskFalse() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setUseDisk(false);
        svc.start();
        // all the properties are set
        BufferManagerImpl mgr = svc.getBufferManager();
        FileStore f = mgr.getCache().createFileStore("x");
        f.write(new byte[1234], 0, 1234);
    }

    @Test public void testFixedSizing() throws Exception {
        BufferServiceImpl svc = new BufferServiceImpl();
        svc.setDiskDirectory(UnitTestUtil.getTestScratchPath()+"/teiid/1");
        svc.setVmMaxMemory(14L<<30);
        svc.setMaxReservedHeapMb(10<<10);
        svc.start();
        BufferManagerImpl impl = svc.getBufferManager();
        BufferFrontedFileStoreCache cache = (BufferFrontedFileStoreCache)impl.getCache();
        assertEquals(1073741824, cache.getMemoryBufferSpace());
    }
}
