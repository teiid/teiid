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

        store.setLength(10000);

        assertEquals(5, msm.getCreated());

        store.setLength(100);

        assertEquals(4, msm.getRemoved());

        store.remove();

        assertEquals(5, msm.getRemoved());
    }

    @Test public void testTruncate() throws Exception {
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

        store.setLength(100);

        assertEquals(1, msm.getRemoved());

    }

}
