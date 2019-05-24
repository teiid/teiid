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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.common.buffer.impl.EncryptedStorageManager.EncryptedFileStore;

public class TestEncryptedStorageManager {

    @Test public void testSetLength() throws Exception {
        MemoryStorageManager msm = new MemoryStorageManager();
        EncryptedStorageManager ssm = new EncryptedStorageManager(msm);
        ssm.initialize();
        String tsID = "0";     //$NON-NLS-1$
        // Add one batch
        EncryptedFileStore store = ssm.createFileStore(tsID);
        TestFileStorageManager.writeBytes(store);

        assertEquals(1, msm.getCreated());

        TestFileStorageManager.writeBytes(store);
        assertEquals(4096, store.getLength());
        assertEquals(4096, store.getFile().getLength());
        store.setLength(256); //multiple of the block size
        assertEquals(256, store.getLength());
        assertEquals(256, store.getFile().getLength());

        store.setLength(100);
        assertEquals(100, store.getLength());
        assertEquals(112, store.getFile().getLength());
        store.readFully(0, new byte[100], 0, 100);
    }

    @Test public void testReadWrite() throws Exception {
        MemoryStorageManager msm = new MemoryStorageManager();
        EncryptedStorageManager ssm = new EncryptedStorageManager(msm);
        ssm.initialize();
        String tsID = "0";     //$NON-NLS-1$
        EncryptedFileStore store = ssm.createFileStore(tsID);
        for (int i = 0; i < 500; i++) {
            byte[] b = new byte[i];
            Arrays.fill(b, (byte)i);
            store.write(b, 0, i);
            store.readFully(store.getLength()-b.length, b, 0, b.length);
            for (int j = 0; j < b.length; j++) {
                assertEquals((byte)i, b[j]);
            }
        }
        int start = 0;
        for (int i = 0; i < 500; i++) {
            byte[] b = new byte[i];
            store.readFully(start, b, 0, b.length);
            for (int j = 0; j < b.length; j++) {
                assertEquals((byte)i, b[j]);
            }
            start += i;
        }
        store.readFully(0, new byte[(int) store.getLength()], 0, (int) store.getLength());
        store.write(16, new byte[100], 0, 100);
        store.write((int)store.getLength() - 100, new byte[99], 0, 99);
    }

    @Test(expected=IOException.class) public void testInvalidRead() throws Exception {
        MemoryStorageManager msm = new MemoryStorageManager();
        EncryptedStorageManager ssm = new EncryptedStorageManager(msm);
        ssm.initialize();
        String tsID = "0";     //$NON-NLS-1$
        EncryptedFileStore store = ssm.createFileStore(tsID);
        store.read(1, new byte[1], 0, 1);
    }


}
