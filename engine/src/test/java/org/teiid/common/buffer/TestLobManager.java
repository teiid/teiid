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
package org.teiid.common.buffer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.LobManager.ReferenceMode;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;

@SuppressWarnings("nls")
public class TestLobManager {

    @Test
    public void testLobPeristence() throws Exception{

        BufferManager buffMgr = BufferManagerFactory.getStandaloneBufferManager();
        FileStore fs = buffMgr.createFileStore("temp");

        ClobType clob = new ClobType(new ClobImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ReaderInputStream(new StringReader("Clob contents One"),  Charset.forName(Streamable.ENCODING));
            }

        }, -1));

        BlobType blob = new BlobType(new BlobImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ReaderInputStream(new StringReader("Blob contents Two"),  Charset.forName(Streamable.ENCODING));
            }

        }));

        BlobType blobEmpty = new BlobType(new BlobImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(new byte[0]);
            }

        }));

        FileStore fs1 = buffMgr.createFileStore("blob");
        FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs1, Streamable.ENCODING);
        FileStoreOutputStream fsos = fsisf.getOuputStream();
        byte[] b = new byte[DataTypeManager.MAX_LOB_MEMORY_BYTES + 1];
        fsos.write(b);
        fsos.close();
        BlobType blob1 = new BlobType(new BlobImpl(fsisf));

        assertNotNull(blob1.getReferenceStreamId());

        LobManager lobManager = new LobManager(new int[] {0, 1, 2, 3}, fs);
        lobManager.setMaxMemoryBytes(4);
        List<?> tuple = Arrays.asList(clob, blob, blob1, blobEmpty);
        lobManager.updateReferences(tuple, ReferenceMode.CREATE);

        assertNotNull(blob1.getReferenceStreamId());

        lobManager.persist();

        Streamable<?>lob = lobManager.getLobReference(clob.getReferenceStreamId());
        assertTrue(lob.getClass().isAssignableFrom(ClobType.class));
        ClobType clobRead = (ClobType)lob;
        assertEquals(ClobType.getString(clob), ClobType.getString(clobRead));
        assertTrue(clobRead.length() != -1);

        lob = lobManager.getLobReference(blob.getReferenceStreamId());
        assertTrue(lob.getClass().isAssignableFrom(BlobType.class));
        BlobType blobRead = (BlobType)lob;
        assertTrue(Arrays.equals(ObjectConverterUtil.convertToByteArray(blob.getBinaryStream()), ObjectConverterUtil.convertToByteArray(blobRead.getBinaryStream())));

        lobManager.updateReferences(tuple, ReferenceMode.REMOVE);

        assertEquals(0, lobManager.getLobCount());

    }

    @Test public void testInlining() throws Exception{

        BufferManager buffMgr = BufferManagerFactory.getStandaloneBufferManager();
        FileStore fs = buffMgr.createFileStore("temp");

        ClobType clob = new ClobType(new ClobImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ReaderInputStream(new StringReader("small"),  Charset.forName(Streamable.ENCODING));
            }

        }, 5));

        assertEquals(StorageMode.OTHER, InputStreamFactory.getStorageMode(clob));

        LobManager lobManager = new LobManager(new int[] {0}, fs);

        lobManager.updateReferences(Arrays.asList(clob), ReferenceMode.CREATE);

        assertEquals(StorageMode.MEMORY, InputStreamFactory.getStorageMode(clob));
    }

}
