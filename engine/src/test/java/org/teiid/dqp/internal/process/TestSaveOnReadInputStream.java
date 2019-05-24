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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestSaveOnReadInputStream {

    @Test public void testSave() throws IOException {
        SaveOnReadInputStream soris = getSaveOnReadInputStream();
        InputStreamFactory isf = soris.getInputStreamFactory();
        InputStream is = isf.getInputStream();
        assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
        InputStream is2 = isf.getInputStream();
        assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is2), Streamable.CHARSET));
    }

    @Test public void testPartialReadSave() throws IOException {
        SaveOnReadInputStream soris = getSaveOnReadInputStream();
        InputStreamFactory isf = soris.getInputStreamFactory();
        InputStream is = isf.getInputStream();
        is.read();

        InputStream is2 = isf.getInputStream();
        assertEquals("ello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
        assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is2), Streamable.CHARSET));
        InputStream is3 = isf.getInputStream();
        assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is3), Streamable.CHARSET));
    }

    @Test public void testStorageMode() throws IOException {
        SaveOnReadInputStream soris = getSaveOnReadInputStream();
        InputStreamFactory isf = soris.getInputStreamFactory();

        assertEquals(StorageMode.MEMORY, isf.getStorageMode());

        InputStream is = isf.getInputStream();
        assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
    }

    private SaveOnReadInputStream getSaveOnReadInputStream() {
        FileStore fs = BufferManagerFactory.getStandaloneBufferManager().createFileStore("test");
        FileStoreInputStreamFactory factory = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);

        InputStream is = new ByteArrayInputStream("hello world".getBytes(Streamable.CHARSET));

        SaveOnReadInputStream soris = new SaveOnReadInputStream(is, factory);
        return soris;
    }

}
