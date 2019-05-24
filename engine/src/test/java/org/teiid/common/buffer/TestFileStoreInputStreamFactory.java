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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.teiid.core.types.Streamable;

@SuppressWarnings("nls")
public class TestFileStoreInputStreamFactory {

    @Test public void testInputStream() throws IOException {
        FileStore fs = BufferManagerFactory.getStandaloneBufferManager().createFileStore("test");
        FileStoreInputStreamFactory factory = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
        OutputStream os = factory.getOuputStream(0);
        os.write(new byte[2]);
        os.close();
        InputStream is = factory.getInputStream(0, 1);
        is.read();
        assertEquals(-1, is.read());
    }

}
