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

package org.teiid.client.lob;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.lob.LobChunkInputStream;
import org.teiid.client.lob.LobChunkProducer;
import org.teiid.core.util.ObjectConverterUtil;

import junit.framework.TestCase;


public class TestLobChunkInputStream extends TestCase {

    public void testReadByteArray() throws Exception {
        LobChunkProducer chunkProducer = new LobChunkProducer() {

            Iterator<LobChunk> chuncks = Arrays.asList(new LobChunk("hello ".getBytes(), false), new LobChunk("world".getBytes(), true)).iterator(); //$NON-NLS-1$ //$NON-NLS-2$

            @Override
            public LobChunk getNextChunk() throws IOException {
                return chuncks.next();
            }

            @Override
            public void close() throws IOException {

            }
        };
        LobChunkInputStream stream = new LobChunkInputStream(chunkProducer);

        assertEquals("hello world", ObjectConverterUtil.convertToString(stream)); //$NON-NLS-1$
    }

}
