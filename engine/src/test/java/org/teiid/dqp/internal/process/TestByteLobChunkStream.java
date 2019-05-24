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

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.teiid.client.lob.LobChunkInputStream;
import org.teiid.core.util.ObjectConverterUtil;

import junit.framework.TestCase;


public class TestByteLobChunkStream extends TestCase {

    public void testGetChunk() throws Exception {
        byte[] bytes = "hello world".getBytes(); //$NON-NLS-1$
        ByteLobChunkStream stream = new ByteLobChunkStream(new ByteArrayInputStream(bytes), 5);

        assertTrue(Arrays.equals(bytes, ObjectConverterUtil.convertToByteArray(new LobChunkInputStream(stream))));
    }

}
