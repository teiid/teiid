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

import java.util.Arrays;

import org.teiid.client.lob.LobChunk;
import org.teiid.core.util.UnitTestUtil;

import junit.framework.TestCase;



public class TestByteLobChunk extends TestCase {

    public void testGetBytes() {
        String testString = "This is test string for testing ByteLobChunk"; //$NON-NLS-1$
        LobChunk chunk = new LobChunk(testString.getBytes(), false);
        assertEquals(testString, new String(chunk.getBytes()));
        assertFalse(chunk.isLast());
    }

    public void testSerialization() throws Exception {
        String testString = "This is test string for testing ByteLobChunk"; //$NON-NLS-1$
        LobChunk chunk = new LobChunk(testString.getBytes(), true);

        LobChunk result = UnitTestUtil.helpSerialize(chunk);
        assertTrue(Arrays.equals(chunk.getBytes(), result.getBytes()));
        assertTrue(result.isLast());
    }

}
