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
import java.nio.ByteBuffer;

import org.junit.Test;

public class TestExtensibleBufferedInputStream {

    @Test public void testReset() throws IOException {
        InputStream is = new ExtensibleBufferedInputStream() {
            boolean returned = false;
            @Override
            protected ByteBuffer nextBuffer() throws IOException {
                if (returned) {
                    return null;
                }
                ByteBuffer result = ByteBuffer.allocate(3);
                returned = true;
                return result;
            }
        };
        is.read();
        is.read();
        is.reset();
        for (int i = 0; i < 3; i++) {
            assertEquals(0, is.read());
        }
        assertEquals(-1, is.read());
    }

}
