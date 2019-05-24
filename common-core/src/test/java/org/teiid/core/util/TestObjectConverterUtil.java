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

package org.teiid.core.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestObjectConverterUtil {

    @Test public void testWriteWithLength() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5});
        ObjectConverterUtil.write(baos, bais, new byte[3], 4);
        assertEquals(4, baos.toByteArray().length);

        StringWriter sw = new StringWriter();
        StringReader sr = new StringReader("123");
        ObjectConverterUtil.write(sw, sr, 2, true);
        assertEquals(2, sw.toString().length());
    }

    @Test public void testCloseArguments() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream() {
            public void close() throws IOException {
                throw new AssertionError();
            }
        };
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5});
        ObjectConverterUtil.write(baos, bais, new byte[3], 4, false, true);
        assertEquals(4, baos.toByteArray().length);
    }

}
