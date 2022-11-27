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

package org.teiid.netty.handler.codec.serialization;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.AccessibleBufferedInputStream;
import org.teiid.core.util.ReaderInputStream;

public class TestObjectDecoderInputStream {

    @Test public void testTimeoutException() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectEncoderOutputStream oeos = new ObjectEncoderOutputStream(new DataOutputStream(baos), 512);
        List<Integer> obj = Arrays.asList(1, 2, 3);
        oeos.writeObject(obj);
        oeos.close();
        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        InputStream is = new InputStream() {
            int count;
            @Override
            public int read() throws IOException {
                if (count++%2==0) {
                    throw new SocketTimeoutException();
                }
                return bais.read();
            }
        };
        ObjectDecoderInputStream odis = new ObjectDecoderInputStream(new AccessibleBufferedInputStream(is, 1024), Thread.currentThread().getContextClassLoader(), 1024);
        Object result = null;
        do {
            try {
                result = odis.readObject();
            } catch (IOException e) {

            }
        } while (result == null);
        assertEquals(obj, result);
    }

    @Test public void testReplaceObject() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectEncoderOutputStream out = new ObjectEncoderOutputStream(new DataOutputStream(baos), 512);

        ClobImpl clob = new ClobImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ReaderInputStream(new StringReader("Clob contents"),  Charset.forName(Streamable.ENCODING)); //$NON-NLS-1$
            }

        }, -1);

        out.writeObject(clob);

        ObjectDecoderInputStream in = new ObjectDecoderInputStream(new AccessibleBufferedInputStream(new ByteArrayInputStream(baos.toByteArray()), 1024), Thread.currentThread().getContextClassLoader(), 1024);
        Object result = in.readObject();
        assertTrue(result instanceof ClobImpl);
    }
}
