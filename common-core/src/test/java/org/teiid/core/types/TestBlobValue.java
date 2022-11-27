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

package org.teiid.core.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.sql.rowset.serial.SerialBlob;

import junit.framework.TestCase;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


public class TestBlobValue extends TestCase {

    public void testBlobValue() throws Exception {
        String testString = "this is test blob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());

        BlobType bv = new BlobType(blob);
        assertEquals(testString, new String(bv.getBytes(1L, (int)bv.length())));
    }

    public void testBlobValuePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());

        BlobType bv = new BlobType(blob);
        String key = bv.getReferenceStreamId();

        // now force to serialize
        BlobType read = UnitTestUtil.helpSerialize(bv);

        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());

        // and lost the original object
        assertNull(read.getReference());
    }

    @Test public void testReferencePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());

        BlobType bv = new BlobType(blob);
        bv.setReferenceStreamId(null);
        // now force to serialize
        BlobType read = UnitTestUtil.helpSerialize(bv);

        assertNull(read.getReferenceStreamId());

        assertEquals(testString, new String(read.getBytes(1, (int)blob.length())));
    }

    public void testBlobCompare() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialBlob blob = new SerialBlob(testString.getBytes());
        BlobType bv = new BlobType(blob);

        SerialBlob blob1 = new SerialBlob(testString.getBytes());
        BlobType bv1 = new BlobType(blob1);
        assertEquals(0, bv1.compareTo(bv));
    }

    public void testBlobImplGetBytes() throws Exception {
        BlobImpl b = new BlobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(new byte[0]);
            }
        });
        byte[] b1 = b.getBytes(1, 0);
        assertEquals(0, b1.length);
        byte[] b2 = b.getBytes(1, 1);
        assertEquals(0, b2.length);

        b = new BlobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(new byte[]{1,2});
            }
        });

        byte[] b3 = b.getBytes(1, 1);
        assertEquals(1, b3.length);
    }

}
