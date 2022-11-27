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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Reader;

import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestClobValue {

    @Test public void testClobValue() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());

        ClobType cv = new ClobType(clob);
        assertEquals(testString, cv.getSubString(1L, (int)cv.length()));
    }

    @Test public void testClobValuePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());

        ClobType cv = new ClobType(clob);
        String key = cv.getReferenceStreamId();

        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);

        assertTrue(read.length() > 0);

        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());

        // and lost the original object
        assertNull(read.getReference());
    }

    @Test public void testReferencePersistence() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());

        ClobType cv = new ClobType(clob);
        cv.setReferenceStreamId(null);

        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);

        assertTrue(read.length() > 0);

        assertEquals(testString, read.getSubString(1, testString.length()));
    }

    @SuppressWarnings("serial")
    @Test public void testReferencePersistenceError() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray()) {
            @Override
            public Reader getCharacterStream() throws SerialException {
                throw new SerialException();
            }
        };

        ClobType cv = new ClobType(clob);
        cv.setReferenceStreamId(null);

        // now force to serialize
        ClobType read = UnitTestUtil.helpSerialize(cv);

        assertTrue(read.length() > 0);
        assertNotNull(read.getReferenceStreamId());
        assertNull(read.getReference());
    }

    @Test public void testClobSubstring() throws Exception {
        ClobImpl clob = new ClobImpl() {
            public java.io.Reader getCharacterStream() throws java.sql.SQLException {
                return new Reader() {

                    int pos = 0;

                    @Override
                    public void close() throws IOException {

                    }

                    @Override
                    public int read(char[] cbuf, int off, int len)
                            throws IOException {
                        if (pos < 2) {
                            cbuf[off] = 'a';
                            pos++;
                            return 1;
                        }
                        return -1;
                    }
                };
            }
        };
        assertEquals("aa", clob.getSubString(1, 3));

        assertEquals("", clob.getSubString(1, 0));

        clob = new ClobImpl("hello world");

        assertEquals("hel", clob.getSubString(1, 3));

        assertEquals("orld", clob.getSubString(8, 5));
    }

    @Test public void testClobCompare() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        ClobType ct = new ClobType(clob);

        SerialClob clob1 = new SerialClob(testString.toCharArray());
        ClobType ct1 = new ClobType(clob1);
        assertEquals(0, ct1.compareTo(ct));
    }

    @Test public void testClobHashError() throws Exception {
        String testString = "this is test clob"; //$NON-NLS-1$
        SerialClob clob = new SerialClob(testString.toCharArray());
        clob.free();
        ClobType ct = new ClobType(clob);
        assertEquals(0, ct.hashCode());
    }

    @Test public void testClobPosition() throws Exception {
        String testString = "this is \u10000 test clob"; //$NON-NLS-1$
        ClobImpl clobImpl = new ClobImpl(testString);

        assertEquals(testString.indexOf("test"), clobImpl.position("test", 2) - 1);
    }

}
