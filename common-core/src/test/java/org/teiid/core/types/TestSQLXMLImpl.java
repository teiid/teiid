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
import java.sql.SQLException;

import javax.xml.transform.stream.StreamSource;

import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestSQLXMLImpl {

    String testStr = "<foo>test</foo>"; //$NON-NLS-1$

    @Test public void testGetSource() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertTrue(xml.getSource(null) instanceof StreamSource);

        StreamSource ss = (StreamSource)xml.getSource(null);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(ss.getInputStream()), Streamable.ENCODING));
    }

    @Test public void testGetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, ObjectConverterUtil.convertToString(xml.getCharacterStream()));
    }

    @Test public void testGetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, new String(ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()), Streamable.ENCODING));
    }

    @Test public void testGetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        assertEquals(testStr, xml.getString());
    }

    @Test(expected=SQLException.class) public void testSetBinaryStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        xml.setBinaryStream();
    }

    @Test(expected=SQLException.class) public void testSetCharacterStream() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        xml.setCharacterStream();
    }

    @Test(expected=SQLException.class) public void testSetString() throws Exception {
        SQLXMLImpl xml = new SQLXMLImpl(testStr);
        xml.setString(testStr);
    }

    @Test public void testGetString1() throws Exception {
        SQLXMLImpl clob = new SQLXMLImpl() {
            public java.io.Reader getCharacterStream() throws java.sql.SQLException {
                return new Reader() {

                    int pos = 0;

                    @Override
                    public void close() throws IOException {

                    }

                    @Override
                    public int read(char[] cbuf, int off, int len)
                            throws IOException {
                        if (pos < 5) {
                            cbuf[off] = 'a';
                            pos++;
                            return 1;
                        }
                        return -1;
                    }
                };
            }
        };
        assertEquals("aaaaa", clob.getString());
    }

}
