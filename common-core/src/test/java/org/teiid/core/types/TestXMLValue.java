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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestXMLValue {

    @Test public void testXMLValue() throws Exception {
        String testString = "<foo>this is an xml value test</foo>"; //$NON-NLS-1$
        SQLXMLImpl xml = new SQLXMLImpl(testString);

        XMLType xv = new XMLType(xml);
        assertEquals(testString, xv.getString());
    }


    @Test public void testXMLValuePersistence() throws Exception {
        String testString = "<foo>this is an xml value test</foo>"; //$NON-NLS-1$
        SQLXMLImpl xml = new SQLXMLImpl(testString);

        XMLType xv = new XMLType(xml);
        String key = xv.getReferenceStreamId();

        // now force to serialize
        XMLType read = UnitTestUtil.helpSerialize(xv);

        // make sure we have kept the reference stream id
        assertEquals(key, read.getReferenceStreamId());

        // and lost the original object
        assertNull(read.getReference());
    }

    @Test public void testReferencePersistence() throws Exception {
        String testString = "<foo>this is an xml value test</foo>"; //$NON-NLS-1$
        SQLXMLImpl xml = new SQLXMLImpl(testString);

        XMLType xv = new XMLType(xml);
        xv.setReferenceStreamId(null);

        // now force to serialize
        XMLType read = UnitTestUtil.helpSerialize(xv);

        assertEquals(testString, read.getString());
    }


    @Test public void testLength() throws Exception {
        String testString = "<foo>this is an xml value test</foo>"; //$NON-NLS-1$
        SQLXMLImpl xml = new SQLXMLImpl(testString);

        XMLType xv = new XMLType(xml);
        assertEquals(36, xv.length());

        xml = new SQLXMLImpl(new InputStreamFactory() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream("<bar/>".getBytes(Streamable.CHARSET));
            }
        });

        xv = new XMLType(xml);
        try {
            xv.length();
            fail();
        } catch (SQLException e) {

        }
    }
}
