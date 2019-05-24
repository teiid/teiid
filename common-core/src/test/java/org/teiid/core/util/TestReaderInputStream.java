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

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestReaderInputStream {

    @Test public void testUTF8() throws Exception {
        FileInputStream fis = new FileInputStream(UnitTestUtil.getTestDataFile("legal_notice.xml")); //$NON-NLS-1$
        ReaderInputStream ris = new ReaderInputStream(new FileReader(UnitTestUtil.getTestDataFile("legal_notice.xml")), Charset.forName("UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$

        int value;
        while (true) {
            value = fis.read();
            assertEquals(value, ris.read());
            if (value == -1) {
                break;
            }
        }
    }

    @Test public void testUTF16() throws Exception {
        String actual = "!?abc"; //$NON-NLS-1$
        ReaderInputStream ris = new ReaderInputStream(new StringReader(actual), Charset.forName("UTF-16").newEncoder(), 2); //$NON-NLS-1$
        byte[] result = ObjectConverterUtil.convertToByteArray(ris);
        String resultString = new String(result, "UTF-16"); //$NON-NLS-1$
        assertEquals(resultString, actual);
    }

    @Test public void testASCII() throws Exception  {
        String actual = "!?abc"; //$NON-NLS-1$
        ReaderInputStream ris = new ReaderInputStream(new StringReader(actual), Charset.forName("US-ASCII").newEncoder(), 1); //$NON-NLS-1$
        byte[] result = ObjectConverterUtil.convertToByteArray(ris);
        String resultString = new String(result, "US-ASCII"); //$NON-NLS-1$
        assertEquals(resultString, actual);
    }

    @Test(expected=IOException.class) public void testASCIIError() throws Exception  {
        String actual = "!?abc\uffffafs"; //$NON-NLS-1$
        Charset cs = Charset.forName("ASCII");
        ReaderInputStream ris = new ReaderInputStream(new StringReader(actual), cs.newEncoder(), 1); //$NON-NLS-1$
        ObjectConverterUtil.convertToByteArray(ris);
    }

}
