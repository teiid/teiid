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

package org.teiid.util;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stax.StAXSource;

import org.teiid.core.types.XMLType;

/**
 * Provides a {@link Reader} adapter for StAX
 */
public class XMLReader extends Reader {
    private static final int BUFFER_SIZE = 1<<13;
    private int pos = 0;
    private StringBuilder builder = new StringBuilder(BUFFER_SIZE);
    private XMLEventReader reader;
    private XMLEventWriter writer;

    public XMLReader(StAXSource source, XMLOutputFactory outFactory) throws XMLStreamException {
        reader = source.getXMLEventReader();
        if (reader == null) {
            this.reader = XMLType.getXmlInputFactory().createXMLEventReader(source.getXMLStreamReader());
        }
        this.writer = outFactory.createXMLEventWriter(new Writer() {

            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                builder.append(cbuf, off, len);
            }

            @Override
            public void flush() throws IOException {

            }

            @Override
            public void write(String str, int off, int len) throws IOException {
                builder.append(str, off, len);
            }

            @Override
            public void close() throws IOException {

            }
        });
    }

    @Override
    public int read() throws IOException {
        while (pos >= builder.length()) {
            if (!reader.hasNext()) {
                return -1;
            }
            if (builder.length() > BUFFER_SIZE) {
                builder.setLength(0);
                pos = 0;
            }
            try {
                XMLEvent event = reader.nextEvent();
                writer.add(event);
                writer.flush();
            } catch (XMLStreamException e) {
                throw new IOException(e);
            }
        }
        return builder.charAt(pos++);
    }


    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int i = 0;
        int c = 0;
        for (i = 0; i < len; i++) {
            c = read();
            if (c == -1) {
                if (i == 0) {
                    return -1;
                }
                break;
            }
            cbuf[i+off] = (char)c;
        }
        return i;
    }


}