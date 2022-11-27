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
import java.io.InputStream;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stax.StAXSource;

import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.AccessibleByteArrayOutputStream;

/**
 * Provides an {@link InputStream} adapter for StAX
 */
public class XMLInputStream extends InputStream {
    private static final int BUFFER_SIZE = 1<<13;
    private int pos = 0;
    private AccessibleByteArrayOutputStream baos = new AccessibleByteArrayOutputStream(BUFFER_SIZE);
    private XMLEventReader reader;
    private XMLEventWriter writer;

    /**
     * Return a UTF-8 {@link InputStream} of the XML
     * @param source
     * @param outFactory
     * @throws XMLStreamException
     */
    public XMLInputStream(StAXSource source, XMLOutputFactory outFactory) throws XMLStreamException {
        this(source, outFactory, Streamable.ENCODING);
    }

    public XMLInputStream(StAXSource source, XMLOutputFactory outFactory, String encoding) throws XMLStreamException {
        reader = source.getXMLEventReader();
        if (reader == null) {
            this.reader = XMLType.getXmlInputFactory().createXMLEventReader(source.getXMLStreamReader());
        }
        this.writer = outFactory.createXMLEventWriter(baos, encoding);
    }

    @Override
    public int read() throws IOException {
        while (pos >= baos.getCount()) {
            if (!reader.hasNext()) {
                return -1;
            }
            if (baos.getCount() > BUFFER_SIZE) {
                baos.setCount(0);
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
        return 0xff & baos.getBuffer()[pos++];
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

}