/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.xquery.saxon;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.ProcessingInstruction;
import javax.xml.stream.events.StartDocument;
import javax.xml.stream.events.XMLEvent;

/**
 * Implementation of the {@link javax.xml.stream.XMLStreamReader} interface that wraps a {@link XMLEventReader}. Useful,
 * because the StAX {@link javax.xml.stream.XMLInputFactory} allows one to create a event reader from a stream reader,
 * but not vice-versa.
 *
 * @author Arjen Poutsma
 * @since 3.0
 */
class XMLEventStreamReader extends AbstractXMLStreamReader {

    private XMLEvent event;

    private final XMLEventReader eventReader;

    XMLEventStreamReader(XMLEventReader eventReader) throws XMLStreamException {
        this.eventReader = eventReader;
        event = eventReader.nextEvent();
    }

    public boolean isStandalone() {
        if (event.isStartDocument()) {
            return ((StartDocument) event).isStandalone();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public String getVersion() {
        if (event.isStartDocument()) {
            return ((StartDocument) event).getVersion();
        }
        else {
            return null;
        }
    }

    public int getTextStart() {
        return 0;
    }

    public String getText() {
        if (event.isCharacters()) {
            return event.asCharacters().getData();
        } else if (event.getEventType() == COMMENT) {
            return event.toString().substring(4, event.toString().length()-3);
        } else {
            throw new IllegalStateException();
        }
    }

    public String getPITarget() {
        if (event.isProcessingInstruction()) {
            return ((ProcessingInstruction) event).getTarget();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public String getPIData() {
        if (event.isProcessingInstruction()) {
            return ((ProcessingInstruction) event).getData();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public int getNamespaceCount() {
        initNamespaces();
        return namespacesList.size();
    }

    public NamespaceContext getNamespaceContext() {
        if (event.isStartElement()) {
            return event.asStartElement().getNamespaceContext();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public QName getName() {
        if (event.isStartElement()) {
            return event.asStartElement().getName();
        }
        else if (event.isEndElement()) {
            return event.asEndElement().getName();
        }
        else {
            throw new IllegalStateException();
        }
    }

    public Location getLocation() {
        return event.getLocation();
    }

    public int getEventType() {
        return event.getEventType();
    }

    public String getEncoding() {
        return null;
    }

    public String getCharacterEncodingScheme() {
        return null;
    }

    public int getAttributeCount() {
        initAttributes();
        return attributesList.size();
    }

    public void close() throws XMLStreamException {
        eventReader.close();
    }

    public QName getAttributeName(int index) {
        return getAttribute(index).getName();
    }

    public String getAttributeType(int index) {
        return getAttribute(index).getDTDType();
    }

    public String getAttributeValue(int index) {
        return getAttribute(index).getValue();
    }

    public String getNamespacePrefix(int index) {
        return getNamespace(index).getPrefix();
    }

    public String getNamespaceURI(int index) {
        return getNamespace(index).getNamespaceURI();
    }

    public Object getProperty(String name) throws IllegalArgumentException {
        return eventReader.getProperty(name);
    }

    public boolean isAttributeSpecified(int index) {
        return getAttribute(index).isSpecified();
    }

    public int next() throws XMLStreamException {
        event = eventReader.nextEvent();
        return event.getEventType();
    }

    public boolean standaloneSet() {
        if (event.isStartDocument()) {
            return ((StartDocument) event).standaloneSet();
        }
        else {
            throw new IllegalStateException();
        }
    }

    private XMLEvent attributesEvent;
    List<Attribute> attributesList;

    private Attribute getAttribute(int index) {
        initAttributes();
        return attributesList.get(index);
    }

    private void initAttributes() {
        if (!event.isStartElement()) {
            throw new IllegalStateException();
        }
        if (event != attributesEvent) {
            attributesEvent = event;
            attributesList = new ArrayList<>();
            Iterator attributes = event.asStartElement().getAttributes();
            while (attributes.hasNext()) {
                Attribute attribute = (Attribute) attributes.next();
                attributesList.add(attribute);
            }
        }
    }

    private XMLEvent namespacesEvent;
    List<Namespace> namespacesList;

    private Namespace getNamespace(int index) {
        initNamespaces();
        return namespacesList.get(index);
    }

    private void initNamespaces() {
        if (event != namespacesEvent) {
            namespacesEvent = event;
            Iterator namespaces;
            if (event.isStartElement()) {
                namespaces = event.asStartElement().getNamespaces();
            }
            else if (event.isEndElement()) {
                namespaces = event.asEndElement().getNamespaces();
            }
            else {
                throw new IllegalStateException();
            }
            namespacesList = new ArrayList<>();
            while (namespaces.hasNext()) {
                Namespace namespace = (Namespace) namespaces.next();
                namespacesList.add(namespace);
            }
        }
    }
}
