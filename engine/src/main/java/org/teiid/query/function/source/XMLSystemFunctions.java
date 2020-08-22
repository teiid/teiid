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

package org.teiid.query.function.source;

import java.io.BufferedReader;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.EventFilter;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.Location;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BaseLob;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.StandardXMLTranslator;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.json.simple.ContentHandler;
import org.teiid.json.simple.JSONParser;
import org.teiid.json.simple.ParseException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.util.CommandContext;
import org.teiid.util.CharsetUtils;
import org.teiid.util.StAXSQLXML;
import org.teiid.util.StAXSQLXML.StAXSourceProvider;
import org.teiid.util.WSUtil;

/**
 * This class contains scalar system functions supporting for XML manipulation.
 *
 * @since 4.2
 */
public class XMLSystemFunctions {

    private static final Charset UTF_32BE = Charset.forName("UTF-32BE"); //$NON-NLS-1$
    private static final Charset UTF_16BE = Charset.forName("UTF-16BE"); //$NON-NLS-1$
    private static final Charset UTF_32LE = Charset.forName("UTF-32LE"); //$NON-NLS-1$
    private static final Charset UTF_16LE = Charset.forName("UTF-16LE"); //$NON-NLS-1$
    private static final Charset UTF_8 = Charset.forName("UTF-8"); //$NON-NLS-1$

    private static final Location dummyLocation = new Location() {
        @Override
        public String getSystemId() {
            return null;
        }

        @Override
        public String getPublicId() {
            return null;
        }

        @Override
        public int getLineNumber() {
            return -1;
        }

        @Override
        public int getColumnNumber() {
            return -1;
        }

        @Override
        public int getCharacterOffset() {
            return -1;
        }
    };

    private static final EventFilter declarationOmittingFilter = new EventFilter() {
        @Override
        public boolean accept(XMLEvent event) {
            return !event.isStartDocument() && !event.isEndDocument();
        }
    };

    private static final class DeclarationStaxSourceProvider implements
            StAXSourceProvider {
        private final XMLEvent start;
        private XMLType value;

        private DeclarationStaxSourceProvider(XMLEvent start, XMLType value) {
            this.start = start;
            this.value = value;
        }

        @Override
        public StAXSource getStaxSource() throws SQLException {
            StAXSource source = value.getSource(StAXSource.class);
            try {
                XMLEventReader reader = getXMLEventReader(source);
                reader = new EventReaderDelegate(reader) {
                    @Override
                    public XMLEvent nextEvent() throws XMLStreamException {
                        return replaceStart(super.nextEvent());
                    }

                    @Override
                    public XMLEvent peek() throws XMLStreamException {
                        return replaceStart(super.peek());
                    }

                    private XMLEvent replaceStart(XMLEvent event) {
                        if (event != null && event.getEventType() == XMLEvent.START_DOCUMENT) {
                            return start;
                        }
                        return event;
                    }

                    @Override
                    public Object next() {
                        try {
                            return nextEvent();
                        } catch (XMLStreamException e) {
                            throw new NoSuchElementException();
                        }
                    }
                };
                return new StAXSource(reader);
            } catch (XMLStreamException e) {
                throw new SQLException(e);
            }
        }

    }

    private static final class JsonToXmlContentHandler implements
            ContentHandler, XMLEventReader {
        private Reader reader;
        private JSONParser parser;
        private XMLEventFactory eventFactory;

        private LinkedList<String> nameStack = new LinkedList<String>();
        private LinkedList<XMLEvent> eventStack = new LinkedList<XMLEvent>();
        private LinkedList<Boolean> parentArray = new LinkedList<Boolean>();

        private boolean end;
        private boolean declaredNs;

        private JsonToXmlContentHandler(String rootName,
                Reader reader, JSONParser parser, XMLEventFactory eventFactory) {
            this.nameStack.push(XMLHelper.getInstance().escapeName(rootName, true));
            this.reader = reader;
            this.eventFactory = eventFactory;
            this.parser = parser;
        }

        @Override
        public boolean startObjectEntry(String key)
                throws org.teiid.json.simple.ParseException, IOException {
            this.nameStack.push(XMLHelper.getInstance().escapeName(key, true));
            return false;
        }

        @Override
        public boolean startObject() throws org.teiid.json.simple.ParseException,
                IOException {
            parentArray.push(false);
            start(null);
            return false;
        }

        private void start(Attribute attribute) {
            Iterator<Namespace> namespaces = null;
            Iterator<Attribute> attributes = null;
            if (!declaredNs) {
                namespaces = Arrays.asList(eventFactory.createNamespace("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI)).iterator(); //$NON-NLS-1$
                declaredNs = true;
            }
            if (attribute != null) {
                attributes = Arrays.asList(attribute).iterator();
            }
            eventStack.add(eventFactory.createStartElement("", "", nameStack.peek(), attributes, namespaces)); //$NON-NLS-1$ //$NON-NLS-2$
        }

        @Override
        public void startJSON() throws org.teiid.json.simple.ParseException,
                IOException {
            //specify the defaults, since different providers emit/omit differently
            eventStack.add(eventFactory.createStartDocument("UTF-8", "1.0")); //$NON-NLS-1$ //$NON-NLS-2$
        }

        @Override
        public boolean startArray() throws org.teiid.json.simple.ParseException,
                IOException {
            if ((nameStack.size() == 1 && parentArray.isEmpty()) || parentArray.peek()) {
                start(null);
            }
            parentArray.push(true);
            return false;
        }

        @Override
        public boolean primitive(Object value)
                throws org.teiid.json.simple.ParseException, IOException {
            if (value != null) {
                String type = "decimal"; //$NON-NLS-1$
                if (value instanceof String) {
                    type = null;
                } else if (value instanceof Boolean) {
                    type = "boolean"; //$NON-NLS-1$
                }
                if (type != null) {
                    //we need to differentiate boolean/decimal entries from their string counter parts
                    start(eventFactory.createAttribute("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, "type", type)); //$NON-NLS-1$ //$NON-NLS-2$
                } else {
                    start(null);
                }
                eventStack.add(eventFactory.createCharacters(value.toString()));
            } else {
                start(eventFactory.createAttribute("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, "nil", "true")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            end();
            return true; //return true, otherwise we don't get the endObjectEntry
        }

        private void end() {
            Iterator<Namespace> namespaces = null;
            if (nameStack.size() == 1) {
                namespaces = Arrays.asList(eventFactory.createNamespace("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI)).iterator(); //$NON-NLS-1$
            }
            eventStack.add(eventFactory.createEndElement("", "", nameStack.peek(), namespaces)); //$NON-NLS-1$ //$NON-NLS-2$
        }

        @Override
        public boolean endObjectEntry()
                throws org.teiid.json.simple.ParseException, IOException {
            this.nameStack.pop();
            return false;
        }

        @Override
        public boolean endObject() throws org.teiid.json.simple.ParseException,
                IOException {
            parentArray.pop();
            end();
            return false;
        }

        @Override
        public void endJSON() throws org.teiid.json.simple.ParseException,
                IOException {
            this.eventStack.add(eventFactory.createEndDocument());
            end = true;
        }

        @Override
        public boolean endArray() throws org.teiid.json.simple.ParseException,
                IOException {
            parentArray.pop();
            if ((nameStack.size() == 1 && parentArray.isEmpty()) || parentArray.peek()) {
                end();
            }
            return false;
        }

        @Override
        public void close() throws XMLStreamException {
            try {
                //this is explicitly against the javadoc, but
                //it's our only chance to close the reader
                this.reader.close();
            } catch (IOException e) {
            }
        }

        @Override
        public String getElementText() throws XMLStreamException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getProperty(String name) throws IllegalArgumentException {
            return null;
        }

        @Override
        public boolean hasNext() {
            return !eventStack.isEmpty() || !end;
        }

        @Override
        public XMLEvent nextEvent() throws XMLStreamException {
            while (eventStack.isEmpty() && !end) {
                try {
                    parser.parse(reader, this, true);
                } catch (IOException e) {
                    throw new XMLStreamException(e);
                } catch (ParseException e) {
                    throw new XMLStreamException(e);
                }
            }
            return eventStack.remove();
        }

        @Override
        public XMLEvent nextTag() throws XMLStreamException {
            throw new UnsupportedOperationException();
        }

        @Override
        public XMLEvent peek() throws XMLStreamException {
            if (hasNext()) {
                XMLEvent next = next();
                this.eventStack.push(next);
                return next;
            }
            return null;
        }

        @Override
        public XMLEvent next() {
            try {
                return nextEvent();
            } catch (XMLStreamException e) {
                throw new TeiidRuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static ThreadLocal<XMLOutputFactory> threadLocalOutputFactory = new ThreadLocal<XMLOutputFactory>() {
        protected XMLOutputFactory initialValue() {
            return newXmlOutputFactory();
        }
    };
    static ThreadLocal<XMLEventFactory> threadLocalEventtFactory = new ThreadLocal<XMLEventFactory>() {
        protected XMLEventFactory initialValue() {
            return XMLEventFactory.newInstance();
        }
        public XMLEventFactory get() {
            XMLEventFactory eventFactory = super.get();
            eventFactory.setLocation(null);
            return eventFactory;
        }
    };
    private static final String P_OUTPUT_VALIDATE_STRUCTURE = "com.ctc.wstx.outputValidateStructure"; //$NON-NLS-1$
    static XMLOutputFactory newXmlOutputFactory() throws FactoryConfigurationError {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        if (factory.isPropertySupported(P_OUTPUT_VALIDATE_STRUCTURE)) {
            factory.setProperty(P_OUTPUT_VALIDATE_STRUCTURE, false);
        }
        return factory;
    }
    static XMLOutputFactory xmlOutputFactory = newXmlOutputFactory();

    private static XMLEventReader getXMLEventReader(StAXSource source) throws XMLStreamException {
        XMLEventReader reader = source.getXMLEventReader();
        if (reader == null) {
            XMLInputFactory inputFactory = XMLType.getXmlInputFactory();
            reader = inputFactory.createXMLEventReader(source.getXMLStreamReader());
        }
        return reader;
    }

    public static XMLOutputFactory getOutputFactory() throws FactoryConfigurationError {
        return getOutputFactory(false);
    }

    public static ClobType xslTransform(CommandContext context, Object xml, Object styleSheet) throws Exception {
        Source styleSource = null;
        Source xmlSource = null;
        try {
            styleSource = convertToSource(styleSheet);
            xmlSource = convertToSource(xml);
            final Source xmlParam = xmlSource;
            TransformerFactory factory = StandardXMLTranslator.getThreadLocalTransformerFactory();
            final Transformer transformer = factory.newTransformer(styleSource);

            //this creates a non-validated sqlxml - it may not be valid xml/root-less xml
            SQLXMLImpl result = XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {

                @Override
                public void translate(Writer writer) throws TransformerException {
                    //transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
                    // Feed the resultant I/O stream into the XSLT processor
                    transformer.transform(xmlParam, new StreamResult(writer));
                }
            }, context);
            return new ClobType(new ClobImpl(result.getStreamFactory(), -1));
        } finally {
            WSUtil.closeSource(styleSource);
            WSUtil.closeSource(xmlSource);
        }
    }

    public static XMLType xmlForest(final CommandContext context, final Evaluator.NameValuePair[] namespaces, final Evaluator.NameValuePair[] values) throws TeiidComponentException, TeiidProcessingException {
        boolean valueExists = false;
        for (Evaluator.NameValuePair nameValuePair : values) {
            if (nameValuePair.value != null) {
                valueExists = true;
                break;
            }
        }
        if (!valueExists) {
            return null;
        }

        XMLType result = new XMLType(XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {

            @Override
            public void translate(Writer writer) throws TransformerException,
                    IOException {
                try {
                    XMLOutputFactory factory = getOutputFactory();
                    XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
                    XMLEventFactory eventFactory = threadLocalEventtFactory.get();
                    for (Evaluator.NameValuePair nameValuePair : values) {
                        if (nameValuePair.value == null) {
                            continue;
                        }
                        addElement(nameValuePair.name, writer, eventWriter, eventFactory, namespaces, null, Collections.singletonList(nameValuePair.value));
                    }
                    eventWriter.close();
                } catch (XMLStreamException e) {
                    throw new TransformerException(e);
                }
            }
        }, context));
        result.setType(Type.CONTENT);
        return result;
    }

    /**
     * Basic support for xmlelement.
     * @param context
     * @param name
     * @param contents
     * @return
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    public static XMLType xmlElement(CommandContext context, final String name,
            final Evaluator.NameValuePair<String>[] namespaces, final Evaluator.NameValuePair<?>[] attributes, final List<?> contents) throws TeiidComponentException, TeiidProcessingException {
        XMLType result = new XMLType(XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {

            @Override
            public void translate(Writer writer) throws TransformerException,
                    IOException {
                try {
                    XMLOutputFactory factory = getOutputFactory();
                    XMLEventWriter eventWriter = factory.createXMLEventWriter(writer);
                    XMLEventFactory eventFactory = threadLocalEventtFactory.get();
                    addElement(name, writer, eventWriter, eventFactory, namespaces, attributes, contents);
                    eventWriter.close();
                } catch (XMLStreamException e) {
                    throw new TransformerException(e);
                }
            }

        }, context));
        result.setType(Type.ELEMENT);
        return result;
    }

    private static void addElement(final String name, Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory,
            Evaluator.NameValuePair<String> namespaces[], Evaluator.NameValuePair<?> attributes[], List<?> contents) throws XMLStreamException, IOException, TransformerException {
        String uri = null;
        if (namespaces != null) {
            //need to determine the uri before creating the start element
            for (Evaluator.NameValuePair<String> nameValuePair : namespaces) {
                if (nameValuePair.name == null) {
                    if (nameValuePair.value == null) {
                        uri = XMLConstants.NULL_NS_URI;
                    } else {
                        uri = nameValuePair.value;
                    }
                }
            }
        }
        eventWriter.add(eventFactory.createStartElement("", uri, name)); //$NON-NLS-1$
        if (namespaces != null) {
            for (Evaluator.NameValuePair<String> nameValuePair : namespaces) {
                if (nameValuePair.name == null) {
                    if (nameValuePair.value == null) {
                        eventWriter.add(eventFactory.createNamespace(XMLConstants.NULL_NS_URI));
                    } else {
                        eventWriter.add(eventFactory.createNamespace(nameValuePair.value));
                    }
                } else {
                    eventWriter.add(eventFactory.createNamespace(nameValuePair.name, nameValuePair.value));
                }
            }
        }
        if (attributes != null) {
            for (Evaluator.NameValuePair<?> nameValuePair : attributes) {
                if (nameValuePair.value != null) {
                    eventWriter.add(eventFactory.createAttribute(new QName(nameValuePair.name), XMLHelper.getInstance().convertToAtomicValue(nameValuePair.value)));
                }
            }
        }
        //add empty chars to close the start tag
        eventWriter.add(eventFactory.createCharacters("")); //$NON-NLS-1$
        for (Object object : contents) {
            convertValue(writer, eventWriter, eventFactory, object);
        }
        eventWriter.add(eventFactory.createEndElement("", null, name)); //$NON-NLS-1$
    }

    public static XMLType xmlConcat(CommandContext context, final XMLType xml, final Object... other) throws TeiidProcessingException {
        //determine if there is just a single xml value and return it
        XMLType singleValue = xml;
        XMLType.Type type = null;
        for (Object object : other) {
            if (object != null) {
                if (singleValue != null) {
                    type = Type.CONTENT;
                    break;
                }
                if (object instanceof XMLType) {
                    singleValue = (XMLType)object;
                } else {
                    type = Type.CONTENT;
                    break;
                }
            }
        }
        if (type == null) {
            return singleValue;
        }

        XmlConcat concat = new XmlConcat(context.getBufferManager());
        concat.addValue(xml);
        for (Object object : other) {
            concat.addValue(object);
        }
        return concat.close(context);
    }

    public static class XmlConcat {
        private XMLOutputFactory factory;
        private XMLEventWriter eventWriter;
        private Writer writer;
        private FileStoreInputStreamFactory fsisf;
        private FileStore fs;
        private Type type;
        private ExtendedWriter ew;

        public XmlConcat(BufferManager bm) throws TeiidProcessingException {
            fs = bm.createFileStore("xml"); //$NON-NLS-1$
            fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
            writer = fsisf.getWriter();
            ew = new ExtendedWriter(writer, fsisf);
            factory = getOutputFactory();
            try {
                eventWriter = factory.createXMLEventWriter(writer);
            } catch (XMLStreamException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30437, e);
            }
        }

        public void addValue(Object object) throws TeiidProcessingException {
            if (type == null) {
                if (object instanceof XMLType) {
                    type = ((XMLType)object).getType();
                }
            } else {
                type = Type.CONTENT;
            }
            try {
                convertValue(ew, eventWriter, threadLocalEventtFactory.get(), object);
            } catch (IOException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30438, e);
            } catch (XMLStreamException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30439, e);
            } catch (TransformerException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30440, e);
            }
        }

        public Writer getWriter() {
            return writer;
        }

        public XMLType close(CommandContext context) throws TeiidProcessingException {
            try {
                eventWriter.flush();
                ew.close();
            } catch (XMLStreamException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30441, e);
            } catch (IOException e) {
                fs.remove();
                 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30442, e);
            }
            XMLType result = new XMLType(createSQLXML(fsisf, ew, context));
            if (type == null) {
                result.setType(Type.CONTENT);
            } else {
                result.setType(type);
            }
            return result;
        }

    }

    public static XMLType xmlPi(String name) {
        return xmlPi(name, ""); //$NON-NLS-1$
    }

    public static XMLType xmlPi(String name, String content) {
        int start = 0;
        char[] chars = content.toCharArray();
        while (start < chars.length && chars[start] == ' ') {
            start++;
        }
        XMLType result = new XMLType(new SQLXMLImpl("<?" + name + " " + content.substring(start) + "?>")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        result.setType(Type.PI);
        return result;
    }

    static void convertValue(Writer writer, XMLEventWriter eventWriter, XMLEventFactory eventFactory, Object object) throws IOException,
            FactoryConfigurationError, XMLStreamException,
            TransformerException {
        if (object == null) {
            return;
        }
        Reader r = null;
        try {
            if (object instanceof XMLType) {
                XMLType xml = (XMLType)object;
                Type type = xml.getType();
                convertReader(writer, eventWriter, null, type, xml);
            } else if (object instanceof ClobType) {
                ClobType clob = (ClobType)object;
                StorageMode storageMode = InputStreamFactory.getStorageMode(clob);
                if ((storageMode == StorageMode.PERSISTENT || storageMode == StorageMode.OTHER) && writer instanceof ExtendedWriter) {
                    ExtendedWriter ew = (ExtendedWriter)writer;
                    if (ew.include(clob)) {
                        return;
                    }
                }
                r = clob.getCharacterStream();
                convertReader(writer, eventWriter, r, Type.TEXT, null);
            } else if (object instanceof BlobType) {
                BlobType blob = (BlobType)object;
                StorageMode storageMode = InputStreamFactory.getStorageMode(blob);
                ClobType clob = FunctionMethods.toChars(blob, CharsetUtils.BASE64_NAME, true);
                if ((storageMode == StorageMode.PERSISTENT || storageMode == StorageMode.OTHER) && writer instanceof ExtendedWriter) {
                    ExtendedWriter ew = (ExtendedWriter)writer;
                    if (ew.include(clob)) {
                        return;
                    }
                }
                r = clob.getCharacterStream();
                convertReader(writer, eventWriter, r, Type.TEXT, null);
            } else if (object instanceof BinaryType) {
                BinaryType binary = (BinaryType)object;
                ClobType clob = FunctionMethods.toChars(new BlobType(binary.getBytesDirect()), CharsetUtils.BASE64_NAME, true);
                r = clob.getCharacterStream();
                convertReader(writer, eventWriter, r, Type.TEXT, null);
            } else {
                String val = XMLHelper.getInstance().convertToAtomicValue(object);
                eventWriter.add(eventFactory.createCharacters(val));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (r != null) {
                r.close();
            }
        }
        //TODO: blob - with base64 encoding
    }

    private static void convertReader(Writer writer,
            XMLEventWriter eventWriter, Reader r, Type type, XMLType xml)
            throws XMLStreamException, IOException, FactoryConfigurationError, SQLException {
        switch(type) {
        case CONTENT:
        case ELEMENT:
        case PI:
        case COMMENT: {//write the value directly to the writer
            eventWriter.flush();
            StorageMode storageMode = InputStreamFactory.getStorageMode(xml);
            if ((storageMode == StorageMode.PERSISTENT || storageMode == StorageMode.OTHER) && writer instanceof ExtendedWriter) {
                ExtendedWriter ew = (ExtendedWriter)writer;
                if (ew.include(xml)) {
                    break;
                }
            }
            char[] buf = new char[1 << 13];
            int read = -1;
            if (r == null) {
                r = xml.getCharacterStream();
            }
            while ((read = r.read(buf)) != -1) {
                writer.write(buf, 0, read);
            }
            break;
        }
        case UNKNOWN:  //assume a document
        case DOCUMENT: //filter the doc declaration
            XMLEventReader eventReader = null;
            XMLInputFactory inputFactory = XMLType.getXmlInputFactory();
            if (r != null) {
                if (!(r instanceof BufferedReader)) {
                    r = new BufferedReader(r);
                }
                eventReader = inputFactory.createXMLEventReader(r);
            } else {
                StAXSource staxSource = xml.getSource(StAXSource.class);
                eventReader = staxSource.getXMLEventReader();
                if (eventReader == null) {
                    eventReader = inputFactory.createXMLEventReader(staxSource.getXMLStreamReader());
                }
            }
            eventReader = inputFactory.createFilteredReader(eventReader, declarationOmittingFilter);
            eventWriter.add(eventReader);
            break;
        case TEXT:
            if (r == null) {
                r = xml.getCharacterStream();
            }
            XMLEventFactory eventFactory = threadLocalEventtFactory.get();
            char[] buf = new char[1 << 13];
            int read = -1;
            while ((read = r.read(buf)) != -1) {
                eventWriter.add(eventFactory.createCharacters(new String(buf, 0, read)));
            }
            break;
        }
    }

    public static XMLType xmlComment(String comment) throws FunctionExecutionException {
        if (comment.contains("--") || comment.endsWith("-")) { //$NON-NLS-1$ //$NON-NLS-2$
            throw new FunctionExecutionException(QueryPlugin.Event.TEIID31159, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31159, comment));
        }
        XMLType result = new XMLType(new SQLXMLImpl("<!--" + comment + "-->")); //$NON-NLS-1$ //$NON-NLS-2$
        result.setType(Type.COMMENT);
        return result;
    }

    public static Source convertToSource(Object value) throws TeiidProcessingException {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof SQLXML) {
                return ((SQLXML)value).getSource(null);
            }
            if (value instanceof Clob) {
                return new StreamSource(((Clob)value).getCharacterStream());
            }
            if (value instanceof Blob) {
                return new StreamSource(((Blob)value).getBinaryStream());
            }
            if (value instanceof String) {
                return new StreamSource(new StringReader((String)value));
            }
        } catch (SQLException e) {
             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30443, e);
        }
        throw new AssertionError("Unknown type"); //$NON-NLS-1$
    }

    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Blob json) throws TeiidComponentException, TeiidProcessingException, SQLException, IOException {
        return jsonToXml(context, rootName, json, false);
    }

    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Blob json, boolean stream) throws TeiidComponentException, TeiidProcessingException, SQLException, IOException {
        Reader r = getJsonReader(json);
        return jsonToXml(context, rootName, r, stream);
    }

    public static InputStreamReader getJsonReader(final Blob json) throws SQLException,
            IOException {
        InputStream is = json.getBinaryStream();
        PushbackInputStream pStream = new PushbackInputStream(is, 4);
        byte[] encoding = new byte[4];
        int read = pStream.read(encoding);
        pStream.unread(encoding, 0, read);
        Charset charset = UTF_8;
        if (read > 3) {
            if (encoding[0] == 0 && encoding[2] == 0) {
                if (encoding[1] == 0) {
                    charset = UTF_32BE;
                } else {
                    charset = UTF_16BE;
                }
            } else if (encoding[1] == 0 && encoding[3] == 0) {
                if (encoding[2] == 0) {
                    charset = UTF_32LE;
                } else {
                    charset = UTF_16LE;
                }
            }
        }
        return new InputStreamReader(pStream, charset);
    }

    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Clob json) throws TeiidComponentException, TeiidProcessingException, SQLException {
        return jsonToXml(context, rootName, json, false);
    }

    public static SQLXML jsonToXml(CommandContext context, final String rootName, final Clob json, boolean stream) throws TeiidComponentException, TeiidProcessingException, SQLException {
        return jsonToXml(context, rootName, json.getCharacterStream(), stream);
    }

    private static SQLXML jsonToXml(CommandContext context,
            final String rootName, final Reader r, boolean stream) throws TeiidComponentException,
            TeiidProcessingException {
        JSONParser parser = new JSONParser();
        final JsonToXmlContentHandler reader = new JsonToXmlContentHandler(rootName, r, parser, threadLocalEventtFactory.get());

        SQLXMLImpl sqlXml = null;
        if (stream) {
            try {
                //jre 1.7 event logic does not set a dummy location and throws an NPE in StAXSource, so we explicitly set a location
                //the streaming result will be directly consumed, so there's no danger that we're stepping on another location
                reader.eventFactory.setLocation(dummyLocation);
                sqlXml = new StAXSQLXML(new StAXSource(reader));
            } catch (XMLStreamException e) {
                throw new TeiidProcessingException(e);
            }
        } else {
            sqlXml = XMLSystemFunctions.saveToBufferManager(context.getBufferManager(), new XMLTranslator() {

                @Override
                public void translate(Writer writer) throws TransformerException,
                        IOException {
                    try {
                        XMLOutputFactory factory = getOutputFactory();
                        final XMLEventWriter streamWriter = factory.createXMLEventWriter(writer);

                        streamWriter.add(reader);
                        streamWriter.flush(); //woodstox needs a flush rather than a close
                    } catch (XMLStreamException e) {
                        throw new TransformerException(e);
                    } finally {
                        try {
                            r.close();
                        } catch (IOException e) {

                        }
                    }
                }
            }, context);
        }
        XMLType result = new XMLType(sqlXml);
        result.setType(Type.DOCUMENT);
        return result;
    }

    private static class Include {
        long start;
        Streamable<?> streamable;

        public Include(long start, Streamable<?> streamable) {
            this.start = start;
            this.streamable = streamable;
        }
    }

    /**
     * An extension to the standard writer to allow for
     * direct inclusion of large values by reference rather than by copy
     */
    public static class ExtendedWriter extends FilterWriter {

        private static int MAX_INCLUDES = 1<<12;

        private FileStoreInputStreamFactory fsisf;
        private List<Include> includes = new ArrayList<Include>();

        public ExtendedWriter(Writer out, FileStoreInputStreamFactory fsisf) {
            super(out);
            this.fsisf = fsisf;
        }

        /**
         * Include the streamable value, rather than inlining.
         * @param s
         * @return true if the value could be included
         * @throws IOException
         */
        public boolean include(Streamable<?> s) throws IOException {
            if (includes.size() == MAX_INCLUDES) {
                return false;
            }
            out.flush();
            long length = fsisf.getLength();
            includes.add(new Include(length, s));
            return true;
        }

    }

    /**
     * This method saves the given XML object to the buffer manager's disk process
     * Documents less than the maxMemorySize will be held directly in memory
     */
    public static SQLXMLImpl saveToBufferManager(BufferManager bufferMgr, XMLTranslator translator, CommandContext context)
        throws TeiidComponentException, TeiidProcessingException {
        boolean success = false;
        final FileStore lobBuffer = bufferMgr.createFileStore("xml"); //$NON-NLS-1$
        final FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(lobBuffer, Streamable.ENCODING);
        try{
            Writer writer = fsisf.getWriter();
            final ExtendedWriter ew = new ExtendedWriter(writer, fsisf);
            translator.translate(ew);
            ew.close();
            success = true;
            return createSQLXML(fsisf, ew, context);
        } catch(IOException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30444, e);
        } catch(TransformerException e) {
             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30445, e);
        } finally {
            if (!success && lobBuffer != null) {
                lobBuffer.remove();
            }
        }
    }

    private static SQLXMLImpl createSQLXML(
            final FileStoreInputStreamFactory fsisf, final ExtendedWriter ew, CommandContext context) {
        if (ew.includes.isEmpty()) {
            if (fsisf.getStorageMode() == StorageMode.MEMORY) {
                //detach if just in memory
                byte[] bytes = fsisf.getMemoryBytes();
                fsisf.free();
                return new SQLXMLImpl(bytes);
            }
            if (context != null) {
                context.addCreatedLob(fsisf);
            }
            return new SQLXMLImpl(fsisf);
        }
        //TODO: allow the detach to happen even if there are includes
        InputStreamFactory isf = new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                List<InputStream> streams = new ArrayList<InputStream>(ew.includes.size() * 2 + 1);
                long last = 0;
                for (int i = 0; i < ew.includes.size(); i++) {
                    Include include = ew.includes.get(i);
                    streams.add(fsisf.getInputStream(last, include.start - last));
                    last = include.start;
                    try {
                        streams.add(((BaseLob)include.streamable.getReference()).getBinaryStream());
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                }
                streams.add(fsisf.getInputStream(last, -1));
                return new SequenceInputStream(Collections.enumeration(streams));
            }

            @Override
            public void free() throws IOException {
                fsisf.free();
                ew.includes.clear();
            }

        };
        if (context != null) {
            context.addCreatedLob(fsisf);
        }
        return new SQLXMLImpl(isf);
    }

    public static Object serialize(XMLSerialize xs, XMLType value) throws TransformationException {
        Type type = value.getType();
        final Charset encoding;
        if (xs.getEncoding() != null) {
            encoding = Charset.forName(xs.getEncoding());
        } else {
            encoding = UTF_8;
        }
        if (Boolean.TRUE.equals(xs.getDeclaration())) {
            //need to replace existing/default declaration
            if (type == Type.ELEMENT || type == Type.DOCUMENT) {
                XMLEventFactory xmlEventFactory = threadLocalEventtFactory.get();
                xmlEventFactory.setLocation(dummyLocation);
                XMLEvent start = null;
                if (xs.getVersion() != null) {
                    start = xmlEventFactory.createStartDocument(encoding.name(), xs.getVersion());
                } else {
                    //use the encoding regardless as different stax impls have different default
                    //behavior
                    start = xmlEventFactory.createStartDocument(encoding.name());
                }
                StAXSourceProvider sourceProvider = new DeclarationStaxSourceProvider(start, value);
                value = new XMLType(new StAXSQLXML(sourceProvider, encoding));
                value.setType(type);
            }
            //else just ignore, since the result is likely invalid
        } else if (type == Type.DOCUMENT && Boolean.FALSE.equals(xs.getDeclaration())){
            final XMLType v = value;
            StAXSourceProvider sourceProvider = new StAXSourceProvider() {
                @Override
                public StAXSource getStaxSource() throws SQLException {
                    try {
                        XMLEventReader eventReader = getXMLEventReader(v.getSource(StAXSource.class));
                        eventReader = XMLType.getXmlInputFactory().createFilteredReader(eventReader, declarationOmittingFilter);
                        return new StAXSource(eventReader);
                    } catch (XMLStreamException e) {
                        throw new SQLException(e);
                    }
                }
            };
            value = new XMLType(new StAXSQLXML(sourceProvider, encoding));
            value.setType(Type.DOCUMENT);
        }
        if (xs.getType() == DataTypeManager.DefaultDataClasses.STRING) {
            return DataTypeManager.transformValue(value, xs.getType());
        }
        if (xs.getType() == DataTypeManager.DefaultDataClasses.CLOB) {
            InputStreamFactory isf = Evaluator.getInputStreamFactory(value);
            return new ClobType(new ClobImpl(isf, -1));
        }
        if (xs.getType() == DataTypeManager.DefaultDataClasses.VARBINARY) {
            try {
                InputStream is = null;
                if (!Charset.forName(value.getEncoding()).equals(encoding)) {
                    is = new ReaderInputStream(value.getCharacterStream(), encoding);
                } else {
                    is = value.getBinaryStream();
                }
                byte[] bytes = ObjectConverterUtil.convertToByteArray(is, DataTypeManager.MAX_VARBINARY_BYTES);
                return new BinaryType(bytes);
            } catch (SQLException e) {
                throw new TransformationException(CorePlugin.Event.TEIID10080, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10080, "XML", "VARBINARY")); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (IOException e) {
                throw new TransformationException(CorePlugin.Event.TEIID10080, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10080, "XML", "VARBINARY")); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        InputStreamFactory isf = null;
        if (!Charset.forName(value.getEncoding()).equals(encoding)) {
            //create a wrapper for the input stream
            isf = new InputStreamFactory.SQLXMLInputStreamFactory(value) {
                public InputStream getInputStream() throws IOException {
                    try {
                        return new ReaderInputStream(sqlxml.getCharacterStream(), encoding);
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                }
            };
        } else {
            isf = Evaluator.getInputStreamFactory(value);
        }
        return new BlobType(new BlobImpl(isf));
    }

    public static XMLOutputFactory getOutputFactory(boolean repairing) {
        if (XMLType.isThreadSafeXmlFactories() && !repairing) {
            return xmlOutputFactory;
        }
        XMLOutputFactory f = threadLocalOutputFactory.get();
        if (repairing && f.isPropertySupported(XMLOutputFactory.IS_REPAIRING_NAMESPACES)) {
            f.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
        } else {
            f.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, false);
        }
        return f;
    }

    @TeiidFunction(category=FunctionCategoryConstants.XML, nullOnNull=true)
    public static XMLType xmlText(String val) throws XMLStreamException, FactoryConfigurationError, IOException, TransformerException {
        //TODO: see if there is a less involved way to escape
        StringWriter writer = new StringWriter();
        XMLEventWriter eventWriter = getOutputFactory().createXMLEventWriter(writer);
        convertValue(writer, eventWriter, threadLocalEventtFactory.get(), val);
        XMLType result = new XMLType(new SQLXMLImpl(writer.toString()));
        result.setType(Type.TEXT);
        return result;
    }

}
