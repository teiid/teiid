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

package org.teiid.xquery.saxon;

import java.io.IOException;
import java.util.Map;

import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;

import net.sf.saxon.Configuration;
import net.sf.saxon.event.ContentHandlerProxy;
import net.sf.saxon.event.FilterFactory;
import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.event.Receiver;
import net.sf.saxon.expr.parser.Location;
import net.sf.saxon.lib.AugmentedSource;
import net.sf.saxon.om.NameChecker;
import net.sf.saxon.om.NamespaceBindingSet;
import net.sf.saxon.om.NodeName;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.SchemaType;
import net.sf.saxon.type.SimpleType;

final class StreamingUtils {
    /**
     * Pre-parser that adds validation and handles a default name space
     *
     * TODO: add support for more general paths including node tests
     *   this could be done as a secondary expression applied to the
     *   context item
     *
     * @param locationPath
     * @param prefixMap
     * @return
     */
    public static String getStreamingPath(String locationPath, Map<String, String> prefixMap) {
        if (locationPath.indexOf("//") >= 0) //$NON-NLS-1$
            throw new IllegalArgumentException("DESCENDANT axis is not supported"); //$NON-NLS-1$

        String path = locationPath.trim();
        if (path.startsWith("/")) path = path.substring(1); //$NON-NLS-1$
        if (path.endsWith("/")) path = path.substring(0, path.length() - 1); //$NON-NLS-1$
        path = path.trim();
        String[] localNames = path.split("/"); //$NON-NLS-1$

        if (localNames.length == 1) {
            throw new IllegalArgumentException(locationPath + " refers to only the root element"); //$NON-NLS-1$
        }

        String fixedPath = ""; //$NON-NLS-1$

        // parse prefix:localName pairs and resolve prefixes to namespaceURIs
        for (int i = 0; i < localNames.length; i++) {
            fixedPath += "/"; //$NON-NLS-1$
            int k = localNames[i].indexOf(':');
            if (k >= 0 && localNames[i].indexOf(':', k+1) >= 0)
                throw new IllegalArgumentException(
                    "QName must not contain more than one colon: " //$NON-NLS-1$
                    + "qname='" + localNames[i] + "', path='" + path + "'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            if (k <= 0) {
                fixedPath += "*:"; //$NON-NLS-1$
            } else {
                String prefix = localNames[i].substring(0, k).trim();
                if (k >= localNames[i].length() - 1)
                    throw new IllegalArgumentException(
                        "Missing localName for prefix: " + "prefix='" //$NON-NLS-1$ //$NON-NLS-2$
                        + prefix + "', path='" + path + "', prefixes=" + prefixMap); //$NON-NLS-1$ //$NON-NLS-2$
                fixedPath += prefix + ":"; //$NON-NLS-1$
            } // end if

            localNames[i] = localNames[i].substring(k + 1).trim();
            if (!localNames[i].equals("*") && !NameChecker.isValidNCName(localNames[i])) { //$NON-NLS-1$
                throw new IllegalArgumentException(localNames[i] + " is not a valid local name."); //$NON-NLS-1$
            }
            fixedPath += localNames[i];
        }
        return fixedPath;
    }

}

/**
 * An {@link XMLReader} designed to bridge between the Saxon document projection logic and the XOM/NUX streaming logic.
 */
final class SaxonReader implements XMLReader {

    private ContentHandler handler;
    private LexicalHandler lexicalHandler;

    private Configuration config;
    private AugmentedSource source;

    public SaxonReader(Configuration config, AugmentedSource source) {
        this.config = config;
        this.source = source;
    }

    @Override
    public void setProperty(String name, Object value)
            throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) { //$NON-NLS-1$
            this.lexicalHandler = (LexicalHandler) value;
        }
    }

    @Override
    public void setFeature(String name, boolean value)
            throws SAXNotRecognizedException, SAXNotSupportedException {
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void setDTDHandler(DTDHandler handler) {

    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        this.handler = handler;
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        final ContentHandlerProxy chp = new ContentHandlerProxy();
        chp.setLexicalHandler(lexicalHandler);
        chp.setUnderlyingContentHandler(handler);
        this.source.addFilter(new FilterFactory() {

            @Override
            public ProxyReceiver makeFilter(Receiver arg0) {
                return new ContentHandlerProxyReceiver(chp, arg0);
            }
        });
        try {
            config.buildDocumentTree(source);
        } catch (XPathException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException,
            SAXNotSupportedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException,
            SAXNotSupportedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return null;
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DTDHandler getDTDHandler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ContentHandler getContentHandler() {
        return this.handler;
    }

}

/**
 * Adapts the {@link ContentHandlerProxy} to be a {@link ProxyReceiver}
 */
final class ContentHandlerProxyReceiver extends ProxyReceiver {

    private Receiver reciever;

    public ContentHandlerProxyReceiver(Receiver receiver, Receiver next) {
        super(next);
        this.reciever = receiver;
        this.reciever.setPipelineConfiguration(next.getPipelineConfiguration());
    }

    @Override
    public void attribute(NodeName nameCode, SimpleType typeCode,
            CharSequence value, Location locationId, int properties)
            throws XPathException {
        reciever.attribute(nameCode, typeCode, value, locationId,
                properties);
    }

    public void characters(CharSequence chars, Location locationId,
            int properties) throws XPathException {
        reciever.characters(chars, locationId, properties);
    }

    public void close() throws XPathException {
        reciever.close();
        super.close();
    }

    public void comment(CharSequence content, Location locationId, int properties)
            throws XPathException {
        reciever.comment(content, locationId, properties);
    }

    public void endDocument() throws XPathException {
        reciever.endDocument();
        super.endDocument() ;
    }

    public void endElement() throws XPathException {
        reciever.endElement();
    }

    public String getSystemId() {
        return reciever.getSystemId();
    }

    @Override
    public void namespace(NamespaceBindingSet namespaceBindings, int properties)
            throws XPathException {
        reciever.namespace(namespaceBindings, properties);
    }

    public void open() throws XPathException {
        super.open();
        reciever.open();
    }

    public void processingInstruction(String name, CharSequence data,
            Location locationId, int properties) throws XPathException {
        reciever.processingInstruction(name, data, locationId, properties);
    }

    public void setSystemId(String systemId) {
        reciever.setSystemId(systemId);
        super.setSystemId(systemId);
    }

    public void setUnparsedEntity(String name, String systemID,
            String publicID) throws XPathException {
        reciever.setUnparsedEntity(name, systemID, publicID);
    }

    public void startContent() throws XPathException {
        reciever.startContent();
    }

    public void startDocument(int properties) throws XPathException {
        super.startDocument(properties);
        reciever.startDocument(properties);
    }

    @Override
    public void startElement(NodeName elemName, SchemaType typeCode,
            Location locationId, int properties) throws XPathException {
        reciever.startElement(elemName, typeCode, locationId, properties);
    }

}