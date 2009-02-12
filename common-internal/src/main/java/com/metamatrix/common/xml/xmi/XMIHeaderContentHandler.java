/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.xml.xmi;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.xml.sax.*;

import com.metamatrix.api.core.xmi.XMIConstants;
import com.metamatrix.api.core.xmi.XMIHeader;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public class XMIHeaderContentHandler implements ContentHandler {
    private XMIHeader header;
    private String currentTagName;
    private boolean foundXMIStartElement;
    private boolean	foundHeaderStartElement;
    private boolean	foundHeaderEndElement;

    public XMIHeaderContentHandler() throws IOException {
        this.header = new XMIHeader();
        this.currentTagName          = null;
        this.foundHeaderStartElement = false;
        this.foundHeaderEndElement   = false;
        this.foundXMIStartElement    = false;
    }

	// ########################## ContentHandler Methods ###################################

    /**
     * Provide reference to <code>Locator</code> which provides
     * information about where in a document callbacks occur.
     * @param locator <code>Locator</code> object tied to callback process
     */
    public void setDocumentLocator(Locator locator) {
        //System.out.println("    * setDocumentLocator() called");
    }

    /**
     * This indicates the start of a Document parse - this precedes
     * all callbacks in all SAX Handlers with the sole exception
     * of <code>{@link #setDocumentLocator}</code>.
     * @throws <code>SAXException</code> when things go wrong
     */
    public void startDocument() throws SAXException {
        //System.out.println("Parsing begins...");
    }

    /**
     * This indicates the end of a Document parse - this occurs after
     * all callbacks in all SAX Handlers.</code>.
     * @throws <code>SAXException</code> when things go wrong
     */
    public void endDocument() throws SAXException {
        //System.out.println("...Parsing ends.");
    }

    /**
     * This will indicate that a processing instruction (other than
     * the XML declaration) has been encountered.
     * @param target <code>String</code> target of PI
     * @param data <code>String</code containing all data sent to the PI.
     * This typically looks like one or more attribute value pairs.
     * @throws SAXException when things go wrong
     */
    public void processingInstruction(String target, String data) throws SAXException {
        //System.out.println("PI: Target:" + target + " and Data:" + data);
    }

    /**
     * This will add the prefix mapping to the <code>XMIHeader</code> object.
     * @param prefix <code>String</code> namespace prefix.
     * @param uri <code>String</code> namespace URI.
     */
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        //System.out.println("Mapping starts for prefix " + prefix + " mapped to URI " + uri);
// TODO add namespace information to header?
//       this.header.addNamespace(prefix, uri);
    }

    /**
     * <p>
     * This will add the prefix mapping to the <code>XMIHeader</code> object.
     * @param prefix <code>String</code> namespace prefix.
     * @param uri <code>String</code> namespace URI.
     */
    public void endPrefixMapping(String prefix) throws SAXException {
        //System.out.println("Mapping ends for prefix " + prefix);
    }

    /**
     * This reports the occurrence of an actual element.  It will include
     *   the element's attributes, with the exception of XML vocabulary
     *   specific attributes, such as
     *   <code>xmlns:[namespace prefix]</code> and
     *   <code>xsi:schemaLocation</code>.
     * @param uri <code>String</code> namespace URI this element
     * is associated with, or an empty <code>String</code>
     * @param localName <code>String</code> name of element (with no
     * namespace prefix, if one is present)
     * @param qName <code>String</code> XML 1.0 version of element name:
     * [namespace prefix]:[localName]
     * @param atts <code>Attributes</code> list for this element
     * @throws SAXException when things go wrong
     */
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        //System.out.println("startElement: " + localName);
        this.currentTagName = localName;
        if (localName.equals(XMIConstants.ElementName.XMI)) {
            this.foundXMIStartElement = true;
        } else if (localName.equals(XMIConstants.ElementName.HEADER)) {
            this.foundHeaderStartElement = true;
        }
        this.checkHeader();
        this.processStartElement(localName,atts);
    }

    /**
     * Capture ignorable whitespace as text.  If setIgnoringElementContentWhitespace(true)
     * has been called then this method does nothing.
     * @param ch <code>[]</code> - char array of ignorable whitespace
     * @param start <code>int</code> - starting position within array
     * @param length <code>int</code> - length of whitespace after start
     * @throws SAXException when things go wrong
     */
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        //System.out.println("ignorableWhitespace: \"" + s + "\"");
        this.checkHeader();
        String data = new String(ch, start, length);
        this.processElementContent(data.trim());
    }
    /**
     * This will report character data (within an element).
     * @param ch <code>char[]</code> character array with character data
     * @param start <code>int</code> index in array where data starts.
     * @param length <code>int</code> length of data.
     * @throws SAXException when things go wrong
     */
    public void characters(char[] ch, int start, int length) throws SAXException {
        //System.out.println("characters: start,end " + start + " " + length);
        this.checkHeader();
        String data = new String(ch, start, length);
        this.processElementContent(data.trim());
    }

    /**
     * Indicates the end of an element (<code>&lt;/[element name]&gt;</code>)
     * is reached.  Note that the parser does not distinguish between empty
     * elements and non-empty elements, so this will occur uniformly.
     * @param namespaceURI <code>String</code> URI of namespace this element is associated with
     * @param localName <code>String</code> name of element without prefix
     * @param qName <code>String</code> name of element in XML 1.0 form
     * @throws SAXException when things go wrong
     */
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        //System.out.println("endElement: " + localName + "\n");
        this.checkHeader();
        if (localName.equals(XMIConstants.ElementName.HEADER)) {
            this.foundHeaderEndElement = true;
        }
        this.currentTagName = null;
    }

	/**
     * This will report an entity that is skipped by the parser.  This should
     * only occur for non-validating parsers, and then is still implementation-dependent behavior.
     * @param name <code>String</code> name of entity being skipped
     * @throws <code>SAXException</code> when things go wrong
     */
    public void skippedEntity(String name) throws SAXException {
        //System.out.println("Skipping entity " + name);
    }

	// ########################## XMIHeaderContentHandler Methods ###################################

    /**
     * Return the XMI Header for this content handler.
     */
    public XMIHeader getHeader() {
        return this.header;
    }

    /**
     * This method is called after the end tag for the XMI header is encountered,
     * and allows the subclass to provide specific behavior if necessary.
     * This implementation does nothing by default.
     */
    public void complete() throws SAXException {
    }

    protected void checkHeader() throws SAXException {
        if ( !this.foundXMIStartElement ) {
            throw new XMINotFoundException();
        }
        if (this.foundHeaderStartElement && this.foundHeaderEndElement) {
            this.complete();
            //throw new HeaderFoundException();
        }
    }

    private void processStartElement(String tagName, Attributes atts) {
        //System.out.println("processStartElement: " + tagName);

// TODO add version/timestamp information to header?
//        if (tagName.equals(XMIConstants.ElementName.XMI)) {
//            String attName = null;
//            for (int i=0, len=atts.getLength(); i<len; i++) {
//                attName = atts.getLocalName(i);
//                if (attName.equals(XMIConstants.AttributeName.XMI_VERSION)) {
//                    this.header.setXMIVersion(atts.getValue(i));
//                } else if (attName.equals(XMIConstants.AttributeName.TIMESTAMP)) {
//                    this.header.setTimestamp(atts.getValue(i));
//                }
//            }
//        }

        if (tagName.equals(XMIConstants.ElementName.MODEL)) {
            HeaderEntryInfo info = new HeaderEntryInfo(atts);
            this.header.addModel(info.getName(),info.getVersion(),info.getHref());
        } else if (tagName.equals(XMIConstants.ElementName.METAMODEL)) {
            HeaderEntryInfo info = new HeaderEntryInfo(atts);
            this.header.addMetaModel(info.getName(),info.getVersion(),info.getHref());
        } else if (tagName.equals(XMIConstants.ElementName.METAMETAMODEL)) {
            HeaderEntryInfo info = new HeaderEntryInfo(atts);
            this.header.addMetaMetaModel(info.getName(),info.getVersion(),info.getHref());
        } else if (tagName.equals(XMIConstants.ElementName.IMPORT)) {
            HeaderEntryInfo info = new HeaderEntryInfo(atts);
            this.header.addImport(info.getName(),info.getVersion(),info.getHref());
        }
    }

    private void processElementContent(String content) {
        //System.out.println("processElementContent: " + content + " for current tag " + this.currentTagName);
        if (this.currentTagName == null || content == null) {
            return;
        }
        if (this.currentTagName.equals(XMIConstants.ElementName.OWNER)) {
            this.header.getDocumentation().setOwner(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.CONTACT)) {
            this.header.getDocumentation().setContact(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.LONG_DESCRIPTION)) {
            this.header.getDocumentation().setLongDescription(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.SHORT_DESCRIPTION)) {
            this.header.getDocumentation().setShortDescription(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.EXPORTER)) {
            this.header.getDocumentation().setExporter(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.EXPORTER_VERSION)) {
            this.header.getDocumentation().setExporterVersion(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.EXPORTER_ID)) {
            this.header.getDocumentation().setExporterID(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.NOTICE)) {
            this.header.getDocumentation().setNotice(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.MODEL)) {
            XMIHeader.Model model = (XMIHeader.Model) this.getLastElement( this.header.getModels() );
            model.addContent(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.METAMODEL)) {
            XMIHeader.Model model = (XMIHeader.Model) this.getLastElement( this.header.getMetaModels() );
            model.addContent(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.METAMETAMODEL)) {
            XMIHeader.Model model = (XMIHeader.Model) this.getLastElement( this.header.getMetaMetaModels() );
            model.addContent(content);
        } else if (this.currentTagName.equals(XMIConstants.ElementName.IMPORT)) {
            XMIHeader.Import imp = (XMIHeader.Import) this.getLastElement( this.header.getImports() );
            imp.addContent(content);
        }
    }

    private Object getLastElement(Collection elements) {
        Object lastElement = null;
        Iterator itr = elements.iterator();
        while (itr.hasNext()) {
            lastElement = itr.next();
        }
        return lastElement;
    }

    /**
     * Subclass of SAXException used to indicate that the </XMI> element was
     * not found and to halt further processing of the document. This exception should
     * never be visible to the user.
     */
    private static class XMINotFoundException extends SAXException {
        XMINotFoundException() {
        	super(CommonPlugin.Util.getString(ErrorMessageKeys.XML_ERR_0008));
        }
    }

    /**
     * Private inner class used to bundle the set of attributes representing
     * a XMI header model, metamodel, metametamodel, or import tag.
     */
    private static class HeaderEntryInfo {
        private String href;
        private String name;
        private String version;

        public HeaderEntryInfo( Attributes atts ) {
            String attName = null;
            for (int i=0, len=atts.getLength(); i<len; i++) {
                attName = atts.getLocalName(i);
                if (attName.equals(XMIConstants.AttributeName.XMI_NAME)) {
                    this.name = atts.getValue(i);
                } else if (attName.equals(XMIConstants.AttributeName.XMI_VERSION)) {
                    this.version = atts.getValue(i);
                } else if (attName.equals(XMIConstants.AttributeName.HREF)) {
                    this.href = atts.getValue(i);
                }
            }
        }
        public String getName() {
            return this.name;
        }
        public String getVersion() {
            return this.version;
        }
        public String getHref() {
            return this.href;
        }
    }

}

