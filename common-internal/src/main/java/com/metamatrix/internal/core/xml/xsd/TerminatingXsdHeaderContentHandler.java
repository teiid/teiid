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

package com.metamatrix.internal.core.xml.xsd;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TerminatingXsdHeaderContentHandler extends DefaultHandler {
    
    public static final String SCHEMA_NOT_FOUND_EXCEPTION_MESSAGE = "SchemaNotFoundException"; //$NON-NLS-1$
    public static final String SCHEMA_FOUND_EXCEPTION_MESSAGE     = "SchemaFoundException"; //$NON-NLS-1$
    
    private static final String SCHEMA_TAG_NAME     = "schema"; //$NON-NLS-1$
    private static final String IMPORT_TAG_NAME     = "import"; //$NON-NLS-1$
    private static final String INCLUDE_TAG_NAME    = "include"; //$NON-NLS-1$

    private static final String TARGET_NAMESPACE_ATTRIBUTE_NAME = "targetNamespace"; //$NON-NLS-1$
    private static final String NAMESPACE_ATTRIBUTE_NAME        = "namespace"; //$NON-NLS-1$
    private static final String SCHEMA_LOCATION_ATTRIBUTE_NAME  = "schemaLocation"; //$NON-NLS-1$

    private static final String ELEMENT_TAG_NAME     = "element"; //$NON-NLS-1$
    private static final String SIMPLE_TYPE_TAG_NAME     = "simpleType"; //$NON-NLS-1$
    private static final String COMPLEX_TYPE_TAG_NAME     = "complexType"; //$NON-NLS-1$
    private static final String GROUP_TAG_NAME     = "group"; //$NON-NLS-1$
    private static final String ATTRIBUTE_GROUP_TAG_NAME     = "attributeGroup"; //$NON-NLS-1$
    private static final String ATTRIBUTE_TAG_NAME     = "attribute"; //$NON-NLS-1$
    private static final String NOTATION_TAG_NAME     = "notation"; //$NON-NLS-1$

    
    
    private boolean foundSchemaStartElement;
    private boolean foundTerminatingTag;
    private XsdHeader header;

    public TerminatingXsdHeaderContentHandler() {
        super();
        this.foundSchemaStartElement = false;
        this.foundTerminatingTag     = false;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /** 
     * @see org.xml.sax.ContentHandler#startPrefixMapping(java.lang.String, java.lang.String)
     */
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        this.getXsdHeader().addNamespaceURI(uri);
        super.startPrefixMapping(prefix, uri);
    }

    /** 
     * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        // Found the schema tag 
        if (SCHEMA_TAG_NAME.equals(localName)) {
            this.processAttributes(atts);
            this.foundSchemaStartElement = true;
        } 
        // Found the schema import tag
        else if (IMPORT_TAG_NAME.equals(localName)) {
            this.processImportAttributes(atts);
        }
        // Found the schema include tag
        else if (INCLUDE_TAG_NAME.equals(localName)) {
            this.processIncludeAttributes(atts);
        }

        else {
            //we can stop processing if encountering any of the folowing tags
            if(ELEMENT_TAG_NAME.equals(localName)
                || SIMPLE_TYPE_TAG_NAME.equals(localName)
                || COMPLEX_TYPE_TAG_NAME.equals(localName)
                || GROUP_TAG_NAME.equals(localName)
                || ATTRIBUTE_GROUP_TAG_NAME.equals(localName)
                || ATTRIBUTE_TAG_NAME.equals(localName)
                || NOTATION_TAG_NAME.equals(localName)) {
                
                this.foundTerminatingTag = true;
            }
        }

        this.checkForCompletion();
        super.startElement(uri, localName, qName, atts);
    }

    /** 
     * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        this.checkForCompletion();
        super.endElement(namespaceURI, localName, qName);
    }
    
    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================
    
    public XsdHeader getXsdHeader() {
        if (this.header == null) {
            this.header = new XsdHeader();
        }
        return this.header;
    }
    
    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================
    
    private void checkForCompletion() throws SAXException {
        if ( !this.foundSchemaStartElement ) {
            throw new SAXException(SCHEMA_NOT_FOUND_EXCEPTION_MESSAGE);
        }
        if (this.foundTerminatingTag) {
            throw new SAXException(SCHEMA_FOUND_EXCEPTION_MESSAGE);
        }
    }
    
    private void processAttributes(final Attributes atts) {
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (TARGET_NAMESPACE_ATTRIBUTE_NAME.equals(name)) {
                this.getXsdHeader().setTargetNamespaceURI(value);
            }
        }
    }
    
    private void processImportAttributes(final Attributes atts) {
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (NAMESPACE_ATTRIBUTE_NAME.equals(name)) {
                this.getXsdHeader().addImportNamespaceURI(value);
            } else if (SCHEMA_LOCATION_ATTRIBUTE_NAME.equals(name)) {
                this.getXsdHeader().addImportSchemaLocation(value);
            }
        }
    }
    
    private void processIncludeAttributes(final Attributes atts) {
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (SCHEMA_LOCATION_ATTRIBUTE_NAME.equals(name)) {
                this.getXsdHeader().addIncludeSchemaLocation(value);
            }
        }
    }

}

