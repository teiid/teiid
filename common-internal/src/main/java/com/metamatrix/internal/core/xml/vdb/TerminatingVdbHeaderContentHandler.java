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

package com.metamatrix.internal.core.xml.vdb;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TerminatingVdbHeaderContentHandler extends DefaultHandler {
    
    public static final String HEADER_FOUND_EXCEPTION_MESSAGE  = "HeaderFoundException"; //$NON-NLS-1$
    public static final String XMI_NOT_FOUND_EXCEPTION_MESSAGE = "XMINotFoundException"; //$NON-NLS-1$
    
    private static final String XMI_TAG_NAME               = "XMI"; //$NON-NLS-1$
    private static final String VIRTUAL_DATABASE_TAG_NAME  = "VirtualDatabase"; //$NON-NLS-1$
    private static final String XMI_VERSION_0020_ATTRIBUTE_NAME = "xmi:version"; //$NON-NLS-1$
    private static final String XMI_VERSION_0011_ATTRIBUTE_NAME = "xmi.version"; //$NON-NLS-1$
    
    private static final String NAME_ATTRIBUTE_NAME        = "name"; //$NON-NLS-1$
    private static final String MODEL_PATH_ATTRIBUTE_NAME  = "path"; //$NON-NLS-1$
    private static final String MODEL_LOC_ATTRIBUTE_NAME   = "modelLocation"; //$NON-NLS-1$
    private static final String UUID_ATTRIBUTE_NAME        = "uuid"; //$NON-NLS-1$
    private static final String DESCRIPTION_ATTRIBUTE_NAME = "description"; //$NON-NLS-1$
    private static final String SEVERITY_ATTRIBUTE_NAME    = "severity"; //$NON-NLS-1$
    private static final String MODEL_TYPE_ATTRIBUTE_NAME  = "modelType"; //$NON-NLS-1$
    private static final String PRIMARY_URI_ATTRIBUTE_NAME = "primaryMetamodelUri"; //$NON-NLS-1$
    private static final String VISIBLE_ATTRIBUTE_NAME     = "visible"; //$NON-NLS-1$
    private static final String CHECKSUM_ATTRIBUTE_NAME    = "checksum"; //$NON-NLS-1$
    private static final String PRODUCER_NAME_ATTRIUBTE_NAME    = "producerName"; //$NON-NLS-1$
    private static final String PRODUCER_VERSION_ATTRIUBTE_NAME = "producerVersion"; //$NON-NLS-1$
    private static final String TIME_CHANGED_ATTRIUBTE_NAME     = "timeLastChanged"; //$NON-NLS-1$
    private static final String TIME_PRODUCED_ATTRIUBTE_NAME    = "timeLastProduced"; //$NON-NLS-1$    
    
    private static final String MODELS_TAG_NAME            = "models"; //$NON-NLS-1$
    private static final String NON_MODELS_TAG_NAME        = "nonModels"; //$NON-NLS-1$
    private static final String MARKERS_TAG_NAME           = "markers"; //$NON-NLS-1$

    private boolean foundXmiStartElement;
    private boolean foundVdbStartElement;
    private boolean foundVdbEndElement;
    private VdbHeader header;

    public TerminatingVdbHeaderContentHandler() {
        super();
        this.foundXmiStartElement        = false;
        this.foundVdbStartElement        = false;
        this.foundVdbEndElement          = false;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /** 
     * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        // Found the XMI tag 
        if (localName.equalsIgnoreCase(XMI_TAG_NAME)) {
            this.processAttributes(atts);
            this.foundXmiStartElement = true;
        } 
        // Found the virtual database element
        else if (localName.equalsIgnoreCase(VIRTUAL_DATABASE_TAG_NAME)) {
            this.processVdbAttributes(atts);
            this.foundVdbStartElement = true;
        }
        // Found the markers element
        else if (localName.equalsIgnoreCase(MARKERS_TAG_NAME)) {
            this.processMarkerAttributes(atts);
        }
        // Found the models element
        else if (localName.equalsIgnoreCase(MODELS_TAG_NAME) && this.foundVdbStartElement) {
            this.processModelRefAttributes(atts);
        }
        // Found the nonModel element
        else if (localName.equalsIgnoreCase(NON_MODELS_TAG_NAME)) {
            this.processNonModelRefAttributes(atts);
        }
        this.checkForCompletion();
        super.startElement(uri, localName, qName, atts);
    }

    /** 
     * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        this.checkForCompletion();
        if (localName.equals(VIRTUAL_DATABASE_TAG_NAME)) {
            this.foundVdbEndElement = true;
        }
        super.endElement(namespaceURI, localName, qName);
    }
    
    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================
    
    public VdbHeader getVdbHeader() {
        if (this.header == null) {
            this.header = new VdbHeader();
        }
        return this.header;
    }
    
    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================
    
    private void checkForCompletion() throws SAXException {
        if ( !this.foundXmiStartElement && !this.foundVdbStartElement) {
            throw new SAXException(XMI_NOT_FOUND_EXCEPTION_MESSAGE);
        }
        if (this.foundVdbStartElement && this.foundVdbEndElement) {
            throw new SAXException(HEADER_FOUND_EXCEPTION_MESSAGE);
        }
    }
    
    private void processAttributes(final Attributes atts) {
        // Check the root for model annotation information
        for (int i = 0; i < atts.getLength(); i++){
            String value = atts.getValue(i);
            String qname = atts.getQName(i);
            if (qname.equalsIgnoreCase(XMI_VERSION_0020_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setXmiVersion(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0011_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setXmiVersion(value);
            }
        }
    }
    
    private void processVdbAttributes(final Attributes atts) {
        // Check the root for model annotation information
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            String qname = atts.getQName(i);
            // The UUID identifier for the VirtualDatabase instance should the same 
            // whether it is the "xmi:uuid" attribute or the "uuid" attribute.
            if (name.equalsIgnoreCase(UUID_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setUUID(value);
            } else if (name.equalsIgnoreCase(NAME_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setName(value);
            } else if (name.equalsIgnoreCase(DESCRIPTION_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setDescription(value);
            } else if (name.equalsIgnoreCase(SEVERITY_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setSeverity(value.toUpperCase());
            } else if (name.equalsIgnoreCase(PRODUCER_NAME_ATTRIUBTE_NAME)) {
                this.getVdbHeader().setProducerName(value);
            } else if (name.equalsIgnoreCase(PRODUCER_VERSION_ATTRIUBTE_NAME)) {
                this.getVdbHeader().setProducerVersion(value);
            } else if (name.equalsIgnoreCase(TIME_CHANGED_ATTRIUBTE_NAME)) {
                this.getVdbHeader().setTimeLastChanged(value);
            } else if (name.equalsIgnoreCase(TIME_PRODUCED_ATTRIUBTE_NAME)) {
                this.getVdbHeader().setTimeLastProduced(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0020_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setXmiVersion(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0011_ATTRIBUTE_NAME)) {
                this.getVdbHeader().setXmiVersion(value);
            }
        }
    }
    
    private void processModelRefAttributes(final Attributes atts) {
        // Check the root for model annotation information
        VdbModelInfo info = new VdbModelInfo();
        for (int i = 0; i < atts.getLength(); i++){
            String qname = atts.getQName(i);
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            // The UUID of the resource being referenced by the ModelReference instance is 
            // the uuid attibute and not the xmi:uuid.  The xmi:uuid attribute is the identifier
            // for the ModelReference instance itself and not the identifier for the referenced
            // model.  For the xmi:uuid attribute the local name is "uuid" but it's qname is "xmi:uuid".
            // For the uuid attribute both the local and qname are the same, "uuid".
            if (name.equalsIgnoreCase(UUID_ATTRIBUTE_NAME) && qname.equalsIgnoreCase(UUID_ATTRIBUTE_NAME)) {
                info.setUUID(value);
            } else if (name.equalsIgnoreCase(NAME_ATTRIBUTE_NAME)) {
                info.setName(value);
            } else if (name.equalsIgnoreCase(MODEL_PATH_ATTRIBUTE_NAME)) {
                info.setPath(value);
            } else if (name.equalsIgnoreCase(MODEL_LOC_ATTRIBUTE_NAME)) {
                info.setLocation(value);
            } else if (name.equalsIgnoreCase(MODEL_TYPE_ATTRIBUTE_NAME)) {
                info.setModelType(value);
            } else if (name.equalsIgnoreCase(PRIMARY_URI_ATTRIBUTE_NAME)) {
                info.setPrimaryMetamodelURI(value);
            } else if (name.equalsIgnoreCase(VISIBLE_ATTRIBUTE_NAME)) {
                info.setVisible(value);
            } else if (name.equalsIgnoreCase(CHECKSUM_ATTRIBUTE_NAME)) {
                info.setCheckSum(value);
            }
        }
        this.getVdbHeader().addModelInfo(info);
    }
    
    private void processNonModelRefAttributes(final Attributes atts) {
        // Check the root for model annotation information
        VdbNonModelInfo info = new VdbNonModelInfo();
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (name.equalsIgnoreCase(NAME_ATTRIBUTE_NAME)) {
                info.setName(value);
            } else if (name.equalsIgnoreCase(MODEL_PATH_ATTRIBUTE_NAME)) {
                info.setPath(value);
            } else if (name.equalsIgnoreCase(CHECKSUM_ATTRIBUTE_NAME)) {
                info.setCheckSum(value);
            }
        }
        this.getVdbHeader().addNonModelInfo(info);
    }
    
    private void processMarkerAttributes(final Attributes atts) {
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (name.equalsIgnoreCase(SEVERITY_ATTRIBUTE_NAME)) {
                Integer markerValue  = (Integer)VdbHeader.severityNameToValueMap.get(value.toUpperCase());
                if (markerValue != null && markerValue.compareTo(this.getVdbHeader().getSeverityValue()) > 0) {
                    this.getVdbHeader().setSeverity(value.toUpperCase());
                }
            }
        }
    }

}

