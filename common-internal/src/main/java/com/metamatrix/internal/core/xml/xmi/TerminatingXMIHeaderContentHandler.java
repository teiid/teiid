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

package com.metamatrix.internal.core.xml.xmi;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TerminatingXMIHeaderContentHandler extends DefaultHandler {
    
    public static final String HEADER_FOUND_EXCEPTION_MESSAGE  = "HeaderFoundException"; //$NON-NLS-1$
    public static final String XMI_NOT_FOUND_EXCEPTION_MESSAGE = "XMINotFoundException"; //$NON-NLS-1$
    
    private static final String XMI_TAG_NAME               = "XMI"; //$NON-NLS-1$
    private static final String VIRTUAL_DATABASE_TAG_NAME  = "VirtualDatabase"; //$NON-NLS-1$
    private static final String MODEL_ANNOTATION_TAG_NAME  = "ModelAnnotation"; //$NON-NLS-1$
    private static final String XMI_VERSION_0020_ATTRIBUTE_NAME = "xmi:version"; //$NON-NLS-1$
    private static final String XMI_VERSION_0011_ATTRIBUTE_NAME = "xmi.version"; //$NON-NLS-1$
    private static final String PRODUCER_NAME_ATTRIBUTE_NAME    = "ProducerName"; //$NON-NLS-1$
    private static final String PRODUCER_VERSION_ATTRIBUTE_NAME = "ProducerVersion"; //$NON-NLS-1$
    private static final String UUID_ATTRIBUTE_NAME        = "uuid"; //$NON-NLS-1$
    private static final String DESCRIPTION_ATTRIBUTE_NAME = "description"; //$NON-NLS-1$
    private static final String PRIMARY_URI_ATTRIBUTE_NAME = "primaryMetamodelUri"; //$NON-NLS-1$
    private static final String MODEL_TYPE_ATTRIBUTE_NAME  = "modelType"; //$NON-NLS-1$
    private static final String VISIBLE_ATTRIBUTE_NAME     = "visible"; //$NON-NLS-1$
    private static final String MODEL_NAMESPACE_URI        = "namespaceURI"; //$NON-NLS-1$

    private static final String MODEL_IMPORT_TAG_NAME      = "modelImports"; //$NON-NLS-1$
    private static final String MODELS_TAG_NAME            = "models"; //$NON-NLS-1$
    private static final String IMPORT_PATH_ATTRIBUTE_NAME = "path"; //$NON-NLS-1$
    private static final String IMPORT_LOC_ATTRIBUTE_NAME  = "modelLocation"; //$NON-NLS-1$
    private static final String IMPORT_NAME_ATTRIBUTE_NAME = "name"; //$NON-NLS-1$
    private static final String IMPORT_TYPE_ATTRIBUTE_NAME = "modelType"; //$NON-NLS-1$
    private static final String IMPORT_UUID_ATTRIBUTE_NAME = "uuid"; //$NON-NLS-1$
    private static final String IMPORT_PRIMARY_METAMODEL_URI_ATTRIBUTE_NAME = "primaryMetamodelUri"; //$NON-NLS-1$

    private boolean foundXmiStartElement;
    private boolean foundAnnotationStartElement;
    private boolean foundAnnotationEndElement;
    private boolean foundVdbStartElement;
    private boolean foundVdbEndElement;
    private XMIHeader header;

    public TerminatingXMIHeaderContentHandler() {
        super();
        this.foundXmiStartElement        = false;
        this.foundAnnotationStartElement = false;
        this.foundAnnotationEndElement   = false;
        this.foundVdbStartElement        = false;
        this.foundVdbEndElement          = false;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /** 
     * @see org.xml.sax.ContentHandler#startPrefixMapping(java.lang.String, java.lang.String)
     */
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        this.getXmiHeader().addNamespaceURI(uri);
        super.startPrefixMapping(prefix, uri);
    }

    /** 
     * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        // Found the XMI tag 
        if (localName.equalsIgnoreCase(XMI_TAG_NAME)) {
            this.processAttributes(atts);
            this.foundXmiStartElement = true;
        } 
        // Found the model annotation element
        else if (localName.equalsIgnoreCase(MODEL_ANNOTATION_TAG_NAME)) {
            this.processAttributes(atts);
            this.foundAnnotationStartElement = true;
        }
        // Found the virtual database element
        else if (localName.equalsIgnoreCase(VIRTUAL_DATABASE_TAG_NAME)) {
            this.processVdbAttributes(atts);
            this.foundAnnotationStartElement = true;
            this.foundVdbStartElement = true;
        }
        // Found the models element
        else if (localName.equalsIgnoreCase(MODELS_TAG_NAME) && this.foundVdbStartElement) {
            this.processImportAttributes(atts);
        }
        // Found the model import element
        else if (localName.equalsIgnoreCase(MODEL_IMPORT_TAG_NAME)) {
            this.processImportAttributes(atts);
        }
        this.checkForCompletion();
        super.startElement(uri, localName, qName, atts);
    }

    /** 
     * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        this.checkForCompletion();
        if (localName.equals(MODEL_ANNOTATION_TAG_NAME)) {
            this.foundAnnotationEndElement = true;
        }
        else if (localName.equals(VIRTUAL_DATABASE_TAG_NAME)) {
            this.foundAnnotationEndElement = true;
            this.foundVdbEndElement = true;
        }
        super.endElement(namespaceURI, localName, qName);
    }
    
    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================
    
    public XMIHeader getXmiHeader() {
        if (this.header == null) {
            this.header = new XMIHeader();
        }
        return this.header;
    }
    
    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================
    
    private void checkForCompletion() throws SAXException {
        if ( !this.foundXmiStartElement && !this.foundAnnotationStartElement && !this.foundVdbStartElement) {
            throw new SAXException(XMI_NOT_FOUND_EXCEPTION_MESSAGE);
        }
        if (this.foundAnnotationStartElement && this.foundAnnotationEndElement && !this.foundVdbStartElement) {
            throw new SAXException(HEADER_FOUND_EXCEPTION_MESSAGE);
        }
        if (this.foundVdbStartElement && this.foundVdbEndElement) {
            throw new SAXException(HEADER_FOUND_EXCEPTION_MESSAGE);
        }
    }
    
    private void processAttributes(final Attributes atts) {
        // Check the root for model annotation information
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            String qname = atts.getQName(i);
            // The UUID associated with the model is the ModelAnnotation UUID
            // unless this is a VDB in which case it is the VirtualDatabase UUID
            if (name.equalsIgnoreCase(UUID_ATTRIBUTE_NAME) && !this.foundVdbStartElement) {
                this.getXmiHeader().setUUID(value);
            } else if (name.equalsIgnoreCase(DESCRIPTION_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setDescription(value);
            } else if (name.equalsIgnoreCase(PRODUCER_NAME_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setProducerName(value);
            } else if (name.equalsIgnoreCase(PRODUCER_VERSION_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setProducerVersion(value);
            } else if (name.equalsIgnoreCase(PRIMARY_URI_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setPrimaryMetamodelURI(value);
            } else if (name.equalsIgnoreCase(MODEL_TYPE_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setModelType(value);
            } else if (name.equalsIgnoreCase(MODEL_NAMESPACE_URI)) {
                this.getXmiHeader().setModelNamespaceUri(value);
            } else if (name.equalsIgnoreCase(VISIBLE_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setVisible(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0020_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setXmiVersion(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0011_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setXmiVersion(value);
            }
        }
    }
    
    private void processVdbAttributes(final Attributes atts) {
        // Check the root for model annotation information
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            String qname = atts.getQName(i);
            // The UUID associated with the model is the ModelAnnotation UUID
            // unless this is a VDB in which case it is the VirtualDatabase UUID
            if (name.equalsIgnoreCase(UUID_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setUUID(value);
            } else if (qname.equalsIgnoreCase(XMI_VERSION_0020_ATTRIBUTE_NAME)) {
                this.getXmiHeader().setXmiVersion(value);
            }
        }
    }
    
    private void processImportAttributes(final Attributes atts) {
        // Check the root for model annotation information
        ModelImportInfo info = new ModelImportInfo();
        for (int i = 0; i < atts.getLength(); i++){
            String name  = atts.getLocalName(i);
            String value = atts.getValue(i);
            if (name.equalsIgnoreCase(IMPORT_PATH_ATTRIBUTE_NAME)) {
                info.setPath(value);
            } else if (name.equalsIgnoreCase(IMPORT_LOC_ATTRIBUTE_NAME)) {
                info.setLocation(value);
            } else if (name.equalsIgnoreCase(IMPORT_UUID_ATTRIBUTE_NAME)) {
                info.setUUID(value);
            } else if (name.equalsIgnoreCase(IMPORT_NAME_ATTRIBUTE_NAME)) {
                info.setName(value);
            } else if (name.equalsIgnoreCase(IMPORT_TYPE_ATTRIBUTE_NAME)) {
                info.setModelType(value);
            } else if (name.equalsIgnoreCase(IMPORT_PRIMARY_METAMODEL_URI_ATTRIBUTE_NAME)) {
                info.setPrimaryMetamodelURI(value);
            }
        }
        this.getXmiHeader().addModelImportInfo(info);
    }
//    // Check the root for model annotation information
//    VdbModelInfo info = new VdbModelInfo();
//    for (int i = 0; i < atts.getLength(); i++){
//        String name  = atts.getLocalName(i);
//        String value = atts.getValue(i);
//        if (name.equalsIgnoreCase(UUID_ATTRIBUTE_NAME)) {
//            info.setUUID(value);
//        } else if (name.equalsIgnoreCase(NAME_ATTRIBUTE_NAME)) {
//            info.setName(value);
//        } else if (name.equalsIgnoreCase(MODEL_PATH_ATTRIBUTE_NAME)) {
//            info.setPath(value);
//        } else if (name.equalsIgnoreCase(MODEL_TYPE_ATTRIBUTE_NAME)) {
//            info.setModelType(value);
//        } else if (name.equalsIgnoreCase(PRIMARY_URI_ATTRIBUTE_NAME)) {
//            info.setPrimaryMetamodelURI(value);
//        } else if (name.equalsIgnoreCase(VISIBLE_ATTRIBUTE_NAME)) {
//            info.setVisible(value);
//        }
//    }
//    this.getVdbHeader().addModelInfo(info);

}

