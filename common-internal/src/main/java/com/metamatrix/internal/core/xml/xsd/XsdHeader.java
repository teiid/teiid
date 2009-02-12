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

import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents the content of an XMI file's header. 
 */
public final class XsdHeader {

    private List<String> namespaceURIs = new ArrayList<String>();
    private List<String> importNamespaceURIs = new ArrayList<String>();
    private List<String> importSchemaLocations = new ArrayList<String>();
    private List<String> includeSchemaLocations = new ArrayList<String>();
    private String targetNamespaceURI;
    
    public String getTargetNamespaceURI() {
        return this.targetNamespaceURI;
    }

    public String[] getNamespaceURIs() {
        return this.namespaceURIs.toArray(new String[this.namespaceURIs.size()]);
    }

    public String[] getImportNamespaces() {
        return this.importNamespaceURIs.toArray(new String[this.importNamespaceURIs.size()]);
    }

    public String[] getImportSchemaLocations() {
        return this.importSchemaLocations.toArray(new String[this.importSchemaLocations.size()]);
    }

    public String[] getIncludeSchemaLocations() {
        return this.includeSchemaLocations.toArray(new String[this.includeSchemaLocations.size()]);
    }
    
    public void addNamespaceURI(final String uri) {
        if (! this.namespaceURIs.contains(uri)) {
            this.namespaceURIs.add( uri );
        }
    }
    
    public void addImportNamespaceURI(final String uri) {
        if (! this.importNamespaceURIs.contains(uri)) {
            this.importNamespaceURIs.add( uri );
        }
    }
    
    public void addImportSchemaLocation(final String location) {
        if (! this.importSchemaLocations.contains(location)) {
            this.importSchemaLocations.add( location );
        }
    }
    
    public void addIncludeSchemaLocation(final String location) {
        if (! this.includeSchemaLocations.contains(location)) {
            this.includeSchemaLocations.add( location );
        }
    }
    
    public void setTargetNamespaceURI(final String uri) {
        this.targetNamespaceURI = uri;
    }
    
    /**
     * Method to print the contents of the XMI Header object.
     * @param stream the stream
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append("Xsd Header:"); //$NON-NLS-1$
        sb.append("\n  targetNamespace:       "); //$NON-NLS-1$
        sb.append(this.getTargetNamespaceURI());
        sb.append("\n  Namespace URIs:"); //$NON-NLS-1$
        String[] nsUris = this.getNamespaceURIs();
        for (int i = 0; i < nsUris.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(nsUris[i]);
        }
        sb.append("\n  Import Namespace URIs:"); //$NON-NLS-1$
        String[] imports = this.getImportNamespaces();
        for (int i = 0; i < imports.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(imports[i]);
        }
        sb.append("\n  Import Schema Locations:"); //$NON-NLS-1$
        String[] importLocs = this.getImportSchemaLocations();
        for (int i = 0; i < importLocs.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(importLocs[i]);
        }
        sb.append("\n  Include Schema Locations:"); //$NON-NLS-1$
        String[] includeLocs = this.getIncludeSchemaLocations();
        for (int i = 0; i < includeLocs.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(includeLocs[i]);
        }
        return sb.toString();
    }
}

