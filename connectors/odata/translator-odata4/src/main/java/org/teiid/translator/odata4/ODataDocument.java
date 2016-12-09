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
package org.teiid.translator.odata4;

import java.util.List;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;

public class ODataDocument extends org.teiid.translator.document.Document {
    
    ODataDocument(String name, ODataDocument parent) {
        super(name, false, parent);
    }     

    public ODataDocument() {
    }     
    
    public static ODataDocument createDocument(Entity entity) {
        ODataDocument document = new ODataDocument();
        List<Property> properties = entity.getProperties();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }
    
    public static ODataDocument createDocument(ComplexValue complex) {
        ODataDocument document = new ODataDocument();
        List<Property> properties = complex.getValue();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }    
    
    private static ODataDocument createDocument(String name,
            ComplexValue complex, ODataDocument parent) {
        ODataDocument document = new ODataDocument(name, parent);
        List<Property> properties = complex.getValue();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }
    
    @SuppressWarnings("unchecked")
    private static void populateDocument(Property property, ODataDocument document) {
        if (property.isCollection()) {
            if (property.isPrimitive()) {
                document.addProperty(property.getName(), property.asCollection());
            } else {
                List<ComplexValue> complexRows = (List<ComplexValue>)property.asCollection();
                for (ComplexValue complexRow : complexRows) {
                    document.addChildDocument(property.getName(), createDocument(property.getName(), complexRow, document));
                }
            }
        } else {
            if (property.isPrimitive()) {
                document.addProperty(property.getName(), property.asPrimitive());
            } else if (property.isComplex()) {
                document.addChildDocument(property.getName(), createDocument(
                        property.getName(), property.asComplex(), document));
            } else if (property.isGeospatial()) {
                document.addProperty(property.getName(), property.asGeospatial());
            } else {
                throw new AssertionError(property.getType() + " not supported"); //$NON-NLS-1$
            }
        }
    }    
}
