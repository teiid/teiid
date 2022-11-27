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
