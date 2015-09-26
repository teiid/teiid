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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;

/**
 * TODO: need to back this up with something like MapDB, to avoid OOM
 * Also need to write our own JSONPaser that returns this object directly.
 */
public class ODataResponseDocument {
    private String name;
    private Map<String, Object> properties;
    private List<List<ODataResponseDocument>> children;
    private ODataResponseDocument parent;
    
    public ODataResponseDocument() {
    }
    
    ODataResponseDocument(String name, ODataResponseDocument parent) {
        this.name = name;
        this.parent = parent;
    }    
    
    public static List<Map<String, Object>> crossjoinWith(
            List<Map<String, Object>> left, List<ODataResponseDocument> rightDocuments) {

        ArrayList<Map<String, Object>> joined = new ArrayList<Map<String,Object>>();
        for (ODataResponseDocument right : rightDocuments) {
            List<Map<String,Object>> rightRows = right.flatten();            
            for (Map<String, Object> outer : left) {
                for (Map<String, Object> inner : rightRows) {
                    LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
                    row.putAll(outer);
                    row.putAll(inner);
                    joined.add(row);
                }
            }
        }
        return joined;
    }
    
    public List<Map<String, Object>> flatten(){
        List<Map<String, Object>> joined = new ArrayList<Map<String, Object>>();

        LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
        row.putAll(this.properties);
        joined.add(row);            
        
        if (this.children != null && !this.children.isEmpty()) {
            for (List<ODataResponseDocument> childDoc:this.children) {
                joined = crossjoinWith(joined, childDoc);
            }
        }
        return joined;
    }

    private static String name(String s1, String s2) {
        if (s1 != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(s1).append("/").append(s2);
            return sb.toString();
        } else {
            return s2;
        }
    }
    
    public String getName() {
        if (this.parent != null) {
            return name(this.parent.getName(), this.name);
        }
        return name;
    }

    public void addProperty(String key, Object value) {
        if (this.properties == null) {
            this.properties = new LinkedHashMap<String, Object>();
        }
        if (this.parent == null) {
            this.properties.put(key, value);
        } else {
            this.properties.put(name(getName(), key), value);
        }
    }

    public void addChildDocuments(List<ODataResponseDocument> child) {
        if (this.children == null) {
            this.children = new ArrayList<List<ODataResponseDocument>>();
        }
        this.children.add(child);
    }
    
    
    public static ODataResponseDocument createDocument(Entity entity) {
        ODataResponseDocument document = new ODataResponseDocument();
        List<Property> properties = entity.getProperties();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }
    
    public static ODataResponseDocument createDocument(ComplexValue complex) {
        ODataResponseDocument document = new ODataResponseDocument();
        List<Property> properties = complex.getValue();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }    
    
    private static ODataResponseDocument createDocument(String name,
            ComplexValue complex, ODataResponseDocument parent) {
        ODataResponseDocument document = new ODataResponseDocument(name, parent);
        List<Property> properties = complex.getValue();
        for (Property property : properties) {
            populateDocument(property, document);
        }
        return document;
    }
    
    @SuppressWarnings("unchecked")
    private static void populateDocument(Property property, ODataResponseDocument document) {
        if (property.isCollection()) {
            if (property.isPrimitive()) {
                document.addProperty(property.getName(), property.asCollection());
            } else {
                List<ComplexValue> complexRows = (List<ComplexValue>)property.asCollection();
                ArrayList<ODataResponseDocument> rows = new ArrayList<ODataResponseDocument>();
                for (ComplexValue complexRow : complexRows) {
                    rows.add(createDocument(property.getName(), complexRow, document));
                }
                document.addChildDocuments(rows);
            }
        } else {
            if (property.isPrimitive()) {
                document.addProperty(property.getName(), property.asPrimitive());
            } else {
                document.addChildDocuments(Arrays.asList(createDocument(
                        property.getName(), property.asComplex(), document)));
            }
        }
    }    
}
