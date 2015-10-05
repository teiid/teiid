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
package org.teiid.translator.document;

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
public class ODataDocument {
    private String name;
    private Map<String, Object> properties;
    private Map<String, List<ODataDocument>> children;
    private ODataDocument parent;
    
    public ODataDocument() {
    }
    
    ODataDocument(String name, ODataDocument parent) {
        this.name = name;
        this.parent = parent;
    }    
    
    static List<Map<String, Object>> crossjoinWith(
            List<Map<String, Object>> left, List<ODataDocument> rightDocuments) {
        ArrayList<Map<String, Object>> joined = new ArrayList<Map<String,Object>>();
        for (ODataDocument right : rightDocuments) {
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
        if (this.properties != null) {
            row.putAll(this.properties);
        }
        joined.add(row);            
        if (this.children != null && !this.children.isEmpty()) {
            for (List<ODataDocument> childDoc:this.children.values()) {
                joined = crossjoinWith(joined, childDoc);
            }
        }
        return joined;
    }
    
    Map<String, Object> getProperties(){
        return this.properties;
    }
    
    List<ODataDocument> getChildDocuments(String path) {
        if (this.children != null) {
            int index = path.indexOf('/');
            if (index != -1) {
                String parentName = path.substring(0, index);
                if (parentName.equals(this.name)) {                    
                    return this.children.get(path.substring(index+1));    
                } else {
                    // then this is the sibiling
                    return this.parent.getChildDocuments(parentName);
                }
            }
            List<ODataDocument> children =  this.children.get(path);
            if (children == null && this.parent != null) {
                children = this.parent.getChildDocuments(path);
            }
            return children;
        }
        return null;
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

    public void addChildDocuments(String path, List<ODataDocument> child) {
        if (this.children == null) {
            this.children = new LinkedHashMap<String, List<ODataDocument>>();
        }
        this.children.put(path, child);
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
                ArrayList<ODataDocument> rows = new ArrayList<ODataDocument>();
                for (ComplexValue complexRow : complexRows) {
                    rows.add(createDocument(property.getName(), complexRow, document));
                }
                document.addChildDocuments(property.getName(), rows);
            }
        } else {
            if (property.isPrimitive()) {
                document.addProperty(property.getName(), property.asPrimitive());
            } else {
                document.addChildDocuments(property.getName(), Arrays.asList(createDocument(
                        property.getName(), property.asComplex(), document)));
            }
        }
    }
    
    public String toString() {
        return this.name;
    }
}
