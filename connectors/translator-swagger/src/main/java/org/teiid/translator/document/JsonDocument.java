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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class JsonDocument {
    
    private String name;
    private Map<String, Object> properties;
    private Map<String, List<JsonDocument>> children;
    private JsonDocument parent;
    
    public JsonDocument(String name, JsonDocument parent){
        this.name = name;
        this.parent = parent;
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
    
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void addChildDocuments(String key, JsonDocument document) {
        if (this.children == null){
            List<JsonDocument> value = new LinkedList<JsonDocument>();
            this.children = new LinkedHashMap<String,  List<JsonDocument>>();
            children.put(key, value);
        }
        
        List<JsonDocument> list = children.get(key);
        list.add(document);
        
    }
    
    public String getName() {
        if (this.parent != null) {
            return name(this.parent.getName(), this.name);
        }
        return name;
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
    
    public static JsonDocument createDocument(String name, JsonDocument parent, Map<String, Object> results) {
        
        JsonDocument document = new JsonDocument(name, parent);
        for(Entry<String, Object> entry : results.entrySet()){
            String key = entry.getKey();
            Object value = entry.getValue();
            if(value instanceof Map){
                Map<String, Object> subMap = (Map<String, Object>) value;
                document.addChildDocuments(key, createDocument(key, document, subMap));
            } else {
                document.addProperty(key, value);
            }
        }
        
        return document;
    }

}
