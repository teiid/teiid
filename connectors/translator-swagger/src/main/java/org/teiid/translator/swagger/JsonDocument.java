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
package org.teiid.translator.swagger;

import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getPathSeparator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class JsonDocument {
    
    private String name;
    private Map<String, Object> properties;
    
    public JsonDocument(String name, Map<String, Object> results){
        this.name = name;
        this.properties = new LinkedHashMap<String, Object>();
        visit(results);
    }
    
    @SuppressWarnings("unchecked")
    private void visit(Map<String, Object> results) {
        for(Entry<String, Object> entry : results.entrySet()){
            String key = entry.getKey();
            Object value = entry.getValue();
            if(value instanceof Map) {
                Map<String, Object> subMap = (Map<String, Object>) value;
                JsonDocument doc = new JsonDocument(this.getName() == null ? key : this.getName() + getPathSeparator() + key, subMap);
                this.properties.putAll(doc.getProperties());
            } else {
                this.properties.put(this.getName() == null ? key : this.getName() + getPathSeparator() + key, value);
            }
        }
        
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    
    public String getName() {
        return name;
    }

    public static JsonDocument createDocument(String name, Map<String, Object> results) {        
        return new JsonDocument(name, results);
    }

}
