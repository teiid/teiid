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
package org.teiid.translator.document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: need to back this up with something like MapDB, to avoid OOM
 * Also need to write our own JSONPaser that returns this object directly.
 */
public class Document {
    private String name;
    private Map<String, Object> properties = new LinkedHashMap<String, Object>();
    private Map<String, List<Document>> children = new LinkedHashMap<String, List<Document>>();
    private boolean array;
    private Document parent;

    public Document() {
    }

    public Document(String name, boolean array, Document parent) {
        this.name = name;
        this.parent = parent;
        this.array = array;
    }

    public boolean isArray() {
        return array;
    }

    static List<Map<String, Object>> crossjoinWith(
            List<Map<String, Object>> left, List<? extends Document> rightDocuments) {
        ArrayList<Map<String, Object>> joined = new ArrayList<Map<String,Object>>();
        for (Document right : rightDocuments) {
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
            for (List<? extends Document> childDoc:this.children.values()) {
                joined = crossjoinWith(joined, childDoc);
            }
        }
        return joined;
    }

    public Map<String, Object> getProperties(){
        return this.properties;
    }

    public Map<String, List<Document>> getChildren() {
        return children;
    }

    public List<? extends Document> getChildDocuments(String path) {
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
            List<? extends Document> children =  this.children.get(path);
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
        if (this.parent == null) {
            this.properties.put(key, value);
        } else {
            this.properties.put(name(getName(), key), value);
        }
    }

    public void addArrayProperty(String key, Object value) {
        if (this.properties == null) {
            this.properties = new LinkedHashMap<String, Object>();
        }

        String propkey = this.parent == null?key:name(getName(), key);
        @SuppressWarnings("unchecked")
        List<Object> propValue = (List<Object>)this.properties.get(propkey);

        if (propValue == null) {
            propValue = new ArrayList<Object>();
            propValue.add(value);
        } else {
            propValue.add(value);
        }
        this.properties.put(propkey, propValue);
    }

    public void addChildDocuments(String path, List<Document> child) {
        this.children.put(path, child);
    }

    public List<Document> addChildDocument(String path, Document child) {
        if (this.children == null) {
            this.children = new LinkedHashMap<String, List<Document>>();
        }
        if (children.get(path) == null) {
            children.put(path, new ArrayList<Document>());
        }
        this.children.get(path).add(child);
        return this.children.get(path);
    }

    public String toString() {
        return this.name;
    }

    public Document getParent() {
        return this.parent;
    }

    public String getSimpleName() {
        return name;
    }
}
