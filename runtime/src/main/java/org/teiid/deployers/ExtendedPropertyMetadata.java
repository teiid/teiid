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
package org.teiid.deployers;

import java.util.ArrayList;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.runtime.RuntimePlugin;


/**
 * This is used with ra.xml properties file to extend the metadata on the properties.
 */
public class ExtendedPropertyMetadata {
    String displayName;
    String description;
    boolean advanced;
    boolean masked;
    boolean editable = true;
    boolean required;
    ArrayList<String> allowed;
    String name;
    String dataType;
    String defaultValue;
    String category;
    String owner;

    public ExtendedPropertyMetadata() {
    }

    public ExtendedPropertyMetadata(String name, String type, String encodedData, String defaultValue) {
        this.name = name;
        this.dataType = type;
        this.defaultValue = defaultValue;

        encodedData = encodedData.trim();

        // if not begins with { then treat as if just a simple description field.
        if (!encodedData.startsWith("{")) { //$NON-NLS-1$
            this.displayName = encodedData;
            return;
        }

        if (!encodedData.endsWith("}")) { //$NON-NLS-1$
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40034, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40034, encodedData));
        }
        encodedData = encodedData.substring(1, encodedData.length()-1);

        int index = 0;
        int start = -1;
        boolean inQuotes = false;
        int inQuotesStart = -1;
        boolean inArray = false;

        String propertyName = null;
        ArrayList<String> values = new ArrayList<String>();
        for (char c:encodedData.toCharArray()) {
            if (c == '$' && start == -1) {
                start = index;
            }
            else if (c == '"') {
                inQuotes = !inQuotes;
                if (inQuotes && inQuotesStart == -1) {
                    inQuotesStart = index;
                }
                else if (!inQuotes && inQuotesStart != -1) {
                    if (inQuotesStart+1 != index) {
                        values.add(encodedData.substring(inQuotesStart+1, index));
                    }
                    else {
                        values.add(""); //$NON-NLS-1$
                    }
                    inQuotesStart = -1;
                }
            }
            else if (c == '[') {
                inArray = true;
            }
            else if (c == ']') {
                inArray = false;
            }
            else if (c == ':' && !inQuotes && !inArray && start != -1) {
                propertyName = encodedData.substring(start, index);
            }
            else if (c == ',' && !inQuotes && !inArray && start != -1) {
                addProperty(propertyName, values);
                propertyName = null;
                values = new ArrayList<String>();
                start = -1;
            }
            index++;
        }
        // add last property
        if (propertyName != null) {
            addProperty(propertyName, values);
        }
    }

    private void addProperty(String name, ArrayList<String> values) {
        if (name.equals("$display")) { //$NON-NLS-1$
            this.displayName = values.get(0);
        }
        else if (name.equals("$description")) { //$NON-NLS-1$
            this.description = values.get(0);
        }
        else if (name.equals("$advanced")) { //$NON-NLS-1$
            this.advanced = Boolean.parseBoolean(values.get(0));
        }
        else if (name.equals("$masked")) { //$NON-NLS-1$
            this.masked = Boolean.parseBoolean(values.get(0));
        }
        else if (name.equals("$editable")) { //$NON-NLS-1$
            this.editable = Boolean.parseBoolean(values.get(0));
        }
        else if (name.equals("$allowed")) { //$NON-NLS-1$
            this.allowed = new ArrayList<String>(values);
        }
        else if (name.equals("$required")) { //$NON-NLS-1$
            this.required = Boolean.parseBoolean(values.get(0));
        }
    }

    public String name() {
        return this.name;
    }
    public String description() {
        return description;
    }
    public String display() {
        return displayName;
    }
    public boolean advanced() {
        return advanced;
    }
    public boolean masked() {
        return masked;
    }
    public boolean readOnly() {
        return !editable;
    }
    public boolean required() {
        return required;
    }
    public String[] allowed() {
        if (allowed != null) {
            return allowed.toArray(new String[allowed.size()]);
        }
        return new String[] {};
    }
    public String datatype() {
        return this.dataType;
    }
    public String defaultValue() {
        return this.defaultValue;
    }
    public String category() {
        return this.category;
    }
    public String owner() {
        return this.owner;
    }
}
