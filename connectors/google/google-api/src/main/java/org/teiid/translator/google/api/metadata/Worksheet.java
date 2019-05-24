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

package org.teiid.translator.google.api.metadata;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Worksheet {
    private String id;
    private String name;
    private LinkedHashMap<String, Column> columns = new LinkedHashMap<String, Column>();
    private boolean headerEnabled=false;

    public LinkedHashMap<String, Column> getColumns() {
        return columns;
    }

    public List<Column> getColumnsAsList() {
        return new ArrayList<Column>(columns.values());
    }

    public void addColumn(String label, Column column) {
        columns.put(label, column);
    }

    public String getColumnID(String columnLabel) {
        Column column = columns.get(columnLabel);
        if (column == null) {
            return null;
        }
        return column.getAlphaName();
    }

    public Worksheet( String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isHeaderEnabled() {
        return headerEnabled;
    }

    public void setHeaderEnabled(boolean headerEnabled) {
        this.headerEnabled = headerEnabled;
    }

}
