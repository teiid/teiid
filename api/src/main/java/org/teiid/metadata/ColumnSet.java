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

package org.teiid.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;

public class ColumnSet<T extends AbstractMetadataRecord> extends AbstractMetadataRecord {

    private static final long serialVersionUID = -1185104601468519829L;

    private List<Column> columns;
    private T parent;
    private transient Map<String, Column> columnMap;

    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Get the {@link Column} via a case-insensitive lookup
     * @param name
     * @return the {@link Column} or null if it doesn't exist
     */
    public Column getColumnByName(String name) {
        if (columns == null || name == null) {
            return null;
        }
        Map<String, Column> map = columnMap;
        if (map == null) {
            map = new TreeMap<String, Column>(String.CASE_INSENSITIVE_ORDER);
            for (Column c : columns) {
                map.put(c.getName(), c);
            }
            columnMap = map;
        }
        return map.get(name);
    }

    public void addColumn(Column column) {
        if (columns == null) {
            columns = new ArrayList<Column>();
        }
        if (getColumnByName(column.getName()) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60016, DataPlugin.Util.gs(DataPlugin.Event.TEIID60016, getFullName() + AbstractMetadataRecord.NAME_DELIM_CHAR + column.getName()));
        }
        columns.add(column);
        Map<String, Column> map = columnMap;
        if (map != null) {
            map.put(column.getName(), column);
        }
    }

    public void removeColumn(Column column) {
        if (columns == null) {
            return;
        }
        columns.remove(column);
        Map<String, Column> map = columnMap;
        if (map != null) {
            map.remove(column.getName());
        }
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
        columnMap = null;
    }

    @Override
    public T getParent() {
        return parent;
    }

    public void setParent(T parent) {
        this.parent = parent;
    }

}