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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;

/**
 * Represents the JSONTABLE table function.
 */
public class JsonTable extends TableFunctionReference {

    public static class JsonColumn extends ProjectedColumn {
        private boolean ordinal;
        private String path;

        public JsonColumn(String name) {
            super(name, DataTypeManager.DefaultDataTypes.INTEGER);
            this.ordinal = true;
        }

        public JsonColumn(String name, String type, String path) {
            super(name, type);
            this.path = path;
        }

        protected JsonColumn() {

        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isOrdinal() {
            return ordinal;
        }

        public void setOrdinal(boolean ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof JsonColumn)) {
                return false;
            }
            JsonColumn other = (JsonColumn)obj;
            return this.ordinal == other.ordinal
                && EquivalenceUtil.areEqual(this.path, other.path);
        }

        @Override
        public JsonColumn clone() {
            JsonColumn clone = new JsonColumn();
            super.copyTo(clone);
            clone.ordinal = this.ordinal;
            clone.path = this.path;
            return clone;
        }
    }

    private Expression json;
    private List<JsonColumn> columns = new ArrayList<JsonColumn>();
    private String rowPath;
    private Boolean nullLeaf;

    public Boolean getNullLeaf() {
        return nullLeaf;
    }

    public void setNullLeaf(Boolean nullLeaf) {
        this.nullLeaf = nullLeaf;
    }

    public List<JsonColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<JsonColumn> columns) {
        this.columns = columns;
    }

    public Expression getJson() {
        return json;
    }

    public void setJson(Expression json) {
        this.json = json;
    }

    public String getRowPath() {
        return rowPath;
    }

    public void setRowPath(String jsonQuery) {
        this.rowPath = jsonQuery;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected JsonTable cloneDirect() {
        JsonTable clone = new JsonTable();
        this.copy(clone);
        clone.setJson((Expression)this.json.clone());
        clone.rowPath = this.rowPath;
        clone.nullLeaf = this.nullLeaf;
        for (JsonColumn column : columns) {
            clone.getColumns().add(column.clone());
        }
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof JsonTable)) {
            return false;
        }
        JsonTable other = (JsonTable)obj;
        return this.columns.equals(other.columns)
            && EquivalenceUtil.areEqual(json, other.json)
            && EquivalenceUtil.areEqual(nullLeaf, other.nullLeaf)
            && EquivalenceUtil.areEqual(rowPath, other.rowPath);
    }

}
