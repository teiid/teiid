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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.TranslatorException;

import com.mongodb.DB;
import com.mongodb.DBRef;

public class MergeDetails implements Cloneable {
    enum Association {ONE, MANY};

    private String parentTable;
    private IDRef id;
    private List<String> referenceColumns;
    private List<String> columns;

    private String embeddedTable;
    private Association association;
    private String name;
    private String idReference;
    private String referenceName;
    private String alias;
    private boolean nested;
    private MongoDocument document;

    public MergeDetails(MongoDocument document) {
        this.document = document;
    }

    public String getAlias() {
        if (this.alias != null) {
            return alias;
        }
        return this.name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public MongoDocument getDocument() {
        return this.document;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DBRef getDBRef(DB db, boolean push) {
        if (this.id != null) {
            if (this.idReference != null) {
                return new DBRef(db.getName(), push?this.parentTable:this.embeddedTable, new DBRef(db.getName(), this.idReference, this.id.getValue()));
            }
            return new DBRef(db.getName(), push?this.parentTable:this.embeddedTable, this.id.getValue());
        }
        return null;
    }

    public Object getValue() {
        if (this.id != null) {
            return this.id.getValue();
        }
        return null;
    }

    public String getParentTable() {
        return this.parentTable;
    }

    public void setParentTable(String parentTable) {
        this.parentTable = parentTable;
    }

    public Object getId() throws TranslatorException {
        if (this.id == null) {
            return null;
        }
        if (this.id.pk.keySet().size() != this.columns.size()) {
            throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18014));
        }
        return this.id.getValue();
    }

    public void setId(String column, Object value) {
        if (this.id == null) {
            this.id = new IDRef();
        }
        this.id.addColumn(column, value);
    }

    public List<String> getReferenceColumns() {
        return this.referenceColumns;
    }

    public void setReferenceColumns(List<String> columns) {
        this.referenceColumns = new ArrayList<String>(columns);
    }

    public String getEmbeddedTable() {
        return this.embeddedTable;
    }

    public void setEmbeddedTable(String embeddedTable) {
        this.embeddedTable = embeddedTable;
    }

    public Association getAssociation() {
        return this.association;
    }

    public void setAssociation(Association association) {
        this.association = association;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = new ArrayList<String>(columns);
    }

    public String getIdReference() {
        return this.idReference;
    }

    public void setIdReference(String idReference) {
        this.idReference = idReference;
    }

    public boolean isNested() {
        return nested;
    }

    public void setNested(boolean nested) {
        this.nested = nested;
    }

    public String getParentColumnName(String columnName) {
        for(int i = 0; i< this.columns.size(); i++) {
            if (this.columns.get(i).equalsIgnoreCase(columnName)) {
                return this.referenceColumns.get(i);
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ParentTable:").append(this.parentTable); //$NON-NLS-1$
        sb.append(" id:").append(this.id); //$NON-NLS-1$
        sb.append(" EmbeddedTable:").append(this.embeddedTable); //$NON-NLS-1$
        return sb.toString();
    }

    @Override
    public MergeDetails clone() {
        MergeDetails clone = new MergeDetails(this.document);
        clone.parentTable = this.parentTable;
        if (this.id != null) {
            clone.id = this.id.clone();
        }
        clone.referenceColumns = new ArrayList<>(this.referenceColumns);
        clone.columns = new ArrayList<String>(this.columns);
        clone.embeddedTable = this.embeddedTable;
        clone.association = this.association;
        clone.name = this.name;
        clone.idReference = this.idReference;
        clone.referenceName = this.referenceName;
        clone.alias = this.alias;
        clone.nested = this.nested;
        return clone;
    }
}
