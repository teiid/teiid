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

package org.teiid.language;

import java.util.Iterator;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;


public class Insert extends BaseLanguageObject implements BulkCommand {

    private NamedTable table;
    private List<ColumnReference> columns;
    private InsertValueSource valueSource;
    private Iterator<? extends List<?>> parameterValues;

    private boolean upsert;

    public Insert(NamedTable group, List<ColumnReference> elements, InsertValueSource valueSource) {
        this.table = group;
        this.columns = elements;
        this.valueSource = valueSource;
    }

    public NamedTable getTable() {
        return this.table;
    }

    public List<ColumnReference> getColumns() {
        return this.columns;
    }

    public InsertValueSource getValueSource() {
        return this.valueSource;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setTable(NamedTable group) {
        this.table = group;
    }

    public void setColumns(List<ColumnReference> elements) {
        this.columns = elements;
    }

    public void setValueSource(InsertValueSource values) {
        this.valueSource = values;
    }

    @Override
    public Iterator<? extends List<?>> getParameterValues() {
        return this.parameterValues;
    }

    public void setParameterValues(Iterator<? extends List<?>> parameterValues) {
        this.parameterValues = parameterValues;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

}
