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

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

public class WithItem extends BaseLanguageObject implements SubqueryContainer {

    private NamedTable table;
    private List<ColumnReference> columns;
    private QueryExpression queryExpression;
    private List<? extends List<?>> dependentValues;
    private boolean recusive;

    public NamedTable getTable() {
        return table;
    }

    public void setTable(NamedTable table) {
        this.table = table;
    }

    public List<ColumnReference> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnReference> columns) {
        this.columns = columns;
    }

    @Override
    public QueryExpression getSubquery() {
        return queryExpression;
    }

    @Override
    public void setSubquery(QueryExpression query) {
        this.queryExpression = query;
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setDependentValues(List<? extends List<?>> tupleBufferList) {
        this.dependentValues = tupleBufferList;
    }

    public List<? extends List<?>> getDependentValues() {
        return dependentValues;
    }

    public boolean isRecusive() {
        return recusive;
    }

    public void setRecusive(boolean recusive) {
        this.recusive = recusive;
    }

}
