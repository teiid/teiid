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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;

/**
 * Represents the ArrayTable table function.
 */
public class ArrayTable extends TableFunctionReference {

    private Expression arrayValue;
    private List<ProjectedColumn> columns = new ArrayList<ProjectedColumn>();
    private Boolean singleRow;

    public List<ProjectedColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<ProjectedColumn> columns) {
        this.columns = columns;
    }

    public Expression getArrayValue() {
        return arrayValue;
    }

    public void setArrayValue(Expression arrayValue) {
        this.arrayValue = arrayValue;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public void setSingleRow(Boolean singleRow) {
        this.singleRow = singleRow;
    }

    public Boolean getSingleRow() {
        return singleRow;
    }

    @Override
    protected ArrayTable cloneDirect() {
        ArrayTable clone = new ArrayTable();
        this.copy(clone);
        clone.singleRow = singleRow;
        clone.setArrayValue((Expression)this.arrayValue.clone());
        for (ProjectedColumn column : columns) {
            clone.getColumns().add(column.copyTo(new ProjectedColumn()));
        }
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ArrayTable)) {
            return false;
        }
        ArrayTable other = (ArrayTable)obj;
        return this.columns.equals(other.columns)
            && EquivalenceUtil.areEqual(arrayValue, other.arrayValue)
            && EquivalenceUtil.areEqual(singleRow, other.singleRow);
    }

}
