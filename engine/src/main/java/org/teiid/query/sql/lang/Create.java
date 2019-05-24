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
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * @since 5.5
 */
public class Create extends Command implements TargetedCommand {

    public enum CommitAction {
        PRESERVE_ROWS,
    }

    /** Identifies the table to be created. */
    private GroupSymbol table;
    private List<ElementSymbol> primaryKey = new ArrayList<ElementSymbol>();
    private List<Column> columns = new ArrayList<Column>();
    private List<ElementSymbol> columnSymbols;
    private Table tableMetadata;
    private String on;
    private CommitAction commitAction;

    public GroupSymbol getTable() {
        return table;
    }

    @Override
    public GroupSymbol getGroup() {
        return table;
    }

    public void setTable(GroupSymbol table) {
        this.table = table;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<ElementSymbol> getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Derived ElementSymbol list.  Do not modify without also modifying the columns.
     * @return
     */
    public List<ElementSymbol> getColumnSymbols() {
        if (columnSymbols == null) {
            columnSymbols = new ArrayList<ElementSymbol>(columns.size());
            for (Column column : columns) {
                ElementSymbol es = new ElementSymbol(column.getName());
                es.setType(DataTypeManager.getDataTypeClass(column.getRuntimeType()));
                es.setGroupSymbol(table);
                columnSymbols.add(es);
            }
        }
        return columnSymbols;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#getType()
     * @since 5.5
     */
    public int getType() {
        return Command.TYPE_CREATE;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#clone()
     * @since 5.5
     */
    public Object clone() {
        Create copy = new Create();
        GroupSymbol copyTable = table.clone();
        copy.setTable(copyTable);
        copy.columns = new ArrayList<Column>(columns.size());
        for (Column column : columns) {
            Column copyColumn = new Column();
            copyColumn.setName(column.getName());
            copyColumn.setRuntimeType(column.getRuntimeType());
            copyColumn.setAutoIncremented(column.isAutoIncremented());
            copyColumn.setNullType(column.getNullType());
            copy.columns.add(copyColumn);
        }
        copy.primaryKey = LanguageObject.Util.deepClone(primaryKey, ElementSymbol.class);
        copyMetadataState(copy);
        copy.setTableMetadata(this.tableMetadata);
        copy.on = this.on;
        copy.commitAction = this.commitAction;
        return copy;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#getProjectedSymbols()
     * @since 5.5
     */
    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     * @since 5.5
     */
    public boolean areResultsCachable() {
        return false;
    }

    /**
     * @see org.teiid.query.sql.LanguageObject#acceptVisitor(org.teiid.query.sql.LanguageVisitor)
     * @since 5.5
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public void setElementSymbolsAsColumns(List<ElementSymbol> columns) {
        this.columns.clear();
        for (ElementSymbol elementSymbol : columns) {
            Column c = new Column();
            c.setName(elementSymbol.getShortName());
            c.setRuntimeType(DataTypeManager.getDataTypeName(elementSymbol.getType()));
            c.setNullType(NullType.Nullable);
            this.columns.add(c);
        }
    }

    public int hashCode() {
        return this.table.hashCode();
    }

    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Create)) {
            return false;
        }

        Create other = (Create) obj;

        if (other.columns.size() != this.columns.size()) {
            return false;
        }

        for (int i = 0; i < this.columns.size(); i++) {
            Column c = this.columns.get(i);
            Column o = other.columns.get(i);
            if (!c.getName().equalsIgnoreCase(o.getName())
                || DataTypeManager.getDataTypeClass(c.getRuntimeType().toLowerCase()) != DataTypeManager.getDataTypeClass(o.getRuntimeType().toLowerCase())
                || c.isAutoIncremented() != o.isAutoIncremented()
                || c.getNullType() != o.getNullType()) {
                return false;
            }
        }

        return this.commitAction == other.commitAction
               && EquivalenceUtil.areEqual(getTable(), other.getTable()) &&
               EquivalenceUtil.areEqual(getPrimaryKey(), other.getPrimaryKey()) &&
               EquivalenceUtil.areEqual(this.on, other.on) &&
               //metadata equality methods are basically identity based, so we need a better check
               ((tableMetadata == null && other.tableMetadata == null) || (tableMetadata != null && other.tableMetadata != null && this.toString().equals(other.toString())));
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public Table getTableMetadata() {
        return tableMetadata;
    }

    public void setTableMetadata(Table tableMetadata) {
        if (tableMetadata != null) {
            this.columns = tableMetadata.getColumns();
            this.table = new GroupSymbol(tableMetadata.getName());
        }
        this.tableMetadata = tableMetadata;
    }

    public CommitAction getCommitAction() {
        return commitAction;
    }

    public void setCommitAction(CommitAction commitAction) {
        this.commitAction = commitAction;
    }
}
