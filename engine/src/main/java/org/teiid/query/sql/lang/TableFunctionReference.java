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
import java.util.Collection;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;

public abstract class TableFunctionReference extends FromClause {

    public static class ProjectedColumn {
        private String name;
        private String type;
        private ElementSymbol symbol;

        public ProjectedColumn(String name, String type) {
            this.name = name;
            this.type = type;
            this.symbol = new ElementSymbol(name);
            symbol.setType(DataTypeManager.getDataTypeClass(type));
        }

        protected ProjectedColumn() {

        }

        public ElementSymbol getSymbol() {
            return symbol;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ProjectedColumn)) {
                return false;
            }
            ProjectedColumn other = (ProjectedColumn)obj;
            return this.symbol.equals(other.symbol)
                && this.name.equals(other.name)
                && this.type.equals(other.type);
        }

        public int hashCode() {
            return symbol.hashCode();
        }

        public ProjectedColumn copyTo(ProjectedColumn copy) {
            copy.name = this.name;
            copy.type = this.type;
            copy.symbol = this.symbol.clone();
            return copy;
        }

    }

    private GroupSymbol symbol;
    private SymbolMap correlatedReferences;

    public SymbolMap getCorrelatedReferences() {
        return correlatedReferences;
    }

    public void setCorrelatedReferences(SymbolMap correlatedReferences) {
        this.correlatedReferences = correlatedReferences;
    }

    public void copy(TableFunctionReference copy) {
        copy.symbol = this.symbol.clone();
        if (correlatedReferences != null) {
            copy.correlatedReferences = correlatedReferences.clone();
        }
    }

    @Override
    public void collectGroups(Collection<GroupSymbol> groups) {
        groups.add(getGroupSymbol());
    }

    /**
     * Get name of this clause.
     * @return Name of clause
     */
    public String getName() {
        return this.symbol.getName();
    }

    public String getOutputName() {
        return this.symbol.getOutputName();
    }

    /**
     * Get GroupSymbol representing the named subquery
     * @return GroupSymbol representing the subquery
     */
    public GroupSymbol getGroupSymbol() {
        return this.symbol;
    }

    /**
     * Reset the alias for this subquery from clause and it's pseudo-GroupSymbol.
     * WARNING: this will modify the hashCode and equals semantics and will cause this object
     * to be lost if currently in a HashMap or HashSet.
     * @param name New name
     * @since 4.3
     */
    public void setName(String name) {
        this.symbol = new GroupSymbol(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TableFunctionReference)) {
            return false;
        }
        TableFunctionReference other = (TableFunctionReference)obj;
        return EquivalenceUtil.areEqual(symbol, other.symbol);
    }

    @Override
    public int hashCode() {
        return this.symbol.hashCode();
    }

    public abstract List<? extends ProjectedColumn> getColumns();

    public List<ElementSymbol> getProjectedSymbols() {
        ArrayList<ElementSymbol> symbols = new ArrayList<ElementSymbol>(getColumns().size());
        for (ProjectedColumn col : getColumns()) {
            symbols.add(col.getSymbol());
        }
        return symbols;
    }

}
