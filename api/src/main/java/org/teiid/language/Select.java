/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.language;

import java.util.List;
import java.util.Map;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a simple SELECT query.
 */
public class Select extends QueryExpression {

    private List<DerivedColumn> derivedColumns;
    private boolean isDistinct;
    private List<TableReference> from;
    private Condition where;
    private GroupBy groupBy;
    private Condition having;
    private Map<String, List<? extends List<?>>> dependentValues;
        
    public Select(List<DerivedColumn> derivedColumns, boolean distinct, List<TableReference> from, Condition where,
                     GroupBy groupBy, Condition having, OrderBy orderBy) {
        this.derivedColumns = derivedColumns;
        this.isDistinct = distinct;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.setOrderBy(orderBy);
    }
    
    public List<DerivedColumn> getDerivedColumns() {
        return derivedColumns;
    }

    public boolean isDistinct() {
        return this.isDistinct;
    }

    public void setDerivedColumns(List<DerivedColumn> symbols) {
        this.derivedColumns = symbols;
    }

    public void setDistinct(boolean distinct) {
        this.isDistinct = distinct;
    }

    /**
     * Get FROM clause, should never be null.
     * @return From clause object
     */
    public List<TableReference> getFrom() {
        return from;
    }

    /**
     * Get WHERE clause, may be null.
     * @return A criteria object
     */
    public Condition getWhere() {
        return where;
    }

    /**
     * Get GROUP BY clause, may be null.
     * @return A group by object
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * Get HAVING clause, may be null.
     * @return A criteria object
     */
    public Condition getHaving() {
        return having;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set FROM clause, should never be null.
     * @param from From clause object
     */
    public void setFrom(List<TableReference> from) {
        this.from = from;
    }
    
    /**
     * Set WHERE clause, may be null.
     * @param criteria A criteria object
     */
    public void setWhere(Condition criteria) {
        this.where = criteria;
    }
    
    /**
     * Set GROUP BY clause, may be null.
     * @param groupBy A group by object
     */
    public void setGroupBy(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    /**
     * Set HAVING clause, may be null.
     * @param criteria A criteria object
     */
    public void setHaving(Condition criteria) {
        this.having = criteria;
    }
    
    public Select getProjectedQuery() {
        return this;
    }
    
    /**
     * Gets the dependent value lists.  The lists are memory-safe.  Caution should be used
     * to not access large lists in a non-memory safe manner.  The lists are invalid
     * after returning results from the pushdown query.
     * @return the map of dependent values or null if this is not a dependent join pushdown
     */
    public Map<String, List<? extends List<?>>> getDependentValues() {
		return dependentValues;
	}
    
    public void setDependentValues(Map<String, List<? extends List<?>>> dependentValues) {
		this.dependentValues = dependentValues;
	}
}
