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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.SymbolMap;


/**
 * This object acts as a Set operator on multiple Queries - UNION,
 * INTERSECT, and EXCEPT can be implemented with this Class
 */
public class SetQuery extends QueryCommand {

    public enum Operation {
        /** Represents UNION of two queries */
        UNION,
        /** Represents intersection of two queries */
        INTERSECT,
        /** Represents set difference of two queries */
        EXCEPT
    }

    private boolean all = true;
    private Operation operation;
    private QueryCommand leftQuery;
    private QueryCommand rightQuery;

    private List<Class<?>> projectedTypes = null;  //set during resolving
    private QueryMetadataInterface metadata = null; // set during resolving

    /**
     * Construct query with operation type
     * @param operation Operation as specified like {@link Operation#UNION}
     */
    public SetQuery(Operation operation) {
        this.operation = operation;
    }

    public SetQuery(Operation operation, boolean all, QueryCommand leftQuery, QueryCommand rightQuery) {
        this.operation = operation;
        this.all = all;
        this.leftQuery = leftQuery;
        this.rightQuery = rightQuery;
    }

    public Query getProjectedQuery() {
        if (leftQuery instanceof SetQuery) {
            return ((SetQuery)leftQuery).getProjectedQuery();
        }
        return (Query)leftQuery;
    }

       /**
     * Return type of command.
     * @return TYPE_QUERY
     */
    public int getType() {
        return Command.TYPE_QUERY;
    }

    /**
     * Set type of operation
     * @param operation Operation constant as defined in this class
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /**
     * Get operation for this set
     * @return Operation as defined in this class
     */
    public Operation getOperation() {
        return this.operation;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

       /**
     * Get the ordered list of all elements returned by this query.  These elements
     * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
     * single column.
     * @return Ordered list of SingleElementSymbol
     */
    public List getProjectedSymbols() {
        Query query = getProjectedQuery();
        List projectedSymbols = query.getProjectedSymbols();
        if (projectedTypes != null) {
            return getTypedProjectedSymbols(projectedSymbols, projectedTypes, metadata);
        }
        return projectedSymbols;
    }

    public static List<Expression> getTypedProjectedSymbols(List<? extends Expression> acutal, List<Class<?>> projectedTypes, QueryMetadataInterface metadata) {
        List<Expression> newProject = new ArrayList<Expression>();
        for (int i = 0; i < acutal.size(); i++) {
            Expression originalSymbol = acutal.get(i);
            Expression symbol = originalSymbol;
            Class<?> type = projectedTypes.get(i);
            if (symbol.getType() != type) {
                symbol = SymbolMap.getExpression(originalSymbol);
                try {
                    symbol = ResolverUtil.convertExpression(symbol, DataTypeManager.getDataTypeName(type), metadata);
                } catch (QueryResolverException err) {
                     throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30447, err);
                }

                if (originalSymbol instanceof Symbol) {
                    symbol = new AliasSymbol(Symbol.getShortName(originalSymbol), symbol);
                }
            }
            newProject.add(symbol);
        }
        return newProject;
    }

    /**
     * Deep clone this object to produce a new identical query.
     * @return Deep clone
     */
    public Object clone() {
        SetQuery copy = new SetQuery(this.operation);

        this.copyMetadataState(copy);

        copy.leftQuery = (QueryCommand)this.leftQuery.clone();
        copy.rightQuery = (QueryCommand)this.rightQuery.clone();

        copy.setAll(this.all);

        if(this.getOrderBy() != null) {
            copy.setOrderBy(this.getOrderBy().clone());
        }

        if(this.getLimit() != null) {
            copy.setLimit( this.getLimit().clone() );
        }

        copy.setWith(LanguageObject.Util.deepClone(this.getWith(), WithQueryCommand.class));

        if (this.projectedTypes != null) {
            copy.setProjectedTypes(new ArrayList<Class<?>>(projectedTypes), this.metadata);
        }

        return copy;
    }

    /**
     * Compare two queries for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof SetQuery)) {
            return false;
        }

        SetQuery other = (SetQuery) obj;

        return getOperation() == other.getOperation() &&
        EquivalenceUtil.areEqual(this.isAll(), other.isAll()) &&
        EquivalenceUtil.areEqual(this.leftQuery, other.leftQuery) &&
        EquivalenceUtil.areEqual(this.rightQuery, other.rightQuery) &&
        EquivalenceUtil.areEqual(getOrderBy(), other.getOrderBy()) &&
        EquivalenceUtil.areEqual(getLimit(), other.getLimit()) &&
        EquivalenceUtil.areEqual(getWith(), other.getWith()) &&
        sameOptionAndHint(other);
    }

    /**
     * Get hashcode for query.  WARNING: This hash code relies on the hash codes of the
     * Select and Criteria clauses.  If the query changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after query has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        // For speed, this hash code relies only on the hash codes of its select
        // and criteria clauses, not on the from, order by, or option clauses
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.operation);
        myHash = HashCodeUtil.hashCode(myHash, getProjectedQuery());
        return myHash;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable() {
        return leftQuery.areResultsCachable() && rightQuery.areResultsCachable();
    }

    /**
     * @return the left and right queries as a list.  This list cannot be modified.
     */
    public List<QueryCommand> getQueryCommands() {
        return Collections.unmodifiableList(Arrays.asList(leftQuery, rightQuery));
    }

    public void setProjectedTypes(List<Class<?>> projectedTypes, QueryMetadataInterface metadata) {
        this.projectedTypes = projectedTypes;
        this.metadata = metadata;
    }

    /**
     * @return Returns the projectedTypes.
     */
    public List<Class<?>>  getProjectedTypes() {
        return this.projectedTypes;
    }

    public boolean isAll() {
        return this.all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public QueryCommand getLeftQuery() {
        return this.leftQuery;
    }

    public void setLeftQuery(QueryCommand leftQuery) {
        this.leftQuery = leftQuery;
    }

    public QueryCommand getRightQuery() {
        return this.rightQuery;
    }

    public void setRightQuery(QueryCommand rightQuery) {
        this.rightQuery = rightQuery;
    }

}
