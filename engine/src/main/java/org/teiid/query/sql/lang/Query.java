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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;

/**
 * A representation of a data query.  A query consists of various parts,
 * referred to as clauses.  The following list the types of clauses
 * that a query can hold, and their purpose: <p>
 * <pre>
 *      CLAUSE          PURPOSE
 *      =========       ==============================================
 *      Select          Defines the variables data to be retrieved for
 *        From            Defines the groups to retrieve data from
 *      Criteria        Defines constraints on data retrieval ("where")
 *         GroupBy            Defines how rows being returned should be grouped
 *         Having            Defines how groups should be filtered, also a criteria
 *      OrderBy         Defines how the results should be sorted
 *         Option            Defines any extra options on the query
 * </pre>
 */
public class Query extends QueryCommand implements FilteredCommand {

    /** The select clause. */
    private Select select;

    /** The from clause. */
    private From from;

    /** The criteria specifying constraints on what data will be retrieved. */
    private Criteria criteria;

    /** The group by specifying how to group rows. */
    private GroupBy groupBy;

    /** The having specifying which group rows will be returned. */
    private Criteria having;

    /** The into clause. */
    private Into into;

    //currently set by parser, but can be derived
    private boolean isRowConstructor;

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public Query() {
        super();
    }

    public static Query rowConstructor(List<Expression> select) {
        Query query = new Query();
        query.setSelect(new Select());
        query.getSelect().addSymbols(select);
        query.setRowConstructor(true);
        return query;
    }


    /**
     * Constructs an instance of this class given the specified clauses
     * @param select SELECT clause
     * @param from FROM clause
     * @param criteria WHERE clause
     * @param orderBy ORDER BY clause
     * @param option OPTION clause
     */
    public Query( Select select, From from, Criteria criteria, OrderBy orderBy, Option option ) {
        super();
        setSelect( select );
        setFrom( from );
        setCriteria( criteria );
        setOrderBy( orderBy );
        setOption( option );
    }

    /**
     * Constructs an instance of this class given all the clauses
     * @param select SELECT clause
     * @param from FROM clause
     * @param criteria WHERE clause
     * @param groupBy GROUP BY clause
     * @param having HAVING clause
     * @param orderBy ORDER BY clause
     * @param option OPTION clause
     */
    public Query( Select select, From from, Criteria criteria, GroupBy groupBy, Criteria having, OrderBy orderBy, Option option ) {
        super();
        setSelect( select );
        setFrom( from );
        setCriteria( criteria );
        setGroupBy( groupBy );
        setHaving( having );
        setOrderBy( orderBy );
        setOption( option );
    }

       /**
     * Return type of command.
     * @return TYPE_QUERY
     */
    public int getType() {
        return Command.TYPE_QUERY;
    }

    // =========================================================================
    //                     S E L E C T      M E T H O D S
    // =========================================================================

    /**
     * Get the select clause for the query.
     * @return SELECT clause
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Set the select clause for the query.
     * @param select SELECT clause
     */
    public void setSelect( Select select ) {
        this.select = select;
    }

    // =========================================================================
    //                     F R O M      M E T H O D S
    // =========================================================================

    /**
     * Get the from clause for the query.
     * @return FROM clause
     */
    public From getFrom() {
        return from;
    }

    /**
     * Set the from clause for the query.
     * @param from FROM clause
     */
    public void setFrom( From from  ) {
        this.from = from;
    }

    // =========================================================================
    //                   C R I T E R I A      M E T H O D S
    // =========================================================================

    /**
     * Get the criteria clause for the query.
     * @return WHERE clause
     */
    public Criteria getCriteria() {
        return criteria;
    }

    /**
     * Set the criteria clause for the query.
     * @param criteria WHERE clause
     */
    public void setCriteria( Criteria criteria ) {
        this.criteria = criteria;
    }

    /**
     * Set the criteria clause to null
     */
    public void clearCriteria() {
        this.criteria = null;
    }

    // =========================================================================
    //                    G R O U P   B Y     M E T H O D S
    // =========================================================================

    /**
     * Get the group by clause for the query.
     * @return GROUP BY clause
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * Set the group by clause for the query.
     * @param groupBy GROUP BY clause
     */
    public void setGroupBy( GroupBy groupBy ) {
        this.groupBy = groupBy;
    }


    // =========================================================================
    //                   H A V I N G      M E T H O D S
    // =========================================================================

    /**
     * Get the having clause for the query.
     * @return HAVING clause
     */
    public Criteria getHaving() {
        return having;
    }

    /**
     * Set the criteria clause for the query.
     * @param having HAVING clause
     */
    public void setHaving( Criteria having ) {
        this.having = having;
    }

    // =========================================================================
    //                   I N T O      M E T H O D S
    // =========================================================================

    /**
     * @return
     */
    public Into getInto() {
        return into;
    }

    /**
     * @param into
     */
    public void setInto(Into into) {
        this.into = into;
    }

    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

       /**
     * Get the ordered list of all elements returned by this query.  These elements
     * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
     * single column.
     * @return Ordered list of SingleElementSymbol
     */
    public List<Expression> getProjectedSymbols() {
        if(getSelect() != null) {
            if(getInto() != null){
                //SELECT INTO clause
                return Command.getUpdateCommandSymbol();
            }
            return getSelect().getProjectedSymbols();
        }
        return Collections.emptyList();
    }

    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Deep clone Query to produce a new identical query.
     * @return Deep clone
     */
    public Object clone() {
        Query copy = new Query();

        if(select != null) {
            copy.setSelect( select.clone());
        }

        if(from != null) {
            copy.setFrom( (From) from.clone());
        }

        if(criteria != null) {
            copy.setCriteria( (Criteria) criteria.clone());
        }

        if(groupBy != null) {
            copy.setGroupBy( (GroupBy) groupBy.clone());
        }

        if(having != null) {
            copy.setHaving( (Criteria) having.clone());
        }

        if(getOrderBy() != null) {
            copy.setOrderBy(getOrderBy().clone());
        }

        if(getLimit() != null) {
            copy.setLimit( getLimit().clone());
        }

        copy.setWith(LanguageObject.Util.deepClone(this.getWith(), WithQueryCommand.class));

        if (into != null) {
            copy.into = (Into)into.clone();
        }

        copyMetadataState(copy);
        copy.setRowConstructor(this.isRowConstructor);
        return copy;
    }

    /**
     * Compare two queries for equality.  Queries will only evaluate to equal if
     * they are IDENTICAL: select variables are in the same order, criteria are in
     * the same exact structure.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof Query)) {
            return false;
        }

        Query other = (Query) obj;

        return EquivalenceUtil.areEqual(getSelect(), other.getSelect()) &&
               EquivalenceUtil.areEqual(getFrom(), other.getFrom()) &&
               EquivalenceUtil.areEqual(getCriteria(), other.getCriteria()) &&
               EquivalenceUtil.areEqual(getGroupBy(), other.getGroupBy()) &&
               EquivalenceUtil.areEqual(getHaving(), other.getHaving()) &&
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
        myHash = HashCodeUtil.hashCode(myHash, this.select);
        myHash = HashCodeUtil.hashCode(myHash, this.criteria);
        return myHash;
    }

    /**
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable() {
        if(this.getInto() != null){
            return false;
        }
        List<Expression> projectedSymbols = getProjectedSymbols();
        return areColumnsCachable(projectedSymbols);
    }

    public static boolean areColumnsCachable(Collection<? extends Expression> projectedSymbols) {
        return true;
    }

    /**
     * @see org.teiid.query.sql.lang.QueryCommand#getProjectedQuery()
     */
    @Override
    public Query getProjectedQuery() {
        return this;
    }

    @Override
    public boolean returnsResultSet() {
        return into == null;
    }

    public boolean hasAggregates() {
        return getGroupBy() != null
        || getHaving() != null
        || !AggregateSymbolCollectorVisitor.getAggregates(getSelect(), false).isEmpty();
    }

    public boolean isRowConstructor() {
        return isRowConstructor;
    }

    public void setRowConstructor(boolean isRowConstructor) {
        this.isRowConstructor = isRowConstructor;
    }

}  // END CLASS
