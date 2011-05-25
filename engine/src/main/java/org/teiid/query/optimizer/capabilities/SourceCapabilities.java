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

package org.teiid.query.optimizer.capabilities;

public interface SourceCapabilities {

    public enum Capability {
        /**
         * Support indicates connector can accept queries with SELECT DISTINCT
         */
        QUERY_SELECT_DISTINCT,
        /**
         * Support indicates connector can accept queries non-elements in the SELECT
         */
        QUERY_SELECT_EXPRESSION,
        /**
         * Support indicates connector can accept joins where groups have aliases (required for QUERY_FROM_JOIN_SELFJOIN)
         * 
         * @since 3.1 SP2
         */
        QUERY_FROM_GROUP_ALIAS,
        /**
         * Max number of groups appearing in a from clause
         */
        MAX_QUERY_FROM_GROUPS,
        /**
         * @since 6.1.0
         */
        JOIN_CRITERIA_ALLOWED,
        /**
         * Support indicates connector can accept inner joins
         * 
         * @since 3.1 SP2
         */
        QUERY_FROM_JOIN_INNER,
        /**
         * Indicates that the source prefers ANSI style joins
         * 
         * @since 6.0.0
         */
        QUERY_FROM_ANSI_JOIN,
        /**
         * Support indicates connector can accept self-joins where a group is joined to itself with aliases
         * 
         * @since 3.1 SP2
         */
        QUERY_FROM_JOIN_SELFJOIN,
        /**
         * Support indicates connector can accept right or left outer joins
         * 
         * @since 3.1 SP2
         */
        QUERY_FROM_JOIN_OUTER,
        /**
         * Support indicates connector can push down inline views
         * 
         * @since 4.1
         */
        QUERY_FROM_INLINE_VIEWS,
        /**
         * Support indicates connector can accept full outer joins
         * 
         * @since 3.1 SP2
         */
        QUERY_FROM_JOIN_OUTER_FULL,
        /**
         * Support indicates connector accepts criteria of form (element BETWEEN constant AND constant)
         * 
         * @since 4.0
         */
        CRITERIA_BETWEEN,
        /**
         * Support indicates connector accepts criteria of form (element operator constant)
         * 
         * @since 3.1 SP2
         */
        CRITERIA_COMPARE_EQ,
        CRITERIA_COMPARE_ORDERED,
        /**
         * Support indicates connector accepts criteria of form (element LIKE constant)
         * 
         * @since 3.1 SP2
         */
        CRITERIA_LIKE,
        /**
         * Support indicates connector accepts criteria of form (element LIKE constant ESCAPE char) - CURRENTLY NOT USED
         * 
         * @since 3.1 SP2
         */
        CRITERIA_LIKE_ESCAPE,
        /**
         * Support indicates connector accepts criteria of form (element IN set)
         * 
         * @since 3.1 SP2
         */
        CRITERIA_IN,
        /**
         * Support indicates connector accepts IN criteria with a subquery on the right side
         * 
         * @since 4.0
         */
        CRITERIA_IN_SUBQUERY,
        /**
         * Support indicates connector accepts criteria of form (element IS NULL)
         * 
         * @since 3.1 SP2
         */
        CRITERIA_ISNULL,
        /**
         * Support indicates connector accepts logical criteria connected by OR
         * 
         * @since 3.1 SP2
         */
        CRITERIA_OR,
        /**
         * Support indicates connector accepts logical criteria NOT
         * 
         * @since 3.1 SP2
         */
        CRITERIA_NOT,
        /**
         * Support indicates connector accepts the EXISTS criteria
         * 
         * @since 4.0
         */
        CRITERIA_EXISTS,
        /**
         * Support indicates connector accepts the quantified comparison criteria that use SOME
         * 
         * @since 4.0
         */
        CRITERIA_QUANTIFIED_SOME,
        /**
         * Support indicates connector accepts the quantified comparison criteria that use ALL
         * 
         * @since 4.0
         */
        CRITERIA_QUANTIFIED_ALL,
        /**
         * Support indicates connector accepts ORDER BY clause
         * 
         * @since 3.1 SP2
         */
        QUERY_ORDERBY,
        
        QUERY_ORDERBY_UNRELATED,
        QUERY_ORDERBY_NULL_ORDERING,
        QUERY_ORDERBY_DEFAULT_NULL_ORDER,
        /**
         * Composite support for group by and having - not
         * used by the connector layer
         */
        QUERY_AGGREGATES,
        /**
         * @since 6.1.0 indicates support for GROUP BY
         */
        QUERY_GROUP_BY,
        /**
         * @since 6.1.0 indicates support for HAVING
         */
        QUERY_HAVING,
        /**
         * Support indicates connector can accept the SUM aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_SUM,
        /**
         * Support indicates connector can accept the AVG aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_AVG,
        /**
         * Support indicates connector can accept the MIN aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_MIN,
        /**
         * Support indicates connector can accept the MAX aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_MAX,
        /**
         * Support indicates connector can accept the enhanced numeric aggregates
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_ENHANCED_NUMERIC,
        /**
         * Support indicates connector can accept the COUNT aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_COUNT,
        /**
         * Support indicates connector can accept the COUNT(*) aggregate function
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_COUNT_STAR,
        /**
         * Support indicates connector can accept DISTINCT within aggregate functions
         * 
         * @since 3.1 SP2
         */
        QUERY_AGGREGATES_DISTINCT,
        /**
         * Support indicates connector can accept scalar subqueries in the SELECT, WHERE, and HAVING clauses
         * 
         * @since 4.0
         */
        QUERY_SUBQUERIES_SCALAR,
        /**
         * Support indicates connector can accept correalted subqueries wherever subqueries are accepted
         * 
         * @since 4.0
         */
        QUERY_SUBQUERIES_CORRELATED,
        /**
         * Support indicates connector can accept queries with non-searched CASE <expression> WHEN <expression> ... END
         * 
         * @since 4.0
         */
        QUERY_CASE,
        /**
         * Support indicates connector can accept queries with searched CASE WHEN <criteria> ... END
         * 
         * @since 4.0
         */
        QUERY_SEARCHED_CASE,
        /**
         * Support indicates connector can accept UNION and UNION ALL
         * 
         * @since 4.2
         */
        QUERY_UNION,
        /**
         * Support indicates connector can accept INTERSECT
         * 
         * @since 5.6
         */
        QUERY_INTERSECT,
        /**
         * Support indicates connector can accept EXCEPT
         * 
         * @since 5.6
         */
        QUERY_EXCEPT,
        /**
         * Support indicates connector can accept SET QUERY with an ORDER BY clause
         * 
         * @since 4.2
         */
        QUERY_SET_ORDER_BY,
        /**
         * Support indicates connector can accept GROUP BY with functions in it.
         * 
         * @since 5.0
         */
        QUERY_FUNCTIONS_IN_GROUP_BY,
        /**
         * Support indicates connector can accept queries with searched CASE WHEN <criteria> ... END
         * 
         * @since 4.2
         */
        BATCHED_UPDATES,
        BULK_UPDATE,
        /**
         * Support indicates connector can limit result rows
         * 
         * @since 5.0 SP1
         */
        ROW_LIMIT,
        /**
         * Support indicates connector support a SQL clause whose output rows offset from the query's result rows (similar to
         * LIMIT with offset)
         * 
         * @since 5.0 SP1
         */
        ROW_OFFSET,
        /**
         * The Maximum number of values allowed in an IN criteria (Integer)
         * 
         * @since 4.4
         */
        MAX_IN_CRITERIA_SIZE,
        /**
         * The connector ID, which is used by the optimizer to determine when two models are bound to the same connector
         * 
         * @since 5.0.2
         */
        CONNECTOR_ID,
        /**
         * @since 6.0.0 indicates support for where all
         */
        REQUIRES_CRITERIA,
        INSERT_WITH_QUERYEXPRESSION,
        INSERT_WITH_ITERATOR,
        COMMON_TABLE_EXPRESSIONS,
        MAX_DEPENDENT_PREDICATES,
    }

    public enum Scope {
        /**
         * A legal value for the {@link #SCOPE} property.
         */
        SCOPE_GLOBAL,
        /**
         * A legal value for the {@link #SCOPE} property.
         */
        SCOPE_PER_USER
    }

    /**
     * Determine the scope of these capabilities.
     */
    Scope getScope();

    /**
     * Returns true if the capability is supported. The capability constants are all defined in this interface with the meaning of
     * supporting that capability.
     * 
     * @param capability
     *            Name of capability
     * @return True if supported, false otherwise
     */
    public boolean supportsCapability(Capability capability);

    /**
     * This method can be used to check whether a particular function is supported by this connector. This method should only be
     * used if the capability FUNCTION is true.
     * 
     * @param functionName
     *            The function that may be supported
     * @return True if function is supported.
     */
    public boolean supportsFunction(String functionName);

    /**
     * This method returns an Object corresponding to the Source Property
     * 
     * @since 4.4
     */
    public Object getSourceProperty(Capability propertyName);
}
