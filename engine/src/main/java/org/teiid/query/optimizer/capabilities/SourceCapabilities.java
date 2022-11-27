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

package org.teiid.query.optimizer.capabilities;

import org.teiid.translator.ExecutionFactory.Format;

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
        QUERY_FROM_GROUP_ALIAS("TableAlias"), //$NON-NLS-1$
        /**
         * Max number of groups appearing in a from clause
         */
        MAX_QUERY_FROM_GROUPS,
        /**
         * Max number of columns appearing in a select clause
         */
        MAX_QUERY_PROJECTED_COLUMNS,
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
        QUERY_FROM_JOIN_SELFJOIN("SelfJoins"), //$NON-NLS-1$
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
        QUERY_FROM_INLINE_VIEWS("InlineViews"), //$NON-NLS-1$
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
        CRITERIA_IS_DISTINCT,
        /**
         * Support indicates connector accepts criteria of form (element LIKE constant)
         *
         * @since 3.1 SP2
         */
        CRITERIA_LIKE("LikeCriteria"), //$NON-NLS-1$
        /**
         * Support indicates connector accepts criteria of form (element LIKE constant ESCAPE char) - CURRENTLY NOT USED
         *
         * @since 3.1 SP2
         */
        CRITERIA_LIKE_ESCAPE("LikeCriteriaEscapeCharacter"), //$NON-NLS-1$
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

        CRITERIA_ONLY_LITERAL_COMPARE,
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
        QUERY_AGGREGATES_DISTINCT("AggregatesDistinct"), //$NON-NLS-1$
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
         * Support indicates connector can accept queries with non-searched CASE &lt;expression&gt; WHEN &lt;expression&gt; ... END
         *
         * @since 4.0
         */
        QUERY_CASE,
        /**
         * Support indicates connector can accept queries with searched CASE WHEN &lt;criteria&gt; ... END
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
        QUERY_SET_LIMIT_OFFSET,
        /**
         * Support indicates connector can accept GROUP BY with functions in it.
         *
         * @since 5.0
         */
        QUERY_FUNCTIONS_IN_GROUP_BY,
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
        ADVANCED_OLAP("AdvancedOLAP"), //$NON-NLS-1$
        QUERY_AGGREGATES_ARRAY,
        ELEMENTARY_OLAP("ElementaryOLAP"), //$NON-NLS-1$
        WINDOW_FUNCTION_ORDER_BY_AGGREGATES("WindowOrderByAggregates"), //$NON-NLS-1$
        CRITERIA_SIMILAR,
        CRITERIA_LIKE_REGEX,
        DEPENDENT_JOIN,
        WINDOW_FUNCTION_DISTINCT_AGGREGATES("WindowDistinctAggregates"), //$NON-NLS-1$
        QUERY_ONLY_SINGLE_TABLE_GROUP_BY,
        ONLY_FORMAT_LITERALS,
        CRITERIA_ON_SUBQUERY,
        ARRAY_TYPE,
        QUERY_SUBQUERIES_ONLY_CORRELATED,
        QUERY_AGGREGATES_STRING,
        FULL_DEPENDENT_JOIN,
        SELECT_WITHOUT_FROM,
        QUERY_GROUP_BY_ROLLUP,
        QUERY_ORDERBY_EXTENDED_GROUPING,
        INVALID_EXCEPTION, //property saying why the capabilities are invalid
        COLLATION_LOCALE,
        RECURSIVE_COMMON_TABLE_EXPRESSIONS,
        EXCLUDE_COMMON_TABLE_EXPRESSION_NAME,
        CRITERIA_COMPARE_ORDERED_EXCLUSIVE,
        PARTIAL_FILTERS,
        DEPENDENT_JOIN_BINDINGS,
        SUBQUERY_COMMON_TABLE_EXPRESSIONS,
        SUBQUERY_CORRELATED_LIMIT,
        NO_PROJECTION,
        REQUIRED_LIKE_ESCAPE,
        QUERY_SUBQUERIES_SCALAR_PROJECTION,
        TRANSACTION_SUPPORT,
        QUERY_FROM_JOIN_LATERAL,
        QUERY_FROM_JOIN_LATERAL_CONDITION,
        QUERY_FROM_PROCEDURE_TABLE,
        QUERY_GROUP_BY_MULTIPLE_DISTINCT_AGGREGATES,
        UPSERT,
        QUERY_SELECT_EXPRESSION_ARRAY_TYPE,
        QUERY_ONLY_FROM_JOIN_LATERAL_PROCEDURE,
        ONLY_TIMESTAMPADD_LITERAL,
        QUERY_WINDOW_FUNCTION_NTILE,
        QUERY_WINDOW_FUNCTION_PERCENT_RANK,
        QUERY_WINDOW_FUNCTION_CUME_DIST,
        QUERY_WINDOW_FUNCTION_NTH_VALUE,
        WINDOW_FUNCTION_FRAME_CLAUSE,
        QUERY_AGGREGATES_LIST,
        QUERY_AGGREGATES_COUNT_BIG,
        GEOGRAPHY_TYPE,
        PROCEDURE_PARAMETER_EXPRESSION,
        MAX_QUERY_FROM_ONE_TO_MANY,
        QUERY_ONLY_FROM_RELATIONSHIP_JOIN;

        private final String toString;

        Capability(String toString) {
            this.toString = toString;
        }

        Capability() {
            this.toString = name();
        }

        public String toString() {
            return toString;
        }

    }

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

    /**
     *
     * @param sourceType
     * @param targetType
     * @return
     */
    public boolean supportsConvert(int sourceType, int targetType);

    public boolean supportsFormatLiteral(String literal, Format format);

}
