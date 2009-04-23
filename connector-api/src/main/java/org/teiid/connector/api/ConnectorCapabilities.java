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

package org.teiid.connector.api;

import java.util.List;

import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.IBulkInsert;


/**
 * Allows a connector to specify the capabilities that a connector
 * supports.    
 */
public interface ConnectorCapabilities {

    /** 
     * Support indicates connector can accept queries with SELECT DISTINCT
     * @since 3.1 SP2 
     */
    boolean supportsSelectDistinct();

    /** 
     * Support indicates connector can accept literals in the SELECT clause 
     * @since 4.1.2
     */
    boolean supportsSelectLiterals();

    /**
     * Support indicates connector can accept groups with aliases  
     * @since 3.1 SP2
     */
    boolean supportsAliasedGroup();
    
    /** 
     * Support indicates connector can accept joins
     * @since 3.1 SP2 
     */
    boolean supportsJoins();
    
    /** 
     * Support indicates connector can accept self-joins where a 
     * group is joined to itself with aliases.  Connector must also support
     * {@link #supportsAliasedGroup()}. 
     * @since 3.1 SP2
     */
    boolean supportsSelfJoins();
    
    /** 
     * Support indicates connector can accept right or left outer joins 
     * @since 3.1 SP2
     */
    boolean supportsOuterJoins();
    
    /** 
     * Support indicates connector can accept full outer joins
     * @since 3.1 SP2 
     */
    boolean supportsFullOuterJoins();

    /** 
     * Support indicates connector can accept inline views (subqueries
     * in the FROM clause).  
     * @since 4.1 
     */
    boolean supportsInlineViews();

    /** 
     * Support indicates connector can accept a WHERE criteria on queries
     * @since 3.1 SP2 
     */
    boolean supportsCriteria();

    /** 
     * Support indicates connector accepts criteria of form (element BETWEEN constant AND constant) 
     * @since 4.0
     */
    boolean supportsBetweenCriteria();
    
    /** 
     * Support indicates connector accepts criteria of form (element operator constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteria();

    /** 
     * Support indicates connector accepts criteria of form (element = constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaEquals();

    /** 
     * Support indicates connector accepts criteria of form (element &lt;&gt; constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaNotEquals();

    /** 
     * Support indicates connector accepts criteria of form (element &lt; constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaLessThan();

    /** 
     * Support indicates connector accepts criteria of form (element &lt;= constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaLessThanOrEqual();

    /** 
     * Support indicates connector accepts criteria of form (element &gt; constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaGreaterThan();

    /** 
     * Support indicates connector accepts criteria of form (element &gt;= constant) 
     * @since 3.1 SP2
     */
    boolean supportsCompareCriteriaGreaterThanOrEqual();

    /** 
     * Support indicates connector accepts criteria of form (element LIKE constant) 
     * @since 3.1 SP2
     */
    boolean supportsLikeCriteria();
    
    /** 
     * Support indicates connector accepts criteria of form (element LIKE constant ESCAPE char) - CURRENTLY NOT USED 
     * @since 3.1 SP2
     */
    boolean supportsLikeCriteriaEscapeCharacter();

    /** 
     * Support indicates connector accepts criteria of form (element IN set) 
     * @since 3.1 SP2
     */
    boolean supportsInCriteria();

    /** 
     * Support indicates connector accepts IN criteria with a subquery on the right side 
     * @since 4.0
     */
    boolean supportsInCriteriaSubquery();

    /** 
     * Support indicates connector accepts criteria of form (element IS NULL) 
     * @since 3.1 SP2
     */
    boolean supportsIsNullCriteria();

    /** 
     * Support indicates connector accepts logical criteria connected by AND 
     * @since 3.1 SP2
     */
    boolean supportsAndCriteria();

    /** 
     * Support indicates connector accepts logical criteria connected by OR 
     * @since 3.1 SP2
     */
    boolean supportsOrCriteria();

    /** 
     * Support indicates connector accepts logical criteria NOT 
     * @since 3.1 SP2
     */
    boolean supportsNotCriteria();

    /** 
     * Support indicates connector accepts the EXISTS criteria 
     * @since 4.0
     */
    boolean supportsExistsCriteria();

    /** 
     * Support indicates connector accepts quantified subquery comparison criteria
     * @since 4.0
     */
    boolean supportsQuantifiedCompareCriteria();

    /** 
     * Support indicates connector accepts the quantified comparison criteria that 
     * use SOME
     * @since 4.0
     */
    boolean supportsQuantifiedCompareCriteriaSome();

    /** 
     * Support indicates connector accepts the quantified comparison criteria that 
     * use ALL
     * @since 4.0
     */
    boolean supportsQuantifiedCompareCriteriaAll();

    /** 
     * Support indicates connector accepts ORDER BY clause, including multiple elements
     * and ascending and descending sorts.    
     * @since 3.1 SP2
     */
    boolean supportsOrderBy();

    /** 
     * Support indicates connector accepts GROUP BY and HAVING clauses as well as 
     * aggregate functions in the SELECT clause. 
     * @since 3.1 SP2
     */
    boolean supportsAggregates();

    /** 
     * Support indicates connector can accept the SUM aggregate function 
     * @since 3.1 SP2
     */
    boolean supportsAggregatesSum();
    
    /** 
     * Support indicates connector can accept the AVG aggregate function
     * @since 3.1 SP2 
     */
    boolean supportsAggregatesAvg();
    
    /** 
     * Support indicates connector can accept the MIN aggregate function 
     * @since 3.1 SP2
     */
    boolean supportsAggregatesMin();
    
    /** 
     * Support indicates connector can accept the MAX aggregate function 
     * @since 3.1 SP2
     */
    boolean supportsAggregatesMax();
    
    /** 
     * Support indicates connector can accept the COUNT aggregate function
     * @since 3.1 SP2 
     */
    boolean supportsAggregatesCount();
    
    /** 
     * Support indicates connector can accept the COUNT(*) aggregate function 
     * @since 3.1 SP2
     */
    boolean supportsAggregatesCountStar();
    
    /** 
     * Support indicates connector can accept DISTINCT within aggregate functions 
     * @since 3.1 SP2
     */
    boolean supportsAggregatesDistinct();

    /** 
     * Support indicates connector can accept scalar subqueries in the SELECT, WHERE, and
     * HAVING clauses
     * @since 4.0
     */
    boolean supportsScalarSubqueries();

    /** 
     * Support indicates connector can accept correalted subqueries wherever subqueries
     * are accepted 
     * @since 4.0
     */
    boolean supportsCorrelatedSubqueries();
    
    /**
     * Support indicates connector can accept queries with non-searched
     * CASE <expression> WHEN <expression> ... END
     * @since 4.0
     */
    boolean supportsCaseExpressions();

    /**
     * Support indicates connector can accept queries with searched CASE WHEN <criteria> ... END
     * @since 4.0
     */
    boolean supportsSearchedCaseExpressions();
   
    /**
     * Support indicates that the connector supports scalar functions.
     * @since 3.1 SP3
     */ 
    boolean supportsScalarFunctions();

    /**
     * Support indicates that the connector supports the UNION of two queries. 
     * @since 4.2
     */
    boolean supportsUnions();

    /**
     * Support indicates that the connector supports an ORDER BY on a SetQuery. 
     * @since 5.6
     */
    boolean supportsSetQueryOrderBy();
    
    /**
     * Support indicates that the connector supports the INTERSECT of two queries. 
     * @since 5.6
     */
    boolean supportsIntersect();

    /**
     * Support indicates that the connector supports the EXCEPT of two queries. 
     * @since 5.6
     */
    boolean supportsExcept();
        
    /**
     * Get list of all supported function names.  Arithmetic functions have names like
     * &quot;+&quot;.  
     * @since 3.1 SP3    
     */        
    List<String> getSupportedFunctions();
    
    /**
     * Get the integer value representing the number of values allowed in an IN criteria
     * in the WHERE clause of a query
     * @since 5.0
     */
    int getMaxInCriteriaSize();
    
    /**
     * <p>Support indicates that the connector supports functions in GROUP BY, such as:
     *  <code>SELECT dayofmonth(theDate), COUNT(*) FROM table GROUP BY dayofmonth(theDate)</code></p>
     *  
     * <p>This capability requires both {@link #supportsAggregates()} and 
     * {@link #supportsScalarFunctions()} to be true as well to take effective.</p>
     * 
     * @since 5.0
     */
    boolean supportsFunctionsInGroupBy();
    
    /**
     * Gets whether the connector can limit the number of rows returned by a query.
     * @since 5.0 SP1
     */
    boolean supportsRowLimit();
    
    /**
     * Gets whether the connector supports a SQL clause (similar to the LIMIT with an offset) that can return
     * result sets that start in the middle of the resulting rows returned by a query
     * @since 5.0 SP1
     */
    boolean supportsRowOffset();
    
    /**
     * The number of groups supported in the from clause.  Added for a Sybase limitation. 
     * @since 5.6
     * @return the number of groups supported in the from clause, or -1 if there is no limit
     */
    int getMaxFromGroups();
    
    /**
     * Whether the source prefers to use ANSI style joins.
     * @since 6.0
     */
    boolean useAnsiJoin();
    
    /**
     * Whether the source supports queries without criteria.
     * @since 6.0
     */
    boolean requiresCriteria();
    
    /**
     * Whether the source supports {@link IBatchedUpdates}
     * @since 6.0
     */
    boolean supportsBatchedUpdates();
    
    /**
     * Whether the source supports {@link IBulkInsert}
     * @since 6.0
     */
    boolean supportsBulkInsert();
    
    /**
     * Whether the source supports an explicit GROUP BY clause
     * @since 6.0
     */
    boolean supportsGroupBy();
    
}
