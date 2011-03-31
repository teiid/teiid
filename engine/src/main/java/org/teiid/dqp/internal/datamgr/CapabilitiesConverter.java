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

package org.teiid.dqp.internal.datamgr;

import java.util.Iterator;
import java.util.List;

import org.teiid.metadata.FunctionMethod;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.translator.ExecutionFactory;


/**
 * Utility to convert a ConnectorCapabilities class into a Map of 
 * capabilities information that can be passed through the system.
 */
public class CapabilitiesConverter {

    private CapabilitiesConverter() {
    }

    public static SourceCapabilities convertCapabilities(ExecutionFactory srcCaps) {
        return convertCapabilities(srcCaps, null);
    }
    
    public static BasicSourceCapabilities convertCapabilities(ExecutionFactory srcCaps, Object connectorID) {
        BasicSourceCapabilities tgtCaps = new BasicSourceCapabilities();
        
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, srcCaps.supportsSelectExpression());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, srcCaps.supportsSelectDistinct());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, srcCaps.supportsAliasedTable());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, srcCaps.supportsInnerJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, srcCaps.supportsSelfJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, srcCaps.supportsOuterJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, srcCaps.supportsFullOuterJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, srcCaps.supportsInlineViews());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, srcCaps.supportsCompareCriteriaEquals());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, srcCaps.supportsCompareCriteriaOrdered());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_LIKE, srcCaps.supportsLikeCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, srcCaps.supportsLikeCriteriaEscapeCharacter());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_IN, srcCaps.supportsInCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, srcCaps.supportsInCriteriaSubquery());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_ISNULL, srcCaps.supportsIsNullCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_OR, srcCaps.supportsOrCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_NOT, srcCaps.supportsNotCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_EXISTS, srcCaps.supportsExistsCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, srcCaps.supportsQuantifiedCompareCriteriaSome());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, srcCaps.supportsQuantifiedCompareCriteriaAll());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, srcCaps.supportsOrderBy());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, srcCaps.supportsAggregatesSum());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, srcCaps.supportsAggregatesAvg());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, srcCaps.supportsAggregatesMin());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, srcCaps.supportsAggregatesMax());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, srcCaps.supportsAggregatesCount());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, srcCaps.supportsAggregatesCountStar());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, srcCaps.supportsAggregatesDistinct());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, srcCaps.supportsScalarSubqueries());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, srcCaps.supportsCorrelatedSubqueries());
        tgtCaps.setCapabilitySupport(Capability.QUERY_CASE, srcCaps.supportsCaseExpressions());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, srcCaps.supportsSearchedCaseExpressions());
        tgtCaps.setCapabilitySupport(Capability.QUERY_UNION, srcCaps.supportsUnions());
        tgtCaps.setCapabilitySupport(Capability.QUERY_INTERSECT, srcCaps.supportsIntersect());
        tgtCaps.setCapabilitySupport(Capability.QUERY_EXCEPT, srcCaps.supportsExcept());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, srcCaps.supportsSetQueryOrderBy());
        tgtCaps.setCapabilitySupport(Capability.BULK_UPDATE , srcCaps.supportsBulkUpdate());
        tgtCaps.setCapabilitySupport(Capability.BATCHED_UPDATES, srcCaps.supportsBatchedUpdates());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, srcCaps.supportsFunctionsInGroupBy());
        tgtCaps.setCapabilitySupport(Capability.ROW_LIMIT, srcCaps.supportsRowLimit());
        tgtCaps.setCapabilitySupport(Capability.ROW_OFFSET, srcCaps.supportsRowOffset());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, srcCaps.useAnsiJoin());
        tgtCaps.setCapabilitySupport(Capability.REQUIRES_CRITERIA, srcCaps.requiresCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_GROUP_BY, srcCaps.supportsGroupBy());
        tgtCaps.setCapabilitySupport(Capability.QUERY_HAVING, srcCaps.supportsHaving());
        tgtCaps.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, srcCaps.supportsInsertWithQueryExpression());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, srcCaps.supportsOrderByUnrelated());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_ENHANCED_NUMERIC, srcCaps.supportsAggregatesEnhancedNumeric());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, srcCaps.supportsOrderByNullOrdering());
        tgtCaps.setCapabilitySupport(Capability.INSERT_WITH_ITERATOR, srcCaps.supportsInsertWithIterator());
        tgtCaps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, srcCaps.supportsCommonTableExpressions());
        List functions = srcCaps.getSupportedFunctions();
        if(functions != null && functions.size() > 0) {
            Iterator iter = functions.iterator();
            while(iter.hasNext()) {
                String func = (String) iter.next();
                tgtCaps.setFunctionSupport(func.toLowerCase(), true);
            }
        }
        List<FunctionMethod> pushDowns = srcCaps.getPushDownFunctions();
        if(pushDowns != null && pushDowns.size() > 0) {
            for(FunctionMethod func:pushDowns) {
                tgtCaps.setFunctionSupport(func.getName().toLowerCase(), true);
            }
        }
        
        tgtCaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(srcCaps.getMaxInCriteriaSize()));
        tgtCaps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, new Integer(srcCaps.getMaxDependentInPredicates()));
        tgtCaps.setSourceProperty(Capability.CONNECTOR_ID, connectorID);
        tgtCaps.setSourceProperty(Capability.MAX_QUERY_FROM_GROUPS, new Integer(srcCaps.getMaxFromGroups()));
        tgtCaps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, srcCaps.getSupportedJoinCriteria());
        tgtCaps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, srcCaps.getDefaultNullOrder());
        return tgtCaps;
    }

}
