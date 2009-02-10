/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr;

import java.util.*;

import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;

/**
 * Utility to convert a ConnectorCapabilities class into a Map of 
 * capabilities information that can be passed through the system.
 */
public class CapabilitiesConverter {

    private CapabilitiesConverter() {
    }

    public static SourceCapabilities convertCapabilities(ConnectorCapabilities srcCaps) {
        return convertCapabilities(srcCaps, null, false);
    }
    
    public static BasicSourceCapabilities convertCapabilities(ConnectorCapabilities srcCaps, String connectorID, boolean isXa) {
        BasicSourceCapabilities tgtCaps = new BasicSourceCapabilities();
        
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, srcCaps.supportsSelectDistinct());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_LITERALS, srcCaps.supportsSelectLiterals());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, srcCaps.supportsAliasedGroup());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN, srcCaps.supportsJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, srcCaps.supportsSelfJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, srcCaps.supportsOuterJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, srcCaps.supportsFullOuterJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, srcCaps.supportsInlineViews());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE, srcCaps.supportsCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_BETWEEN, srcCaps.supportsBetweenCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, srcCaps.supportsCompareCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, srcCaps.supportsCompareCriteriaEquals());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_NE, srcCaps.supportsCompareCriteriaNotEquals());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LT, srcCaps.supportsCompareCriteriaLessThan());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LE, srcCaps.supportsCompareCriteriaLessThanOrEqual());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GT, srcCaps.supportsCompareCriteriaGreaterThan());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GE, srcCaps.supportsCompareCriteriaGreaterThanOrEqual());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, srcCaps.supportsLikeCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, srcCaps.supportsLikeCriteriaEscapeCharacter());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_IN, srcCaps.supportsInCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_IN_SUBQUERY, srcCaps.supportsInCriteriaSubquery());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, srcCaps.supportsIsNullCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_AND, srcCaps.supportsAndCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_OR, srcCaps.supportsOrCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_NOT, srcCaps.supportsNotCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_EXISTS, srcCaps.supportsExistsCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, srcCaps.supportsQuantifiedCompareCriteria());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, srcCaps.supportsQuantifiedCompareCriteriaSome());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_ALL, srcCaps.supportsQuantifiedCompareCriteriaAll());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, srcCaps.supportsOrderBy());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES, srcCaps.supportsAggregates());
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
        tgtCaps.setCapabilitySupport(Capability.BULK_INSERT , srcCaps.supportsBulkInsert());
        tgtCaps.setCapabilitySupport(Capability.BATCHED_UPDATES, srcCaps.supportsBatchedUpdates());
        tgtCaps.setCapabilitySupport(Capability.FUNCTION, srcCaps.supportsScalarFunctions());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, srcCaps.supportsFunctionsInGroupBy());
        tgtCaps.setCapabilitySupport(Capability.ROW_LIMIT, srcCaps.supportsRowLimit());
        tgtCaps.setCapabilitySupport(Capability.ROW_OFFSET, srcCaps.supportsRowOffset());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, srcCaps.useAnsiJoin());
        tgtCaps.setCapabilitySupport(Capability.REQUIRES_CRITERIA, srcCaps.requiresCriteria());

        List functions = srcCaps.getSupportedFunctions();
        if(functions != null && functions.size() > 0) {
            Iterator iter = functions.iterator();
            while(iter.hasNext()) {
                String func = (String) iter.next();
                tgtCaps.setFunctionSupport(func.toLowerCase(), true);
            }
        }
        
        tgtCaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(srcCaps.getMaxInCriteriaSize()));
        tgtCaps.setSourceProperty(Capability.CONNECTOR_ID, connectorID);
        tgtCaps.setSourceProperty(Capability.MAX_QUERY_FROM_GROUPS, new Integer(srcCaps.getMaxFromGroups()));
        tgtCaps.setSourceProperty(Capability.TRANSACTIONS_XA, isXa);
        return tgtCaps;
    }

}
