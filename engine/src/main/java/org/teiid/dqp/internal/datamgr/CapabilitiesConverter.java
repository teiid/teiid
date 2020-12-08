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

package org.teiid.dqp.internal.datamgr;

import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.QueryPlugin;
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
        tgtCaps.setTranslator(srcCaps);
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, srcCaps.supportsSelectExpression());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, srcCaps.supportsSelectDistinct());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, srcCaps.supportsAliasedTable());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, srcCaps.supportsInnerJoins());
        setSupports(connectorID, tgtCaps, Capability.QUERY_FROM_JOIN_SELFJOIN, srcCaps.supportsSelfJoins(), Capability.QUERY_FROM_GROUP_ALIAS);
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, srcCaps.supportsOuterJoins());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, srcCaps.supportsFullOuterJoins());
        setSupports(connectorID, tgtCaps, Capability.QUERY_FROM_INLINE_VIEWS, srcCaps.supportsInlineViews(), Capability.QUERY_FROM_GROUP_ALIAS);
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_IS_DISTINCT, srcCaps.supportsIsDistinctCriteria());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, srcCaps.supportsCompareCriteriaEquals());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, srcCaps.supportsCompareCriteriaOrdered());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_LIKE, srcCaps.supportsLikeCriteria());
        setSupports(connectorID, tgtCaps, Capability.CRITERIA_LIKE_ESCAPE, srcCaps.supportsLikeCriteriaEscapeCharacter(), Capability.CRITERIA_LIKE);
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_IN, srcCaps.supportsInCriteria() || (srcCaps.supportsCompareCriteriaEquals() && srcCaps.supportsOrCriteria()));
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
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_BIG, srcCaps.supportsAggregatesCountBig());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, srcCaps.supportsAggregatesCountStar());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, srcCaps.supportsAggregatesDistinct());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, srcCaps.supportsScalarSubqueries());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, srcCaps.supportsCorrelatedSubqueries());
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
        tgtCaps.setCapabilitySupport(Capability.QUERY_ONLY_SINGLE_TABLE_GROUP_BY, srcCaps.supportsOnlySingleTableGroupBy());
        tgtCaps.setCapabilitySupport(Capability.QUERY_HAVING, srcCaps.supportsHaving());
        tgtCaps.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, srcCaps.supportsInsertWithQueryExpression());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, srcCaps.supportsOrderByUnrelated());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_ENHANCED_NUMERIC, srcCaps.supportsAggregatesEnhancedNumeric());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, srcCaps.supportsOrderByNullOrdering());
        tgtCaps.setCapabilitySupport(Capability.INSERT_WITH_ITERATOR, srcCaps.supportsBulkUpdate());
        tgtCaps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, srcCaps.supportsCommonTableExpressions());
        setSupports(connectorID, tgtCaps, Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS, srcCaps.supportsRecursiveCommonTableExpressions(), Capability.COMMON_TABLE_EXPRESSIONS);
        tgtCaps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, srcCaps.supportsElementaryOlapOperations());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WINDOW_FUNCTION_NTILE, srcCaps.supportsWindowFunctionNtile());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WINDOW_FUNCTION_PERCENT_RANK, srcCaps.supportsWindowFunctionPercentRank());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WINDOW_FUNCTION_CUME_DIST, srcCaps.supportsWindowFunctionCumeDist());
        tgtCaps.setCapabilitySupport(Capability.QUERY_WINDOW_FUNCTION_NTH_VALUE, srcCaps.supportsWindowFunctionNthValue());
        tgtCaps.setCapabilitySupport(Capability.WINDOW_FUNCTION_FRAME_CLAUSE, srcCaps.supportsWindowFrameClause());
        setSupports(connectorID, tgtCaps, Capability.ADVANCED_OLAP, srcCaps.supportsAdvancedOlapOperations(), Capability.ELEMENTARY_OLAP);
        setSupports(connectorID, tgtCaps, Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, srcCaps.supportsWindowOrderByWithAggregates(), Capability.ELEMENTARY_OLAP);
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_ARRAY, srcCaps.supportsArrayAgg());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_SIMILAR, srcCaps.supportsSimilarTo());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_LIKE_REGEX, srcCaps.supportsLikeRegex());
        setSupports(connectorID, tgtCaps, Capability.WINDOW_FUNCTION_DISTINCT_AGGREGATES, srcCaps.supportsWindowDistinctAggregates(), Capability.ELEMENTARY_OLAP, Capability.QUERY_AGGREGATES_DISTINCT);
        tgtCaps.setCapabilitySupport(Capability.ONLY_FORMAT_LITERALS, srcCaps.supportsOnlyFormatLiterals());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_ONLY_LITERAL_COMPARE, srcCaps.supportsOnlyLiteralComparison());
        tgtCaps.setCapabilitySupport(Capability.DEPENDENT_JOIN, srcCaps.supportsDependentJoins());
        tgtCaps.setCapabilitySupport(Capability.FULL_DEPENDENT_JOIN, srcCaps.supportsFullDependentJoins());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, srcCaps.supportsSubqueryInOn());
        tgtCaps.setCapabilitySupport(Capability.ARRAY_TYPE, srcCaps.supportsArrayType());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION_ARRAY_TYPE, srcCaps.supportsSelectExpressionArrayType());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_ONLY_CORRELATED, srcCaps.supportsOnlyCorrelatedSubqueries());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_STRING, srcCaps.supportsStringAgg());
        tgtCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_LIST, srcCaps.supportsListAgg());
        tgtCaps.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, srcCaps.supportsSelectWithoutFrom());
        tgtCaps.setCapabilitySupport(Capability.QUERY_GROUP_BY_ROLLUP, srcCaps.supportsGroupByRollup());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ORDERBY_EXTENDED_GROUPING, srcCaps.supportsOrderByWithExtendedGrouping());
        tgtCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED_EXCLUSIVE, srcCaps.supportsCompareCriteriaOrderedExclusive());
        tgtCaps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, srcCaps.supportsSubqueryCommonTableExpressions());
        tgtCaps.setCapabilitySupport(Capability.SUBQUERY_CORRELATED_LIMIT, srcCaps.supportsCorrelatedSubqueryLimit());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, srcCaps.supportsScalarSubqueryProjection());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL, srcCaps.supportsLateralJoin());
        setSupports(connectorID, tgtCaps, Capability.QUERY_FROM_JOIN_LATERAL_CONDITION, srcCaps.supportsLateralJoinCondition(), Capability.QUERY_FROM_JOIN_LATERAL);
        tgtCaps.setCapabilitySupport(Capability.QUERY_ONLY_FROM_JOIN_LATERAL_PROCEDURE, srcCaps.supportsOnlyLateralJoinProcedure());
        tgtCaps.setCapabilitySupport(Capability.QUERY_FROM_PROCEDURE_TABLE, srcCaps.supportsProcedureTable());
        tgtCaps.setCapabilitySupport(Capability.QUERY_GROUP_BY_MULTIPLE_DISTINCT_AGGREGATES, srcCaps.supportsGroupByMultipleDistinctAggregates());
        tgtCaps.setCapabilitySupport(Capability.UPSERT, srcCaps.supportsUpsert());
        tgtCaps.setCapabilitySupport(Capability.QUERY_SET_LIMIT_OFFSET, srcCaps.supportsSetQueryLimitOffset());
        tgtCaps.setCapabilitySupport(Capability.ONLY_TIMESTAMPADD_LITERAL, srcCaps.supportsOnlyTimestampAddLiteral());
        tgtCaps.setCapabilitySupport(Capability.GEOGRAPHY_TYPE, srcCaps.supportsGeographyType());
        tgtCaps.setCapabilitySupport(Capability.PROCEDURE_PARAMETER_EXPRESSION, srcCaps.supportsProcedureParameterExpression());
        tgtCaps.setCapabilitySupport(Capability.QUERY_ONLY_FROM_RELATIONSHIP_JOIN, srcCaps.supportsOnlyRelationshipStyleJoins());
        if (srcCaps.supportsPartialFiltering()) {
            //disable supports that could end up being not filterable
            tgtCaps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
            Assertion.assertTrue(!srcCaps.supportsOuterJoins());
            Assertion.assertTrue(!srcCaps.supportsFullOuterJoins());
            Assertion.assertTrue(!srcCaps.supportsInlineViews());
            Assertion.assertTrue(!srcCaps.supportsIntersect());
            Assertion.assertTrue(!srcCaps.supportsExcept());
            Assertion.assertTrue(!srcCaps.supportsSelectExpression());
            Assertion.assertTrue(!srcCaps.supportsUnions());
            Assertion.assertTrue(!srcCaps.supportsSelectDistinct());
            Assertion.assertTrue(!srcCaps.supportsGroupBy());
        }

        List<String> functions = srcCaps.getSupportedFunctions();
        if(functions != null && functions.size() > 0) {
            Iterator<String> iter = functions.iterator();
            while(iter.hasNext()) {
                String func = iter.next();
                tgtCaps.setFunctionSupport(func, true);
            }
        }

        List<FunctionMethod> pushDowns = srcCaps.getPushDownFunctions();
        if(pushDowns != null && pushDowns.size() > 0) {
            for(FunctionMethod func:pushDowns) {
                tgtCaps.setFunctionSupport(func.getName(), true);
            }
        }

        tgtCaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(srcCaps.getMaxInCriteriaSize()));
        tgtCaps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, new Integer(srcCaps.getMaxDependentInPredicates()));
        tgtCaps.setSourceProperty(Capability.CONNECTOR_ID, connectorID);
        tgtCaps.setSourceProperty(Capability.MAX_QUERY_FROM_GROUPS, new Integer(srcCaps.getMaxFromGroups()));
        tgtCaps.setSourceProperty(Capability.MAX_QUERY_PROJECTED_COLUMNS, srcCaps.getMaxProjectedColumns());
        tgtCaps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, srcCaps.getSupportedJoinCriteria());
        tgtCaps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, srcCaps.getDefaultNullOrder());
        tgtCaps.setSourceProperty(Capability.COLLATION_LOCALE, srcCaps.getCollationLocale());
        tgtCaps.setSourceProperty(Capability.REQUIRED_LIKE_ESCAPE, srcCaps.getRequiredLikeEscape());
        tgtCaps.setSourceProperty(Capability.TRANSACTION_SUPPORT, srcCaps.getTransactionSupport());
        tgtCaps.setSourceProperty(Capability.EXCLUDE_COMMON_TABLE_EXPRESSION_NAME, srcCaps.getExcludedCommonTableExpressionName());
        return tgtCaps;
    }

    private static void setSupports(Object connectorID, BasicSourceCapabilities tgtCaps, Capability cap, boolean supports, Capability... required) {
        if (!supports) {
            return;
        }
        for (Capability capability : required) {
            if (!tgtCaps.supportsCapability(capability)) {
                LogManager.logWarning(LogConstants.CTX_CONNECTOR, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30003, cap, capability, connectorID));
                supports = false;
            }
        }
        tgtCaps.setCapabilitySupport(cap, supports);
    }

}
