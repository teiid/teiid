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

package org.teiid.query.optimizer.relational.rules;

import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.Schema;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.translator.ExecutionFactory.NullOrder;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;
import org.teiid.translator.SourceSystemFunctions;


/**
 */
public class CapabilitiesUtil {

    /**
     * Can't construct - just a utilities class
     */
    private CapabilitiesUtil() {
    }

    static boolean supportsInlineView(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
        throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_FROM_INLINE_VIEWS, modelID, metadata, capFinder);
    }

    public static boolean supportsSelfJoins(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_FROM_JOIN_SELFJOIN) &&
                caps.supportsCapability(Capability.QUERY_FROM_GROUP_ALIAS);
    }

    public static boolean supportsGroupAliases(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
        throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_FROM_GROUP_ALIAS, modelID, metadata, capFinder);
    }

    public static boolean supportsJoin(Object modelID, JoinType joinType, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if (!joinType.isOuter()) {
            return caps.supportsCapability(Capability.QUERY_FROM_JOIN_INNER) || caps.supportsCapability(Capability.QUERY_FROM_JOIN_OUTER);
        }

        if(! caps.supportsCapability(Capability.QUERY_FROM_JOIN_OUTER)) {
            return false;
        }

        return !joinType.equals(JoinType.JOIN_FULL_OUTER) || caps.supportsCapability(Capability.QUERY_FROM_JOIN_OUTER_FULL);
    }

    public static boolean supportsAggregates(List groupCols, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if (groupCols != null && !groupCols.isEmpty()) {
            if (!caps.supportsCapability(Capability.QUERY_GROUP_BY)) {
                return false;
            }
            boolean supportsFunctionsInGroupBy = caps.supportsCapability(Capability.QUERY_FUNCTIONS_IN_GROUP_BY);
            boolean supportsInlineView = caps.supportsCapability(Capability.QUERY_FROM_INLINE_VIEWS);

            // Also verify that if there is a function that we can support pushdown of functions in group by
            Iterator colIter = groupCols.iterator();
            while(colIter.hasNext()) {
                Expression col = (Expression) colIter.next();
                if(!(col instanceof ElementSymbol) && !supportsFunctionsInGroupBy && !supportsInlineView) {
                    // Function in GROUP BY can't be pushed
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean supportsAggregateFunction(Object modelID, AggregateSymbol aggregate, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        // Check particular function
        Type func = aggregate.getAggregateFunction();
        switch (func) {
        case COUNT_BIG:
        case COUNT:
            if(aggregate.getArgs().length == 0) {
                if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_COUNT_STAR)) {
                    return false;
                }
            } else {
                if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_COUNT)) {
                    return false;
                }
            }
            break;
        case SUM:
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_SUM)) {
                return false;
            }
            break;
        case AVG:
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_AVG)) {
                return false;
            }
            break;
        case MIN:
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_MIN)) {
                return false;
            }
            break;
        case MAX:
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_MAX)) {
                return false;
            }
            break;
        case ARRAY_AGG:
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_ARRAY)) {
                return false;
            }
            break;
        case STRING_AGG:
            if(!caps.supportsCapability(Capability.QUERY_AGGREGATES_STRING)) {
                //check for more specific list support
                if(aggregate.getType() == DataTypeManager.DefaultDataClasses.BLOB
                        || !caps.supportsCapability(Capability.QUERY_AGGREGATES_LIST)
                        || !EvaluatableVisitor.willBecomeConstant(aggregate.getArg(1))) {
                    return false;
                }
            }
            break;
        case RANK:
        case DENSE_RANK:
        case ROW_NUMBER:
        case FIRST_VALUE:
        case LAST_VALUE:
        case LEAD:
        case LAG:
            if (!caps.supportsCapability(Capability.ELEMENTARY_OLAP)) {
                return false;
            }
            break;
        case NTILE:
            if (!caps.supportsCapability(Capability.QUERY_WINDOW_FUNCTION_NTILE)) {
                return false;
            }
            break;
        case PERCENT_RANK:
            if (!caps.supportsCapability(Capability.QUERY_WINDOW_FUNCTION_PERCENT_RANK)) {
                return false;
            }
            break;
        case CUME_DIST:
            if (!caps.supportsCapability(Capability.QUERY_WINDOW_FUNCTION_CUME_DIST)) {
                return false;
            }
            break;
        case NTH_VALUE:
            if (!caps.supportsCapability(Capability.QUERY_WINDOW_FUNCTION_NTH_VALUE)) {
                return false;
            }
            break;
        case USER_DEFINED:
            if (!supportsScalarFunction(modelID, aggregate, metadata, capFinder)) {
                return false;
            }
            break;
        default:
            if (aggregate.isEnhancedNumeric()) {
                if (!caps.supportsCapability(Capability.QUERY_AGGREGATES_ENHANCED_NUMERIC)) {
                    return false;
                }
            } else {
                return false;
            }
            break;
        }

        // Check DISTINCT if necessary
        if(aggregate.isDistinct() && ! caps.supportsCapability(Capability.QUERY_AGGREGATES_DISTINCT)) {
            return false;
        }

        if (aggregate.getCondition() != null && !caps.supportsCapability(Capability.ADVANCED_OLAP)) {
            return false;
        }

        // Passed all the checks!
        return true;
    }

    public static boolean supportsScalarFunction(Object modelID, Function function, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        FunctionMethod method = function.getFunctionDescriptor().getMethod();
        if (metadata.isVirtualModel(modelID) || method.getPushdown() == PushDown.CANNOT_PUSHDOWN){
            return false;
        }

        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        //capabilities check is only valid for non-schema scoped functions
        //technically the other functions are scoped to SYS or their function model, but that's
        //not formally part of their metadata yet
        Schema schema = method.getParent();
        //TODO: this call should be functionDescriptor.getFullName - but legacy function models are parsed without setting the parent model as the schema
        String fullName = method.getFullName();
        if (schema == null || !schema.isPhysical()) {
            if (!caps.supportsFunction(fullName)) {
                if(SourceSystemFunctions.CONCAT2.equalsIgnoreCase(fullName)) {
                    //special handling for delayed rewrite of concat2
                    return (schema == null
                            && caps.supportsFunction(SourceSystemFunctions.CONCAT)
                            && caps.supportsFunction(SourceSystemFunctions.IFNULL)
                            && caps.supportsCapability(Capability.QUERY_SEARCHED_CASE));
                } else if(SourceSystemFunctions.FROM_UNIXTIME.equalsIgnoreCase(fullName)) {
                    return (schema == null
                            && caps.supportsFunction(SourceSystemFunctions.TIMESTAMPADD));
                } else if(SourceSystemFunctions.FROM_UNIXTIME.equalsIgnoreCase(fullName)) {
                    return (schema == null
                            && caps.supportsFunction(SourceSystemFunctions.TIMESTAMPDIFF));
                } else {
                    FunctionMethod functionMethod = metadata.getPushdownFunction(modelID, fullName);
                    if (functionMethod != null) {
                        //it's not great that we're setting this as a side-effect, but
                        //it's easier that attempting to re-associate at the connector
                        //or another level
                        function.setPushdownFunction(functionMethod);
                        return true;
                    }
                    return false ;
                }
            }
            if (FunctionLibrary.isConvert(function)) {
                Class<?> fromType = function.getArg(0).getType();
                Class<?> targetType = function.getType();
                if (fromType == targetType) {
                    return true; //this should be removed in rewrite
                }
                return caps.supportsConvert(DataTypeManager.getTypeCode(fromType), DataTypeManager.getTypeCode(targetType));
            }
            if (!caps.supportsCapability(Capability.GEOGRAPHY_TYPE)
                    && method.getCategory() != null
                    && method.getCategory().equals(FunctionCategoryConstants.GEOGRAPHY)) {
                //geometry functions can also accept geographies, but that type needs to be supported
                for (Expression ex : function.getArgs()) {
                    if (ex.getType() == DataTypeManager.DefaultDataClasses.GEOGRAPHY) {
                        return false;
                    }
                }
            }
        } else if (!isSameConnector(modelID, schema, metadata, capFinder)) {
            return caps.supportsFunction(fullName);
        }

        return true;
    }

    public static boolean supportsSelectDistinct(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_SELECT_DISTINCT, modelID, metadata, capFinder);
    }

    public static boolean supportsSelectExpression(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_SELECT_EXPRESSION, modelID, metadata, capFinder);
    }

    public static boolean supportsOrderBy(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_ORDERBY, modelID, metadata, capFinder);
    }

    public static boolean supportsSetOp(Object modelID, Operation setOp, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        switch (setOp) {
            case EXCEPT:
                return caps.supportsCapability(Capability.QUERY_EXCEPT);
            case INTERSECT:
                return caps.supportsCapability(Capability.QUERY_INTERSECT);
            case UNION:
                return caps.supportsCapability(Capability.QUERY_UNION);
        }

        return false;
    }

    public static boolean supportsSetQueryOrderBy(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_SET_ORDER_BY, modelID, metadata, capFinder);
    }

    public static boolean supportsCaseExpression(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_CASE, modelID, metadata, capFinder);
    }

    public static boolean supportsSearchedCaseExpression(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_SEARCHED_CASE, modelID, metadata, capFinder);
    }

    public static int getMaxInCriteriaSize(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return getIntProperty(Capability.MAX_IN_CRITERIA_SIZE, modelID, metadata, capFinder);
    }

    public static Object getProperty(Capability cap, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return null;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        return caps.getSourceProperty(cap);
    }

    /**
     * Values are expected to be non-negative except for unknown/invalid = -1
     * @param cap
     * @param modelID
     * @param metadata
     * @param capFinder
     * @return
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    public static int getIntProperty(Capability cap, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        Object i = getProperty(cap, modelID, metadata, capFinder);
        int value = -1;
        if(i != null) {
            value = ((Integer)i).intValue();
        }

        // Check for invalid values and send back code for UNKNOWN
        if(value <= 0) {
            value = -1;
        }
        return value;
    }

    public static int getMaxDependentPredicates(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return getIntProperty(Capability.MAX_DEPENDENT_PREDICATES, modelID, metadata, capFinder);
    }

    public static int getMaxFromGroups(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return getIntProperty(Capability.MAX_QUERY_FROM_GROUPS, modelID, metadata, capFinder);
    }

    public static int getMaxProjectedColumns(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return getIntProperty(Capability.MAX_QUERY_PROJECTED_COLUMNS, modelID, metadata, capFinder);
    }

    public static SupportedJoinCriteria getSupportedJoinCriteria(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {
        if (metadata.isVirtualModel(modelID)){
            return SupportedJoinCriteria.ANY;
        }
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        SupportedJoinCriteria crits = (SupportedJoinCriteria)caps.getSourceProperty(Capability.JOIN_CRITERIA_ALLOWED);
        if (crits == null) {
            return SupportedJoinCriteria.ANY;
        }
        return crits;
    }

    public static NullOrder getDefaultNullOrder(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {
        if (metadata.isVirtualModel(modelID)){
            return NullOrder.UNKNOWN;
        }
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        NullOrder order = (NullOrder)caps.getSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER);
        if (order == null) {
            return NullOrder.UNKNOWN;
        }
        return order;
    }

    public static boolean supportsRowLimit(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.ROW_LIMIT, modelID, metadata, capFinder);
    }

    public static boolean supportsRowOffset(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.ROW_OFFSET, modelID, metadata, capFinder);
    }

    public static boolean isSameConnector(Object modelID, Object modelID1, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {

        if (modelID == null || modelID1 == null || metadata.isVirtualModel(modelID) || metadata.isVirtualModel(modelID1)){
            return false;
        }

        if (modelID.equals(modelID1)) {
            return true;
        }

        if (capFinder == null) {
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        SourceCapabilities caps1 = getCapabilities(modelID1, metadata, capFinder);

        Object connectorID = caps.getSourceProperty(Capability.CONNECTOR_ID);

        return connectorID != null && connectorID.equals(caps1.getSourceProperty(Capability.CONNECTOR_ID));
    }

    static SourceCapabilities getCapabilities(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
        throws QueryMetadataException, TeiidComponentException {

        // Find capabilities
        String modelName = metadata.getFullName(modelID);
        return capFinder.findCapabilities(modelName);
    }

    public static boolean requiresCriteria(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.REQUIRES_CRITERIA, modelID, metadata, capFinder);
    }

    public static boolean useAnsiJoin(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        return supports(Capability.QUERY_FROM_ANSI_JOIN, modelID, metadata, capFinder);
    }

    public static boolean supports(Capability cap, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryMetadataException, TeiidComponentException {
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        if (caps == null) {
            throw new AssertionError(modelID);
        }
        return caps.supportsCapability(cap);
    }

    /**
     * Validate that the elements are searchable and can be used in a criteria against this source.
     * TODO: this check is too general and not type based
     */
    static boolean checkElementsAreSearchable(List<? extends LanguageObject> objs, QueryMetadataInterface metadata, int searchableType)
    throws QueryMetadataException, TeiidComponentException {
        if (objs != null) {
            for (LanguageObject lo : objs) {
                if (lo instanceof OrderByItem) {
                    lo = ((OrderByItem)lo).getSymbol();
                }
                if (!(lo instanceof Expression)) {
                    continue;
                }
                lo = SymbolMap.getExpression((Expression)lo);
                if (!(lo instanceof ElementSymbol)) {
                    continue;
                }
                if (!metadata.elementSupports(((ElementSymbol)lo).getMetadataID(), searchableType)) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean supportsNullOrdering(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, Object modelID, OrderByItem symbol)
            throws QueryMetadataException, TeiidComponentException {
        boolean supportsNullOrdering = CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_NULL_ORDERING, modelID, metadata, capFinder);
        NullOrder defaultNullOrder = CapabilitiesUtil.getDefaultNullOrder(modelID, metadata, capFinder);
        if (symbol.getNullOrdering() != null && !supportsNullOrdering) {
            if (symbol.getNullOrdering() == NullOrdering.FIRST) {
                if (defaultNullOrder != NullOrder.FIRST && !(symbol.isAscending() && defaultNullOrder == NullOrder.LOW)
                        && !(!symbol.isAscending() && defaultNullOrder == NullOrder.HIGH)) {
                    return false;
                }
            } else if (defaultNullOrder != NullOrder.LAST && !(symbol.isAscending() && defaultNullOrder == NullOrder.HIGH)
                    && !(!symbol.isAscending() && defaultNullOrder == NullOrder.LOW)) {
                return false;
            }
        }
        return true;
    }
}
