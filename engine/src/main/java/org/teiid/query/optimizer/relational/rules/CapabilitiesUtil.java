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

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Schema;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.translator.ExecutionFactory.NullOrder;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;


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
        	return caps.supportsCapability(Capability.QUERY_FROM_JOIN_INNER);
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

            // Also verify that if there is a function that we can support pushdown of functions in group by
            Iterator colIter = groupCols.iterator();
            while(colIter.hasNext()) {
                Expression col = (Expression) colIter.next();
                if(!(col instanceof ElementSymbol) && !supportsFunctionsInGroupBy) {
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
        case COUNT:
            if(aggregate.getExpression() == null) {
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
        
        // Passed all the checks!
        return true;
        

    }

    public static boolean supportsScalarFunction(Object modelID, Function function, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, TeiidComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }
        
        //capabilities check is only valid for non-schema scoped functions
        //technically the other functions are scoped to SYS, but that's 
        //not formally part of their metadata yet
        Schema schema = function.getFunctionDescriptor().getMethod().getParent();
        if (schema == null) {
            // Find capabilities
            SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

            if (!caps.supportsFunction(function.getFunctionDescriptor().getName().toLowerCase())) {
                return false;
            }
        } else if (!schema.getFullName().equalsIgnoreCase(metadata.getFullName(modelID))) {
        	return false; //not the right schema
        }
        
        //special check to ensure that special conversions are not pushed down (this can be removed after we support type based function pushdown)            
        if (FunctionLibrary.isConvert(function)) {
            Class fromType = function.getArg(0).getType();
            //object or clob to anything cannot be pushed down
            if (DataTypeManager.DefaultDataClasses.OBJECT.equals(fromType) 
                            || DataTypeManager.DefaultDataClasses.CLOB.equals(fromType)
                            || DataTypeManager.DefaultDataClasses.XML.equals(fromType)) {
                return false;                
            }
            String targetType = (String)((Constant)function.getArg(1)).getValue();
            if (DataTypeManager.DefaultDataTypes.CLOB.equalsIgnoreCase(targetType) 
                            || DataTypeManager.DefaultDataTypes.XML.equalsIgnoreCase(targetType)) {
                return false;                
            }
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
    	return getProperty(Capability.MAX_IN_CRITERIA_SIZE, modelID, metadata, capFinder);
    }
    	
    private static int getProperty(Capability cap, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return -1;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        Object maxInCriteriaSize = caps.getSourceProperty(cap);
        int value = -1;
        if(maxInCriteriaSize != null) {
            value = ((Integer)maxInCriteriaSize).intValue();
        }
        
        // Check for invalid values and send back code for UNKNOWN
        if(value <= 0) {
            value = -1;
        }
        return value;
    }
    
    public static int getMaxDependentPredicates(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, TeiidComponentException {
    	return getProperty(Capability.MAX_DEPENDENT_PREDICATES, modelID, metadata, capFinder);
    }
    
    public static int getMaxFromGroups(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, TeiidComponentException {

        if (metadata.isVirtualModel(modelID)){
            return -1;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        Object maxGroups = caps.getSourceProperty(Capability.MAX_QUERY_FROM_GROUPS);
        int value = -1;
        if(maxGroups != null) {
            value = ((Integer)maxGroups).intValue();
        }
        
        // Check for invalid values and send back code for UNKNOWN
        if(value <= 0) {
            value = -1;
        }
        return value;
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

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        SourceCapabilities caps1 = getCapabilities(modelID1, metadata, capFinder);
        
        Object connectorID = caps.getSourceProperty(Capability.CONNECTOR_ID);
        
        return connectorID != null && connectorID.equals(caps1.getSourceProperty(Capability.CONNECTOR_ID));
    }

    private static SourceCapabilities getCapabilities(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
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

        return caps.supportsCapability(cap);
    }

	/**
	 * Validate that the elements are searchable and can be used in a criteria against this source.
	 * TODO: this check is too general and not type based
	 */
	static boolean checkElementsAreSearchable(List<? extends LanguageObject> objs, QueryMetadataInterface metadata, int searchableType) 
	throws QueryMetadataException, TeiidComponentException {
	    Collection<ElementSymbol> elements = new ArrayList<ElementSymbol>();
	    ElementCollectorVisitor.getElements(objs, elements);
	    for (ElementSymbol elementSymbol : elements) {
	        if (!metadata.elementSupports(elementSymbol.getMetadataID(), searchableType)) {
	        	return false;
	        }                
	    }
	    return true;
	}
}
