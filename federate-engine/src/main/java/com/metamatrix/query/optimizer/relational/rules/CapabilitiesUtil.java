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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;

/**
 */
public class CapabilitiesUtil {

    /**
     * Can't construct - just a utilities class
     */
    private CapabilitiesUtil() {
    }
    
    static boolean supportsInlineView(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
        throws QueryMetadataException, MetaMatrixComponentException {
            
        if (metadata.isVirtualModel(modelID)){
            return false;
        }
        
        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        
        return caps.supportsCapability(Capability.QUERY_FROM_INLINE_VIEWS);
    }
    
    public static boolean supportsJoins(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.QUERY_FROM_JOIN)) {
            return false;
        }
        
        // Capabilities checked out or didn't exist - check against metadata
        return metadata.modelSupports(modelID, SupportConstants.Model.JOIN);
    }

    public static boolean supportsSelfJoins(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
                
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
  
        return caps.supportsCapability(Capability.QUERY_FROM_JOIN_SELFJOIN) &&
                caps.supportsCapability(Capability.QUERY_FROM_GROUP_ALIAS);
    }

    public static boolean supportsGroupAliases(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
        throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
  
        return caps.supportsCapability(Capability.QUERY_FROM_GROUP_ALIAS);
    }
        
    public static boolean supportsOuterJoin(Object modelID, JoinType joinType, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.QUERY_FROM_JOIN_OUTER)) {
            return false;
        }
        
        if(joinType.equals(JoinType.JOIN_FULL_OUTER) && ! caps.supportsCapability(Capability.QUERY_FROM_JOIN_OUTER_FULL)) {
            return false;                     
        }
        
        // Capabilities checked out or didn't exist - check against metadata
        return metadata.modelSupports(modelID, SupportConstants.Model.OUTER_JOIN);
    }

    public static boolean supportsAggregates(List groupCols, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        
        if (!caps.supportsCapability(Capability.QUERY_AGGREGATES)) {
            return false;
        }
        
        boolean supportsFunctionsInGroupBy = caps.supportsCapability(Capability.QUERY_FUNCTIONS_IN_GROUP_BY);

        if(groupCols != null) {
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
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        // Check for basic aggregate function support
        if(! caps.supportsCapability(Capability.QUERY_AGGREGATES)) {
            return false;
        }
                              
        // Check particular function
        String func = aggregate.getAggregateFunction();
        if(func.equals(ReservedWords.COUNT)) {
            if(aggregate.getExpression() == null) {
                if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_COUNT_STAR)) {
                    return false;
                }
            } else {
                if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_COUNT)) {
                    return false;
                }                
            }
        } else if(func.equals(ReservedWords.SUM)) {
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_SUM)) {
                return false;
            }
        } else if(func.equals(ReservedWords.AVG)) {
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_AVG)) {
                return false;
            }
        } else if(func.equals(ReservedWords.MIN)) {
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_MIN)) {
                return false;
            }
        } else if(func.equals(ReservedWords.MAX)) {
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_MAX)) {
                return false;
            }
        }
        
        // Check DISTINCT if necessary
        if(aggregate.isDistinct()) {
            if(! caps.supportsCapability(Capability.QUERY_AGGREGATES_DISTINCT)) {
                return false;
            }                
        }
        
        // Passed all the checks!
        return true;
        

    }

    public static boolean supportsScalarFunction(Object modelID, Function function, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.FUNCTION)) {
            return false;
        }
        
        if (!caps.supportsFunction(function.getName().toLowerCase())) {
            return false;
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
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.QUERY_SELECT_DISTINCT)) {
            return false;
        }            
        
        // Capabilities checked out or didn't exist - check against metadata
        return metadata.modelSupports(modelID, SupportConstants.Model.DISTINCT);
    }

    public static boolean supportsSelectLiterals(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_SELECT_LITERALS);
    }

    public static boolean supportsOrderBy(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.QUERY_ORDERBY)) {
            return false;
        }            
        
        // Capabilities checked out or didn't exist - check against metadata
        return metadata.modelSupports(modelID, SupportConstants.Model.ORDER_BY);
    }

    public static boolean supportsJoinExpression(Object modelID, List joinCriteria, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        if(! caps.supportsCapability(Capability.FUNCTION)) {
            return false;
        }            
    
        if(joinCriteria != null && joinCriteria.size() > 0) {
            Iterator iter = joinCriteria.iterator();
            while(iter.hasNext()) {
                Criteria crit = (Criteria) iter.next();
                Collection functions = FunctionCollectorVisitor.getFunctions(crit, false);
                
                Iterator funcIter = functions.iterator();
                while(funcIter.hasNext()) {
                    Function function = (Function) funcIter.next();
                    if(! supportsScalarFunction(modelID, function, metadata, capFinder)) {
                        return false; 
                    }
                }
            }
        }
                
        // Found nothing unsupported
        return true;            
    }
    
    public static boolean supportsScalarSubquery(Object modelID, ScalarSubquery subquery, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_SUBQUERIES_SCALAR);
    }
    
    public static boolean supportsCorrelatedSubquery(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_SUBQUERIES_CORRELATED);
    }       

    public static boolean supportsSetOp(Object modelID, Operation setOp, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

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
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_SET_ORDER_BY);
    }

    public static boolean supportsCaseExpression(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_CASE);
    }

    public static boolean supportsSearchedCaseExpression(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.QUERY_SEARCHED_CASE);
    }

    public static int getMaxInCriteriaSize(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

        if (metadata.isVirtualModel(modelID)){
            return -1;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);
        Object maxInCriteriaSize = caps.getSourceProperty(Capability.MAX_IN_CRITERIA_SIZE);
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
    
    public static int getMaxFromGroups(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {

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

    
    public static boolean supportsRowLimit(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.ROW_LIMIT);
    }

    public static boolean supportsRowOffset(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        if (metadata.isVirtualModel(modelID)){
            return false;
        }

        // Find capabilities
        SourceCapabilities caps = getCapabilities(modelID, metadata, capFinder);

        return caps.supportsCapability(Capability.ROW_OFFSET);
    }
    
    public static boolean isSameConnector(Object modelID, Object modelID1, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
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
        
        if (connectorID != null && connectorID.equals(caps1.getSourceProperty(Capability.CONNECTOR_ID))) {
            return true;
        }
        
        return false;
    }

    private static SourceCapabilities getCapabilities(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
        throws QueryMetadataException, MetaMatrixComponentException {

        // Find capabilities
        String modelName = metadata.getFullName(modelID);
        return capFinder.findCapabilities(modelName);
    }

}
