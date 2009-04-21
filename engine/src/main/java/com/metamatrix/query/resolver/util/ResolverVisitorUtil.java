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

package com.metamatrix.query.resolver.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ResolverVisitorUtil {
	
	public static class ResolvedLookup {
		private GroupSymbol group;
		private ElementSymbol keyElement;
		private ElementSymbol returnElement;
		
		void setGroup(GroupSymbol group) {
			this.group = group;
		}
		public GroupSymbol getGroup() {
			return group;
		}
		void setKeyElement(ElementSymbol keyElement) {
			this.keyElement = keyElement;
		}
		public ElementSymbol getKeyElement() {
			return keyElement;
		}
		void setReturnElement(ElementSymbol returnElement) {
			this.returnElement = returnElement;
		}
		public ElementSymbol getReturnElement() {
			return returnElement;
		}
	}
	
    /**
     * Check the type of the (left) expression and the type of the single
     * projected symbol of the subquery.  If they are not the same, try to find
     * an implicit conversion from the former type to the latter type, and wrap
     * the left expression in that conversion function; otherwise throw an
     * Exception.
     * @param expression the Expression on one side of the predicate criteria
     * @param crit the SubqueryContainer containing the subquery Command of the other
     * side of the predicate criteria
     * @return implicit conversion Function, or null if none is necessary
     * @throws QueryResolverException if a conversion is necessary but none can
     * be found
     */
    static Expression resolveSubqueryPredicateCriteria(Expression expression, SubqueryContainer crit)
    	throws QueryResolverException {
    
    	// Check that type of the expression is same as the type of the
    	// single projected symbol of the subquery
    	Class exprType = expression.getType();
    	if(exprType == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0030, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0030, expression));
    	}
    	String exprTypeName = DataTypeManager.getDataTypeName(exprType);
    
    	Collection projectedSymbols = crit.getCommand().getProjectedSymbols();
    	if (projectedSymbols.size() != 1){
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0032, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0032, crit.getCommand()));
    	}
    	Class subqueryType = ((Expression)projectedSymbols.iterator().next()).getType();
    	String subqueryTypeName = DataTypeManager.getDataTypeName(subqueryType);
    	Expression result = null;
        try {
            result = ResolverUtil.convertExpression(expression, exprTypeName, subqueryTypeName);
        } catch (QueryResolverException qre) {
            throw new QueryResolverException(qre, ErrorMessageKeys.RESOLVER_0033, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0033, crit));
        }
        return result;
    }

    public static ResolvedLookup resolveLookup(Function lookup, QueryMetadataInterface metadata) throws QueryResolverException, MetaMatrixComponentException {
    	Expression[] args = lookup.getArgs();
    	ResolvedLookup result = new ResolvedLookup();
        // Special code to handle setting return type of the lookup function to match the type of the return element
        if( args[0] instanceof Constant && args[1] instanceof Constant && args[2] instanceof Constant) {
            // If code table name in lookup function refers to virtual group, throws exception
			GroupSymbol groupSym = new GroupSymbol((String) ((Constant)args[0]).getValue());
			try {
				groupSym.setMetadataID(metadata.getGroupID((String) ((Constant)args[0]).getValue()));
				if (groupSym.getMetadataID() instanceof TempMetadataID) {
					throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0065, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0065, ((Constant)args[0]).getValue()));
				}
			} catch(QueryMetadataException e) {
				throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, ((Constant)args[0]).getValue()));
			}
			result.setGroup(groupSym);
			
			List<GroupSymbol> groups = Arrays.asList(groupSym);
			
			String returnElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[1]).getValue(); //$NON-NLS-1$
			ElementSymbol returnElement = new ElementSymbol(returnElementName);
            try {
                ResolverVisitor.resolveLanguageObject(returnElement, groups, metadata);
            } catch(QueryMetadataException e) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, returnElementName));
            }
			result.setReturnElement(returnElement);
            
            String keyElementName = (String) ((Constant)args[0]).getValue() + "." + (String) ((Constant)args[2]).getValue(); //$NON-NLS-1$
            ElementSymbol keyElement = new ElementSymbol(keyElementName);
            try {
                ResolverVisitor.resolveLanguageObject(keyElement, groups, metadata);
            } catch(QueryMetadataException e) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0062, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0062, keyElementName));
            }
			result.setKeyElement(keyElement);
			return result;
        } 
        throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0063, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0063));
    }

    private static QueryResolverException handleUnresolvedGroup(GroupSymbol symbol, String description) {
		UnresolvedSymbolDescription usd = new UnresolvedSymbolDescription(symbol.toString(), description);
	    QueryResolverException e = new QueryResolverException(usd.getDescription()+": "+usd.getSymbol()); //$NON-NLS-1$
	    e.setUnresolvedSymbols(Arrays.asList(usd));
	    return e;
	}

	public static void resolveGroup(GroupSymbol symbol, QueryMetadataInterface metadata)
	    throws MetaMatrixComponentException, QueryResolverException {
	
	    if (symbol.getMetadataID() != null){
	        return;
	    }
	
	    // determine the "metadataID" part of the symbol to look up
	    String potentialID = symbol.getNonCorrelationName();
	    
	    String name = symbol.getName();
	    String definition = symbol.getDefinition();
	
	    Object groupID = null;
	    try {
	        // get valid GroupID for possibleID - this may throw exceptions if group is invalid
	        groupID = metadata.getGroupID(potentialID);
	    } catch(QueryMetadataException e) {
	        // didn't find this group ID
	    } 
	
	    // If that didn't work, try to strip a vdb name from potentialID
	    String vdbName = null;
	    if(groupID == null) {
			String newPotentialID = potentialID;
	        int vdbIndex = potentialID.indexOf(ElementSymbol.SEPARATOR);
	        if(vdbIndex >= 0) {
	            String potentialVdbName = potentialID.substring(0, vdbIndex);
	            newPotentialID = potentialID.substring(vdbIndex+1);
	
	            try {
	                groupID = metadata.getGroupID(newPotentialID);
	                vdbName = potentialVdbName;
	            } catch(QueryMetadataException e) {
	                // ignore - just didn't find it
	            } 
	            if(groupID != null) {
	            	potentialID = newPotentialID;
	            }
	        }
	    }
	
	    // the group could be partially qualified,  verify that this group exists
	    // and there is only one group that matches the given partial name
	    if(groupID == null) {
	    	Collection groupNames = null;
	    	try {
	        	groupNames = metadata.getGroupsForPartialName(potentialID);
	        } catch(QueryMetadataException e) {
	            // ignore - just didn't find it
	        } 
	
	        if(groupNames != null) {
	            int matches = groupNames.size();
	            if(matches == 1) {
	            	potentialID = (String) groupNames.iterator().next();
			        try {
			            // get valid GroupID for possibleID - this may throw exceptions if group is invalid
			            groupID = metadata.getGroupID(potentialID);
			            //set group full name
			            if(symbol.getDefinition() != null){
			            	symbol.setDefinition(potentialID);
			            }else{
			            	symbol.setName(potentialID);
			            }
			        } catch(QueryMetadataException e) {
			            // didn't find this group ID
			        } 
	            } else if(matches > 1) {
	                throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0055));
	            }
	        }
	    }
	    
	    if (groupID == null || metadata.isProcedure(groupID)) {
		    //try procedure relational resolving
	        try {
	            StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(potentialID);
	            symbol.setProcedure(true);
	            groupID = storedProcedureInfo.getProcedureID();
	        } catch(QueryMetadataException e) {
	            // just ignore
	        } 
	    }
	    
	    if(groupID == null) {
	        throw handleUnresolvedGroup(symbol, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0056));
	    }
	    // set real metadata ID in the symbol
	    symbol.setMetadataID(groupID);
	    if(vdbName != null) {
	        // reset name or definition to strip vdb name
	        if(symbol.getDefinition() == null) {
	            symbol.setName(potentialID);
	        } else {
	            symbol.setDefinition(potentialID);
	        }
	    }
	    try {
	        if (!symbol.isProcedure()) {
	            symbol.setIsTempTable(metadata.isTemporaryTable(groupID));
	        }
	    } catch(QueryMetadataException e) {
	        // should not come here
	    } 
	    
	    symbol.setOutputDefinition(definition);
	    symbol.setOutputName(name);
	}

}
