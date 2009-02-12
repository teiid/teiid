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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 * This object acts as a Set operator on multiple Queries - UNION,
 * INTERSECT, and EXCEPT can be implemented with this Class
 */
public class SetQuery extends QueryCommand {
    
    public enum Operation {
        /** Represents UNION of two queries */
        UNION,
        /** Represents intersection of two queries */
        INTERSECT,
        /** Represents set difference of two queries */
        EXCEPT
    }

	private boolean all = true;
    private Operation operation;                     
    private QueryCommand leftQuery;
    private QueryCommand rightQuery;
    
    private List<Class<?>> projectedTypes = null;  //set during resolving
     
    /**
     * Construct query with operation type
     * @param operation Operation as specified like {@link Operation#UNION}
     */   
    public SetQuery(Operation operation) {
        this.operation = operation;
    }
    
    public SetQuery(Operation operation, boolean all, QueryCommand leftQuery, QueryCommand rightQuery) {
        this.operation = operation;
        this.all = all;
        this.leftQuery = leftQuery;
        this.rightQuery = rightQuery;
    }
    
    public Query getProjectedQuery() {
        if (leftQuery instanceof SetQuery) {
            return ((SetQuery)leftQuery).getProjectedQuery();
        }
        return (Query)leftQuery;
    }
    
   	/**
	 * Return type of command.
	 * @return TYPE_QUERY
	 */
	public int getType() {
		return Command.TYPE_QUERY;
	}
    
    /**
     * Set type of operation
     * @param operation Operation constant as defined in this class
     */
    public void setOperation(Operation operation) { 
        this.operation = operation;
    }
    
    /**
     * Get operation for this set
     * @return Operation as defined in this class
     */
    public Operation getOperation() { 
        return this.operation;
    }        
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

   	/**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public List getProjectedSymbols() {
	    Query query = getProjectedQuery();
	    List projectedSymbols = query.getProjectedSymbols();
        if (projectedTypes != null) {
            return getTypedProjectedSymbols(projectedSymbols, projectedTypes);
        } 
        return projectedSymbols;
    }
    
    public static List getTypedProjectedSymbols(List acutal, List projectedTypes) {
        List newProject = new ArrayList();
        for (int i = 0; i < acutal.size(); i++) {
            SingleElementSymbol originalSymbol = (SingleElementSymbol)acutal.get(i);
            SingleElementSymbol symbol = originalSymbol;
            Class type = (Class)projectedTypes.get(i);
            if (symbol.getType() != type) {
                if (symbol instanceof AliasSymbol) {
                    symbol = ((AliasSymbol)symbol).getSymbol();
                } 

                Expression expr = symbol;
                if (symbol instanceof ExpressionSymbol) {
                    expr = ((ExpressionSymbol)symbol).getExpression();
                } 
                
                try {
                    symbol = new ExpressionSymbol(originalSymbol.getShortName(), ResolverUtil.convertExpression(expr, DataTypeManager.getDataTypeName(type)));
                } catch (QueryResolverException err) {
                    throw new MetaMatrixRuntimeException(err);
                }
                
                if (!(originalSymbol instanceof ExpressionSymbol)) {
                    symbol = new AliasSymbol(originalSymbol.getShortName(), symbol);
                } 
            }
            newProject.add(symbol);
        }
        return newProject;
    }

	/**
	 * Deep clone this object to produce a new identical query.
	 * @return Deep clone
	 */
	public Object clone() {
        SetQuery copy = new SetQuery(this.operation);

        this.copyMetadataState(copy);
        
        copy.leftQuery = (QueryCommand)this.leftQuery.clone();
        copy.rightQuery = (QueryCommand)this.rightQuery.clone();
        
        copy.setAll(this.all);
        
        if(this.getOrderBy() != null) { 
            copy.setOrderBy( (OrderBy) this.getOrderBy().clone() );
        }
 
        if(this.getLimit() != null) { 
            copy.setLimit( (Limit) this.getLimit().clone() );
        }
 
        if(this.getOption() != null) { 
            copy.setOption( (Option) this.getOption().clone() );
        }
        
        return copy;
	}
	
    /**
     * Compare two queries for equality.  
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof SetQuery)) {
    		return false;
		}

		SetQuery other = (SetQuery) obj;
		
        return getOperation() == other.getOperation() &&
        EquivalenceUtil.areEqual(this.isAll(), other.isAll()) &&
        EquivalenceUtil.areEqual(this.leftQuery, other.leftQuery) &&
        EquivalenceUtil.areEqual(this.rightQuery, other.rightQuery) &&
        EquivalenceUtil.areEqual(getOrderBy(), other.getOrderBy()) &&
        EquivalenceUtil.areEqual(getLimit(), other.getLimit()) &&
        EquivalenceUtil.areEqual(getOption(), other.getOption());        
    }

    /**
     * Get hashcode for query.  WARNING: This hash code relies on the hash codes of the
     * Select and Criteria clauses.  If the query changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after query has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// For speed, this hash code relies only on the hash codes of its select
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.operation);
		myHash = HashCodeUtil.hashCode(myHash, getProjectedQuery());
		return myHash;
	}

	/**
	 * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return leftQuery.areResultsCachable() && rightQuery.areResultsCachable();
	}
    
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        return getSubCommandsUpdatingModelCount(metadata);
    }

    /**
     * @return the left and right queries as a list.  This list cannot be modified.
     */
    public List<QueryCommand> getQueryCommands() {
        return Collections.unmodifiableList(Arrays.asList(leftQuery, rightQuery));
    }
    
    /** 
     * @param projectedSymbols The projectedSymbols to set.
     */
    public void setProjectedTypes(List<Class<?>> projectedTypes) {
        this.projectedTypes = projectedTypes;
    }

    /** 
     * @return Returns the projectedTypes.
     */
    public List<Class<?>>  getProjectedTypes() {
        return this.projectedTypes;
    }

    public boolean isAll() {
        return this.all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public QueryCommand getLeftQuery() {
        return this.leftQuery;
    }

    public void setLeftQuery(QueryCommand leftQuery) {
        this.leftQuery = leftQuery;
    }
    
    public QueryCommand getRightQuery() {
        return this.rightQuery;
    }
    
    public void setRightQuery(QueryCommand rightQuery) {
        this.rightQuery = rightQuery;
    }
            
}
