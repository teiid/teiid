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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 * Represents a SQL Insert statement of the form:
 * "INSERT INTO <group> (<variables>) VALUES <values>".
 */
public class Insert extends ProcedureContainer {

    /** Identifies the group to be udpdated. */
    private GroupSymbol group;

    /** list of column variables, null = all columns */
    private List variables = new LinkedList();

    /** List of Expressions, required */
    private List values = new LinkedList();
    
    private QueryCommand queryExpression;

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public Insert() {
    }

	/**
	 * Return type of command.
	 * @return TYPE_INSERT
	 */
	public int getType() {
		return Command.TYPE_INSERT;
	}

    /**
     * Construct an instance with group, variable list (may be null), and values
     * @param group Group associated with this insert
     * @param variables List of ElementSymbols that represent columns for the values, null implies all columns
     * @param values List of Expression values to be inserted
     */
    public Insert(GroupSymbol group, List variables, List values) {
        this.group = group;
        this.variables = variables;
        this.values = values;
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the group being inserted into
     * @return Group being inserted into
     */
    public GroupSymbol getGroup() {
        return group;
    }

    /**
     * Set the group for this insert statement
     * @param group Group to be inserted into
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }
    
    public boolean isBulk() {
    	if (this.values == null) {
    		return false;
    	}
    	if (!(this.values.get(0) instanceof Constant)) {
    		return false;
    	}
    	return ((Constant)this.values.get(0)).isMultiValued();
    }

    /**
     * Return an ordered List of variables, may be null if no columns were specified
     * @return List of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public List getVariables() {
        return variables;
    }

    /**
     * Add a variable to end of list
     * @param var Variable to add to the list
     */
    public void addVariable(ElementSymbol var) {
        variables.add(var);
    }

    /**
     * Add a collection of variables to end of list
     * @param vars Variables to add to the list - collection of ElementSymbol
     */
    public void addVariables(Collection vars) {
        variables.addAll(vars);
    }

    /**
     * Returns a list of values to insert
     * to be inserted.
     * @return List of {@link com.metamatrix.query.sql.symbol.Expression}s
     */
    public List getValues() {
        return this.values;
    }

    /**
     * Sets the values to be inserted.
     * @param values List of {@link com.metamatrix.query.sql.symbol.Expression}s
     */
    public void setValues(List values) {
        this.values.clear();
        this.values.addAll(values);
    }
    
    /**
     * Set a collection of variables that replace the existing variables
     * @param vars Variables to be set on this object (ElementSymbols)
     */
    public void setVariables(Collection vars) {
        this.variables.clear();        
        this.variables.addAll(vars);
    }

    /**
     * Adds a value to the list of values
     * @param value Expression to be added to the list of values
     */
    public void addValue(Expression value) {
        values.add(value);
    }

    public void setQueryExpression( QueryCommand query ) {
        this.queryExpression = query;        
    }
    
    public QueryCommand getQueryExpression() {
        return this.queryExpression;        
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
		
    /**
     * Get hashcode for command.  WARNING: This hash code relies on the hash codes of the
     * Group, variables.  If the command changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after command has been
     * completely constructed.
     * @return Hash code for object
     */
    public int hashCode() {
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.group);
		myHash = HashCodeUtil.hashCode(myHash, this.variables);
		return myHash;
	}

    /**
     * Compare two Insert commands for equality.  Will only evaluate to equal if
     * they are IDENTICAL: group is equal, value is equal and variables are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}
        
		// Quick fail tests
    	if(!(obj instanceof Insert)) {
    		return false;
		}

		Insert other = (Insert) obj;
        
        return EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
               EquivalenceUtil.areEqual(getValues(), other.getValues()) &&
               EquivalenceUtil.areEqual(getVariables(), other.getVariables()) &&
               EquivalenceUtil.areEqual(getQueryExpression(), other.getQueryExpression());
    }
    
    /** 
     * @see com.metamatrix.query.sql.lang.ProcedureContainer#getParameters()
     * @since 5.0
     */
    public Map getProcedureParameters() {
        
        int iSize = getVariables().size();
        HashMap map = new HashMap();
        
        for (int j = 0; j < iSize; j++) {
            ElementSymbol symbol = (ElementSymbol)((ElementSymbol)variables.get( j )).clone();
            symbol.setName(ProcedureReservedWords.INPUT + SingleElementSymbol.SEPARATOR + symbol.getShortCanonicalName());
            map.put(symbol, values.get( j ) );
        } // for 
        return map;
    }

	/**
	 * Return a deep copy of this Insert.
	 * @return Deep copy of Insert
	 */
	public Object clone() {
	    GroupSymbol copyGroup = null;
	    if(group != null) { 
	    	copyGroup = (GroupSymbol) group.clone();    
	    }
	    
	    List copyVars = new LinkedList();
    	Iterator iter = getVariables().iterator();
    	while(iter.hasNext()) { 
    		ElementSymbol element = (ElementSymbol) iter.next();
    		copyVars.add( element.clone() );    
    	}    

        List copyVals = new LinkedList();

        if ( getValues() != null && getValues().size() > 0 ) {
        	iter = getValues().iterator();
        	while(iter.hasNext()) { 
        		Expression expression = (Expression) iter.next();
        		copyVals.add( expression.clone() );    
        	}    
        }
        
	    Insert copy = new Insert(copyGroup, copyVars, copyVals);
	    if (this.queryExpression != null) {
	    	copy.setQueryExpression((QueryCommand)this.queryExpression.clone());
	    }
        this.copyMetadataState(copy);
		return copy;
	}
	
	/**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public List getProjectedSymbols(){
        return Command.getUpdateCommandSymbol();
	}
	
	/**
	 * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return false;
	}
    
}
