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

package com.metamatrix.query.sql.proc;

import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.*;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;

/**
 * <p> This class represents a statement used to declare variables in the 
 * storedprocedure language. It extends the <code>Statement</code> that
 * could part of a <code>Block</code>.  This this object holds variable and
 * it's type.</p>
 */
public class DeclareStatement extends AssignmentStatement {

	// type of the variable
	private String varType;
	
	/**
	 * Constructor for DeclareStatement.
	 */
	public DeclareStatement() {
		super();
	}

	/**
	 * Constructor for DeclareStatement.
	 * @param variable The <code>ElementSymbol</code> object that is the variable
	 * @param valueType The type of this variable
	 */
	public DeclareStatement(ElementSymbol variable, String varType) {
		super(variable, null);
        this.varType = varType;
	}
	
	/**
	 * Constructor for DeclareStatement.
	 * @param variable The <code>ElementSymbol</code> object that is the variable
	 * @param valueType The type of this variable
	 */
	public DeclareStatement(ElementSymbol variable, String varType, LanguageObject value) {
        super(variable, value);
		this.varType = varType;
	}

	/**
	 * Get the type of this variable declared in this statement.
	 * @return A string giving the variable type
	 */
	public String getVariableType() {
		return varType;
	}
	
	/**
	 * Set the type of this variable declared in this statement.
	 * @param varType A string giving the variable type
	 */
	public void setVariableType(String varType) {
		this.varType = varType;
	}	
	
	/**
	 * Return the type for this statement, this is one of the types
	 * defined on the statement object.
	 * @return The type of this statement
	 */
	public int getType() {
		return Statement.TYPE_DECLARE;
	}	
	
    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================
        
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
	
	/**
	 * Deep clone statement to produce a new identical statement.
	 * @return Deep clone 
	 */
	public Object clone() {		
        if (getValue() == null) {
            return new DeclareStatement((ElementSymbol)this.getVariable().clone(), this.varType);
        }
        return new DeclareStatement((ElementSymbol)this.getVariable().clone(), this.varType, (LanguageObject)getValue().clone());
	}
	
    /**
     * Compare two DeclareStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: variable and the its type are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(obj == null || !(obj instanceof DeclareStatement) || !super.equals(obj)) {
    		return false;
		}

		DeclareStatement other = (DeclareStatement) obj;
		
        return 
            EquivalenceUtil.areEqual(getVariableType(), other.getVariableType());
    } 

    /**
     * Get hashcode for TableAssignmentStatement.  WARNING: This hash code relies on the hash codes of the
     * statements present in the block.  If statements are added to the block or if
     * statements on the block change the hash code will change. Hash code is only valid
     * after the block has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the variable and its value for this statement
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = super.hashCode();
    	myHash = HashCodeUtil.hashCode(myHash, this.getVariableType());  
        return myHash;
	}
      
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

} // END CLASS
