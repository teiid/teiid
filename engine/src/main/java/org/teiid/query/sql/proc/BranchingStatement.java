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

/*
 */
package org.teiid.query.sql.proc;

import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.sql.LanguageVisitor;

/**
 * <p> This class represents a break statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.</p>
 */
public class BranchingStatement extends Statement {
	
	public enum BranchingMode {
		/**
		 * Teiid specific - only allowed to target loops
		 */
		BREAK,
		/**
		 * Teiid specific - only allowed to target loops
		 */
		CONTINUE,
		/**
		 * ANSI - allowed to leave any block 
		 */
		LEAVE
	}
	
	private String label;
	private BranchingMode mode;
	
	public BranchingStatement() {
		this(BranchingMode.BREAK);
	}
	
	public BranchingStatement(BranchingMode mode) {
		this.mode = mode;
	}
	
	public String getLabel() {
		return label;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public void setMode(BranchingMode mode) {
		this.mode = mode;
	}
	
	public BranchingMode getMode() {
		return mode;
	}
	
    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     */
    public int getType() {
    	switch (mode) {
    	case BREAK:
    		return Statement.TYPE_BREAK;
    	case CONTINUE:
    		return Statement.TYPE_CONTINUE;
    	case LEAVE:
    		return Statement.TYPE_LEAVE;
    	}
    	throw new AssertionError();
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
    public BranchingStatement clone() {     
        BranchingStatement clone = new BranchingStatement();
        clone.mode = mode;
        clone.label = label;
        return clone;
    }
    
    /**
     * Compare two BreakStatements for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }
        if (!(obj instanceof BranchingStatement)) {
        	return false;
        }
        BranchingStatement other = (BranchingStatement)obj;
        return StringUtil.equalsIgnoreCase(label, other.label) 
        && mode == other.mode;
    } 
    
    public int hashCode() {
        return HashCodeUtil.hashCode(mode.hashCode());
    }
      
}
