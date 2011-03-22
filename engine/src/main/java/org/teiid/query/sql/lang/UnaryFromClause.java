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

package org.teiid.query.sql.lang;

import java.util.Collection;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * A FROM subpart that represents a single group.  For example, the FROM clause: 
 * "FROM a, b" will have two UnaryFromClause objects, each holding a reference to 
 * a GroupSymbol (for a and b).
 */
public class UnaryFromClause extends FromClause {

	private GroupSymbol group;
    
    private Command expandedCommand;
	
	/**
	 * Construct default object
	 */
	public UnaryFromClause() {
	}
	
	/**
	 * Construct object with specified group
	 * @param group Group being held
	 */
	public UnaryFromClause(GroupSymbol group) {
		this.group = group;
	}
	
	/**
	 * Set the group held by the clause
	 * @param group Group to hold
	 */
	public void setGroup(GroupSymbol group) {
		this.group = group;
	} 
	
	/**
	 * Get group held by clause
	 * @return Group held by clause
	 */
	public GroupSymbol getGroup() {
		return this.group;
	}
    
    /**
     * Collect all GroupSymbols for this from clause.
     * @param groups Groups to add to
     */
    public void collectGroups(Collection groups) {
        groups.add(this.group);    
    }
	
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Check whether objects are equal
	 * @param obj Other object
	 * @return True if equal
	 */
	public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
		
		if(! (obj instanceof UnaryFromClause)) { 
			return false;
		}		
        
        UnaryFromClause other = (UnaryFromClause)obj;
        
        if( (this.getGroup().getDefinition() == null && other.getGroup().getDefinition() == null) ||
        		(this.getGroup().getDefinition() != null && other.getGroup().getDefinition() != null) ) {
        	return EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
            other.isOptional() == this.isOptional();	
        }
        return false;
	}
	
	/**
	 * Get hash code of object
	 * @return Hash code
	 */
	public int hashCode() {
		if(this.group == null) { 
			return 0;
		}
		return this.group.hashCode();
	}
	
	/**
	 * Get deep clone of object
	 * @return Deep copy of the object
	 */
	public Object clone() {
	    GroupSymbol copyGroup = null;
	    if(this.group != null) { 
	        copyGroup = (GroupSymbol) this.group.clone();
	    }
        UnaryFromClause clonedUnaryFromClause = new UnaryFromClause(copyGroup);
        clonedUnaryFromClause.setOptional(this.isOptional());
        clonedUnaryFromClause.setMakeDep(this.isMakeDep());
        clonedUnaryFromClause.setMakeNotDep(this.isMakeNotDep());
        if (this.expandedCommand != null) {
        	clonedUnaryFromClause.setExpandedCommand((Command)this.expandedCommand.clone());
        }
		return clonedUnaryFromClause;	
	}

    /** 
     * @return Returns the expandedCommand.
     */
    public Command getExpandedCommand() {
        return this.expandedCommand;
    }
    
    /** 
     * @param expandedCommand The expandedCommand to set.
     */
    public void setExpandedCommand(Command expandedCommand) {
        this.expandedCommand = expandedCommand;
    }
		
}
