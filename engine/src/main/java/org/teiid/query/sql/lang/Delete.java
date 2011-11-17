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

import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents a SQL Delete statement of the form:
 * "DELETE FROM <group> [WHERE <criteria>]".
 * Implements Command interface.
 */
public class Delete extends TranslatableProcedureContainer {

    /** Identifies the group to delete data from. */
    private GroupSymbol group;

    /** The criteria specifying constraints on what data will be deleted. */
    private Criteria criteria;

    /**
     * Constructs a default instance of this class.
     */
    public Delete() {
    }

	/**
	 * Return type of command.
	 * @return {@link Command#TYPE_DELETE}
	 */
	public int getType() {
		return Command.TYPE_DELETE;
	}

    /**
     * Constructs an instance of this class given the group.
     * @param group Identifier of the group to delete data from.
     */
    public Delete(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Constructs an instance of this class given the group and criteria.
     * @param group Identifier of the group to delete data from.
     * @param criteria The criteria specifying constraints on what data will be deleted.
     */
    public Delete(GroupSymbol group,  Criteria criteria) {
        this(group);
        this.criteria = criteria;
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the group being deleted from
     * @return Group symbol
     */
    public GroupSymbol getGroup() {
        return group;
    }

    /**
     * Set the group for this Delete command
     * @param group Group to be associated with this command
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Returns the criteria object for this command.
     * @return criteria
     */
    public Criteria getCriteria() {
        return this.criteria;
    }

    /**
     * Set the criteria for this Delete command
     * @param criteria Criteria to be associated with this command
     */
    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
		
    /**
     * Get hashcode for command.  WARNING: This hash code relies on the hash codes of the
     * Group and Criteria clause.  If the command changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after the command has been
     * completely constructed.
     */
    public int hashCode() {
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.group);
        if (this.criteria != null) {
    		myHash = HashCodeUtil.hashCode(myHash, this.criteria);
        }
		return myHash;
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return Command in string form
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /**
     * Compare two commands for equality. Will only evaluate to equal if
     * they are IDENTICAL: group is the same, criteria are in
     * the same exact structure.
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests
    	if(!(obj instanceof Delete)) {
    		return false;
		}

		Delete other = (Delete) obj;
        
        return EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
        	   sameOptionAndHint(other) &&
               EquivalenceUtil.areEqual(getCriteria(), other.getCriteria());
    }
    
    /** 
     * @see org.teiid.query.sql.lang.ProcedureContainer#getParameters()
     * @since 5.0
     */
    public LinkedHashMap<ElementSymbol, Expression> getProcedureParameters() {
        return new LinkedHashMap<ElementSymbol, Expression>();
    }

	/**
	 * Return a copy of this Delete.
	 */
	public Object clone() {
	    GroupSymbol copyGroup = null;
	    if(group != null) { 
	    	copyGroup = group.clone();    
	    }
	    
		Criteria copyCrit = null;
		if(criteria != null) { 
			copyCrit = (Criteria) criteria.clone();
		}

        Delete copy = new Delete(copyGroup, copyCrit);
        copyMetadataState(copy);
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
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return false;
	}
    
}

