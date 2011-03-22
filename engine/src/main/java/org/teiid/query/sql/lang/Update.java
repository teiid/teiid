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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents a SQL Update statement of the form:
 * "UPDATE <group> SET <element> = <expression>, ... [WHERE <criteria>]".
 */
public class Update extends TranslatableProcedureContainer {

    /** Identifies the group to be updated. */
    private GroupSymbol group;

    private SetClauseList changeList = new SetClauseList();

    /** optional criteria defining which row get updated. */
    private Criteria criteria;

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public Update() {
    }
    
	/**
	 * Return type of command.
	 * @return TYPE_UPDATE
	 */
	public int getType() {
		return Command.TYPE_UPDATE;
	}

    /**
     * Construct with group and change list
     * @param group Group to by updated
     * @param changeList List of CompareCriteria that represent Element->expression updates
     */
    public Update(GroupSymbol group, SetClauseList changeList) {
        this.group = group;
        this.changeList = changeList;
    }

    /**
     * Construct with group, change list, and criteria
     * @param group DataGroupID that represents the group being updated
     * @param List of changeCriteria that represent Element->value pairings
     * @param criteria Criteria that defines what rows get updated
     */
    public Update(GroupSymbol group, SetClauseList changeList, Criteria criteria) {
        this(group, changeList);
        this.criteria = criteria;
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the group being updated
     * @return Group being updated
     */
    public GroupSymbol getGroup() {
        return group;
    }

    /**
     * Set the group being updated
     * @param group Group being updated
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }
    
    /**
     * Set the list of CompareCriteria representing updates being made
     * @param changeList List of CompareCriteria
     */
    public void setChangeList(SetClauseList changeList) {
        this.changeList = changeList;
    }    

    /**
     * Return the list of CompareCriteria representing updates being made
     * @return List of CompareCriteria
     */
    public SetClauseList getChangeList() {
        return this.changeList;
    }

    /**
     * Add change to change list - a change is represented by a CompareCriteria
     * internally but can be added here as an element and an expression
     * @param id Element to be changed
     * @param value Expression, often a value, being set
     */
    public void addChange(ElementSymbol id, Expression value) {
        changeList.addClause(id, value);
    }

    /**
     * Returns the criteria object for this command, may be null
     * @return Criteria, may be null
     */
    public Criteria getCriteria() {
        return this.criteria;
    }

    /**
     * Set the criteria for this Update command
     * @param criteria Criteria to be associated with this command
     */
    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    // =========================================================================
    //                  P A R S E R     M E T H O D S
    // =========================================================================

	
    /**
     * Get hashcode for command.  WARNING: This hash code relies on the hash codes of the
     * Group, changeList and Criteria clause.  If the command changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after command has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.group);
        myHash = HashCodeUtil.hashCode(myHash, this.changeList);
        if (this.criteria != null) {
            myHash = HashCodeUtil.hashCode(myHash, this.criteria);
        }
		return myHash;
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

    /**
     * Compare two update commands for equality.  Will only evaluate to equal if
     * they are IDENTICAL: group is equal, changeList contains same compareCriteria, criteria are in
     * the same exact structure.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests
    	if(!(obj instanceof Update)) {
    		return false;
		}

		Update other = (Update) obj;
        
        return 
            // Compare the groups
            EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&
            // Compare the changeList by checking to see if
            // both objects contains exactly the same CompareCriteria objects.
            getChangeList().equals(other.getChangeList()) &&
            // Compare the criteria clauses
            EquivalenceUtil.areEqual(getCriteria(), other.getCriteria());
    }

	/**
	 * Return a copy of this Update.
	 * @return Deep clone
	 */
	public Object clone() {
		Update copy = new Update();
		
	    if(group != null) { 
	        copy.setGroup(group.clone());
	    }
	    
	    copy.setChangeList((SetClauseList)this.changeList.clone());

		if(criteria != null) { 
			copy.setCriteria((Criteria) criteria.clone());
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
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable(){
		return false;
	}
    
    /** 
     * @see org.teiid.query.sql.lang.ProcedureContainer#getProcedureParameters()
     * @since 5.0
     */
    public LinkedHashMap<ElementSymbol, Expression> getProcedureParameters() {
        
    	LinkedHashMap<ElementSymbol, Expression> map = new LinkedHashMap<ElementSymbol, Expression>();
        
        for (Iterator iter = getChangeList().getClauses().iterator(); iter.hasNext();) {
        	SetClause setClause = (SetClause)iter.next();
            ElementSymbol symbol = (ElementSymbol)(setClause.getSymbol()).clone();
            symbol.setGroupSymbol(new GroupSymbol(ProcedureReservedWords.INPUTS));
            map.put( symbol, setClause.getValue() );
        } // for
        
        return map;
    }
    
}


