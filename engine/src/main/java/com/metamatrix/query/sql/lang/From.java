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

import java.util.*;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.*;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.*;

/**
 * Represents a FROM clause in a SELECT query.  The from clause holds a set of 
 * FROM subclauses.  Each FROM subclause can be either a single group 
 * ({@link UnaryFromClause}) or a join predicate ({@link JoinPredicate}).
 */
public class From implements LanguageObject {

	// List of <FromClause>
    private List clauses;

    /**
     * Constructs a default instance of this class.
     */
    public From() {
    	clauses = new ArrayList();
    }

    /**
     * Constructs an instance of this class from an ordered set of from clauses
     * @param parameters The ordered list of from clauses
     */
    public From( List parameters ) {
        clauses = new ArrayList( parameters );
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================
	
	/**
	 * Add a clause to the FROM
	 * @param clause Add a clause to the FROM
	 */
	public void addClause(FromClause clause) {
		this.clauses.add(clause);
	}
	
	/**
	 * Add clauses to the FROM 
	 * @param clauses Collection of {@link FromClause}s
	 */
	public void addClauses(Collection clauses) {
		this.clauses.addAll(clauses);
	}
	
	/** 
	 * Get all the clauses in FROM
	 * @return List of {@link FromClause}
	 */
	public List getClauses() {
		return this.clauses;
	}
    
	/** 
	 * Set all the clauses
	 * @param clauses List of {@link FromClause}
	 */
	public void setClauses(List clauses) {
		this.clauses = clauses;
	}
	
	
    /**
     * Adds a new group to the list (it will be wrapped in a UnaryFromClause)
     * @param group Group to add
     */
    public void addGroup( GroupSymbol group ) {
    	if( group != null ) {
			clauses.add(new UnaryFromClause(group));
        }
    }   

    /**
     * Adds a new collection of groups to the list
     * @param groups Collection of {@link GroupSymbol}
     */
    public void addGroups( Collection groups ) {
    	if(groups != null) {
			Iterator iter = groups.iterator();
			while(iter.hasNext()) {
				clauses.add(new UnaryFromClause((GroupSymbol) iter.next()));
			}
        }
    }

    /**
     * Returns an ordered list of the groups in all sub-clauses.
     * @return List of {@link GroupSymbol}
     */
    public List getGroups() {
        List groups = new ArrayList();
        if(clauses != null) {
            for(int i=0; i<clauses.size(); i++) {
                FromClause clause = (FromClause) clauses.get(i);
                clause.collectGroups(groups);
            }
        }
            
        return groups;
    }
    
    /**
     * Checks if a group is in the From
     * @param group Group to check for
     * @return True if the From contains the group
     */
    public boolean containsGroup( GroupSymbol group ) {
        return getGroups().contains(group);
    }
	
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
		
    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Return copy of this From clause.
     */
    public Object clone() {
        List copyClauses = new ArrayList(clauses.size());
        if(clauses.size() > 0) { 
            Iterator iter = clauses.iterator();
            while(iter.hasNext()) { 
                FromClause c = (FromClause) iter.next();
                copyClauses.add(c.clone());
            }
        }
        
		return new From(copyClauses);
    }

	/**
	 * Compare two Froms for equality.  Order is not important in the from, so
	 * this is a set comparison.
	 */
	public boolean equals(Object obj) {

		if(obj == this) {
			return true;
		}

		if(!(obj instanceof From)) {
			return false;
		}
        
        return EquivalenceUtil.areEqual(getClauses(), ((From)obj).getClauses());
   	}

	/**
	 * Get hashcode for From.  WARNING: The hash code relies on the variables
	 * in the select, so changing the variables will change the hash code, causing
	 * a select to be lost in a hash structure.  Do not hash a From if you plan
	 * to change it.
	 */
	public int hashCode() {
		return HashCodeUtil.hashCode(0, getGroups());
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

}
