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
import com.metamatrix.query.sql.*;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * Represents MetaMatrix extension options to normal SQL.  Options 
 * are declared in a list after the OPTION keyword, such as:
 * "OPTION SHOWPLAN DEBUG".
 */
public class Option implements LanguageObject {
    
    public final static String MAKEDEP = ReservedWords.MAKEDEP; 
    public final static String MAKENOTDEP = ReservedWords.MAKENOTDEP; 
    public final static String OPTIONAL = "optional"; //$NON-NLS-1$

	private boolean showPlan = false;
	private boolean debug = false;
    private List makeDependentGroups = null;        // of String
    private List makeNotDependentGroups = null;        // of String
    private boolean planOnly = false;
	private List noCacheGroups;	// List of String
    private boolean noCache;

	/**
	 * Construct a default instance of the Option clause.
	 */
	public Option() {
	}

	/**
	 * Set flag for returning the query plan with the results
	 * @param flag True to return the query plan with the results, false otherwise
	 */
	public void setShowPlan(boolean flag) {
		this.showPlan = flag;
	}

	/**
	 * Get flag for whether to return the query plan with the results
	 * @return True to return the query plan with the results
	 */
	public boolean getShowPlan() {
		return this.showPlan;
	}	
    
    /**
     * Set flag for returning only the query plan without executing the query
     * @param flag True to return the query plan without the results, false otherwise
     */
    public void setPlanOnly(boolean planOnly) {
        this.planOnly = planOnly;        
    }
    
    /**
     * Get flag for whether to return the query plan without the results
     * @return True to return the query plan without the results
     */
    public boolean getPlanOnly() {
        return this.planOnly;
    }
	
	/**
	 * Set flag to dump debug info on the server
	 * @param flag True to dump debug info for this query, false otherwise
	 */
	public void setDebug(boolean flag) {
		this.debug = flag;
	}

	/**
	 * Get flag for whether to dump debug info on the server
	 * @return True if debug info should be dumped on the server, false otherwise
	 */
	public boolean getDebug() {
		return this.debug;
	}	
    
    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addDependentGroup(String group) {
        if(this.makeDependentGroups == null) {
            this.makeDependentGroups = new ArrayList();
        }
        this.makeDependentGroups.add(group);    
    }
    
    /** 
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List getDependentGroups() {
        return this.makeDependentGroups;
    }
    
    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addNotDependentGroup(String group) {
        if(this.makeNotDependentGroups == null) {
            this.makeNotDependentGroups = new ArrayList();
        }
        this.makeNotDependentGroups.add(group);    
    }
    
    /** 
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List getNotDependentGroups() {
        return this.makeNotDependentGroups;
    }
    
    /**
     * Add group that overrides the default behavior of Materialized View feautre
     * to route the query to the primary virtual group transformation instead of 
     * the Materialized View transformation.
     * @param group Group that overrides the default behavior of Materialized View
     */
    public void addNoCacheGroup(String group) {
        if(this.noCacheGroups == null) {
            this.noCacheGroups = new ArrayList();
        }
        this.noCacheGroups.add(group);    
    }
    
    /** 
     * Get all groups that override the default behavior of Materialized View feautre
     * to route the query to the primary virtual group transformation instead of 
     * the Materialized View transformation.
     * @return List of String defining groups that overrides the default behavior of 
     * Materialized View, may be null if there are no groups
     */
    public List getNoCacheGroups() {
        return this.noCacheGroups;
    }
	
	public boolean isNoCache() {
		return noCache;
	}

	public void setNoCache(boolean noCache) {
		this.noCache = noCache;
	}
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }
    
    /**
     * Compare two Option clauses for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(!(obj instanceof Option)) {
			return false;
		}

		Option other = (Option) obj;
        
        return getShowPlan() == other.getShowPlan() &&
               getDebug() == other.getDebug() &&
               getPlanOnly() == other.getPlanOnly() &&
               noCache == other.noCache &&
               EquivalenceUtil.areEqual(getDependentGroups(), other.getDependentGroups()) &&
               EquivalenceUtil.areEqual(getNotDependentGroups(), other.getNotDependentGroups()) &&
               EquivalenceUtil.areEqual(getNoCacheGroups(), other.getNoCacheGroups());
    }

    /**
     * Get hash code for Option.
     * @return Hash code
     */
    public int hashCode() {
		int hc = 0;
		hc = HashCodeUtil.hashCode(hc, getShowPlan());
		hc = HashCodeUtil.hashCode(hc, getDebug());
        hc = HashCodeUtil.hashCode(hc, getPlanOnly());
        if(getDependentGroups() != null) {
            hc = HashCodeUtil.hashCode(hc, getDependentGroups());
        }
        if(getNotDependentGroups() != null) {
            hc = HashCodeUtil.hashCode(hc, getNotDependentGroups());
        }
        if(getNoCacheGroups() != null) {
            hc = HashCodeUtil.hashCode(hc, getNoCacheGroups());
        }
		return hc;
    }

    /**
     * Return deep copy of this option object
     * @return Deep copy of the object
     */
    public Object clone() {
        Option newOption = new Option();
        newOption.setDebug(getDebug());
        newOption.setShowPlan(getShowPlan());
        newOption.setPlanOnly(getPlanOnly());
        newOption.setNoCache(noCache);
        
        if(getDependentGroups() != null) {
            Iterator iter = getDependentGroups().iterator();
            while(iter.hasNext()) {
                newOption.addDependentGroup( (String) iter.next() );
            }
        }
            
        if(getNotDependentGroups() != null) {
            Iterator iter = getNotDependentGroups().iterator();
            while(iter.hasNext()) {
                newOption.addNotDependentGroup( (String) iter.next() );
            }
        }
            
        if(getNoCacheGroups() != null) {
            Iterator iter = getNoCacheGroups().iterator();
            while(iter.hasNext()) {
                newOption.addNoCacheGroup( (String) iter.next() );
            }
        }
        
		return newOption;
    }
}	
