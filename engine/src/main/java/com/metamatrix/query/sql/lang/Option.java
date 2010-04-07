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
import java.util.List;

import org.teiid.connector.language.SQLReservedWords;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * Represents MetaMatrix extension options to normal SQL.  Options 
 * are declared in a list after the OPTION keyword, such as:
 * "OPTION SHOWPLAN DEBUG".
 */
public class Option implements LanguageObject {
    
    public final static String MAKEDEP = SQLReservedWords.MAKEDEP; 
    public final static String MAKENOTDEP = SQLReservedWords.MAKENOTDEP; 
    public final static String OPTIONAL = "optional"; //$NON-NLS-1$

    private List<String> makeDependentGroups;
    private List<String> makeNotDependentGroups;
	private List<String> noCacheGroups;
    private boolean noCache;

	/**
	 * Construct a default instance of the Option clause.
	 */
	public Option() {
	}

    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addDependentGroup(String group) {
        if(this.makeDependentGroups == null) {
            this.makeDependentGroups = new ArrayList<String>();
        }
        this.makeDependentGroups.add(group);    
    }
    
    /** 
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List<String> getDependentGroups() {
        return this.makeDependentGroups;
    }
    
    /**
     * Add group to make dependent
     * @param group Group to make dependent
     */
    public void addNotDependentGroup(String group) {
        if(this.makeNotDependentGroups == null) {
            this.makeNotDependentGroups = new ArrayList<String>();
        }
        this.makeNotDependentGroups.add(group);    
    }
    
    /** 
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List<String> getNotDependentGroups() {
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
            this.noCacheGroups = new ArrayList<String>();
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
    public List<String> getNoCacheGroups() {
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
        
        return noCache == other.noCache &&
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
        newOption.setNoCache(noCache);
        
        if(getDependentGroups() != null) {
        	newOption.makeDependentGroups = new ArrayList<String>(getDependentGroups());
        }
            
        if(getNotDependentGroups() != null) {
        	newOption.makeNotDependentGroups = new ArrayList<String>(getNotDependentGroups());
        }
            
        if(getNoCacheGroups() != null) {
        	newOption.noCacheGroups = new ArrayList<String>(getNoCacheGroups());
        }
        
		return newOption;
    }
}	
