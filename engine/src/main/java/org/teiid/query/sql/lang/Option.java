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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents MetaMatrix extension options to normal SQL.  Options 
 * are declared in a list after the OPTION keyword, such as:
 * "OPTION SHOWPLAN DEBUG".
 */
public class Option implements LanguageObject {
    
    public final static String MAKEDEP = Reserved.MAKEDEP; 
    public final static String MAKENOTDEP = Reserved.MAKENOTDEP; 
    public final static String OPTIONAL = "optional"; //$NON-NLS-1$

    public static class MakeDep {
    	private Integer min;
    	private Integer max;
    	private boolean join;
    	
    	@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((getMin() == null) ? 0 : getMin().hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof MakeDep)) {
				return false;
			}
			MakeDep other = (MakeDep) obj;
			return EquivalenceUtil.areEqual(getMin(), other.getMin())
					&& EquivalenceUtil.areEqual(max, other.max)
					&& join == other.join; 
		}

		public MakeDep(Integer min) {
			this.setMin(min);
		}
		
		public MakeDep() {
			
		}
		
		@Override
		public String toString() {
			return new SQLStringVisitor().appendMakeDepOptions(this).getSQLString();
		}

		public Integer getMin() {
			return min;
		}

		public void setMin(Integer min) {
			this.min = min;
		}

		public Integer getMax() {
			return max;
		}

		public void setMax(Integer max) {
			this.max = max;
		}
		
		public boolean isJoin() {
			return join;
		}
		
		public void setJoin(boolean join) {
			this.join = join;
		}
    }
    
    private List<String> makeDependentGroups;
    private List<MakeDep> makeDependentOptions;
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
    	addDependentGroup(group, new MakeDep());
    }
	
    public void addDependentGroup(String group, MakeDep makedep) {
    	if (makedep == null) {
    		return;
    	}
        if(this.makeDependentGroups == null) {
            this.makeDependentGroups = new ArrayList<String>();
            this.makeDependentOptions = new ArrayList<MakeDep>();
        }
        this.makeDependentGroups.add(group);    
        this.makeDependentOptions.add(makedep);
    }
    
    /** 
     * Get all groups to make dependent
     * @return List of String defining groups to be made dependent, may be null if no groups
     */
    public List<String> getDependentGroups() {
        return this.makeDependentGroups;
    }
    
    public List<MakeDep> getMakeDepOptions() {
    	return this.makeDependentOptions;
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
               EquivalenceUtil.areEqual(makeDependentGroups, other.makeDependentGroups) &&
               EquivalenceUtil.areEqual(getNotDependentGroups(), other.getNotDependentGroups()) &&
               EquivalenceUtil.areEqual(getNoCacheGroups(), other.getNoCacheGroups());
    }

    /**
     * Get hash code for Option.
     * @return Hash code
     */
    public int hashCode() {
		int hc = 0;
        if(this.makeDependentGroups != null) {
            hc = HashCodeUtil.hashCode(hc, this.makeDependentGroups);
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
        
        if(this.makeDependentGroups != null) {
        	newOption.makeDependentGroups = new ArrayList<String>(this.makeDependentGroups);
        	newOption.makeDependentOptions = new ArrayList<MakeDep>(this.makeDependentOptions);
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
