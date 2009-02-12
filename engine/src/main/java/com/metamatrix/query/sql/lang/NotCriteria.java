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

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.*;

/**
 * A logical criteria that takes the logical NOT of the contained criteria.  
 * That is, if the contained criteria returns true, this criteria returns 
 * false.  For example:  "NOT (element = 5)"
 */
public class NotCriteria extends AtomicCriteria {

	/**
	 * Constructs a default instance of this class.
	 */
	public NotCriteria() {
	}

	/**
	 * Constructs an instance of this class with sub-criteria.
	 * @param crit Contained criteria
	 */
	public NotCriteria(Criteria crit) {
		super(crit);
	}

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
	
    
    /**
     * Compare equality of two AtomicCriteria.
     * @param obj Other object
     * @return True if equivalent
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof NotCriteria)) {
            return false;
        }
        
        return EquivalenceUtil.areEqual(getCriteria(), ((NotCriteria)obj).getCriteria());
    }

    /**
     * Get hash code
     * @return Hash code
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(0, getCriteria());
    }
	
	/**
	 * Deep copy of object
	 * @return Deep copy of object
	 */
	public Object clone() {
		return new NotCriteria( (Criteria) getCriteria().clone() );  
	}
	
}
