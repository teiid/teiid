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

package com.metamatrix.platform.security.api;

import java.io.Serializable;

/**
 * This immutable class represents an identifier for a unique session.
 */
public final class MetaMatrixSessionID implements
                                      Serializable {

    public final static long serialVersionUID = -7872739911360962975L;
    
    private long id;
    
    public MetaMatrixSessionID(long id) {
    	this.id = id;
    }
    
    /**
     * Returns true if the specified object is semantically equal to this instance. Note: this method is consistent with
     * <code>compareTo()</code>.
     * 
     * @param obj
     *            the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof MetaMatrixSessionID) {
            MetaMatrixSessionID that = (MetaMatrixSessionID)obj;
        	return this.id == that.id;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Returns the hash code value for this object.
     * 
     * @return a hash code value for this object.
     */
    public final int hashCode() {
        return (int)id;
    }

    /**
     * Returns a string representing the current state of the object.
     * 
     * @return the string representation of this instance.
     */
    public final String toString() {
        return String.valueOf(id);
    }

}

