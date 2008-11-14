/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
 * This immutable class represents an identifier for a unique MetaMatrix session within a given MetaMatrix System. This object
 * will be returned to the Client when login to the MetaMatrix Server
 */
public final class MetaMatrixSessionID implements
                                      Serializable,
                                      Cloneable,
                                      Comparable {

    public final static long serialVersionUID = -7872739911360962975L;
    
    public static final String DEFAULT_USER = "unknown"; //$NON-NLS-1$

    private long id; //TODO: replace with UUID or other non-guessable identifier when failover support is added

    public MetaMatrixSessionID(long id) {
    	this.id = id;
    }

    public MetaMatrixSessionID(String s) {
    	this.id = Long.parseLong(s);
    }
        
    public long getValue() {
        return this.id;
    }
    
    /**
     * Compares this object to another. If the specified object is an instance of the MetaMatrixSessionID class, then this method
     * compares the contents; otherwise, it throws a ClassCastException (as instances are comparable only to instances of the same
     *  class).
     * <p>
     * Note: this method <i>is </i> consistent with <code>equals()</code>, meaning that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * 
     * @param obj
     *            the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the
     *         specified object, respectively.
     * @throws NullPointerException
     *             if the specified object reference is null
     * @throws ClassCastException
     *             if the specified object's type prevents it from being compared to this instance.
     */
    public int compareTo(Object obj) {
        MetaMatrixSessionID that = (MetaMatrixSessionID)obj; // May throw ClassCastException
        return (int) (this.id - that.id); //May throw NullPointerException
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
        if ( this.getClass().isInstance(obj) ) {
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
        return (int) this.id;
    }

    /**
     * Returns a string representing the current state of the object.
     * 
     * @return the string representation of this instance.
     */
    public final String toString() {
        return Long.toString( this.id );
    }

    /**
     * Return a cloned instance of this object.
     * 
     * @return the object that is the clone of this instance.
     */
    public Object clone() {
        try {
            return super.clone();
        } catch ( CloneNotSupportedException e ) {
        }
        return null;
    }
    
}

