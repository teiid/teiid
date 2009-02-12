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

package com.metamatrix.common.actions;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.HashCodeUtil;

public class AttributeDefinition implements Comparable {
    public int NO_ATTRIBUTE = -999999999;

    private String label;
    private int code;
    int hashCode;

    public AttributeDefinition(int code, String label) {
        this.code = code;
        this.label = ( label != null ) ? label : ""; //$NON-NLS-1$
        this.hashCode = HashCodeUtil.hashCode(0, code);
        this.hashCode = HashCodeUtil.hashCode(this.hashCode, label);
    }

    public String getLabel() {
        return label;
    }

    public int getCode() {
        return code;
    }

    /**
     * Overrides Object hashCode method. Note that the
     * hash code is computed purely from the ID, so two distinct instances that
     * have the same identifier (i.e., full name) will have the same hash code
     * value.
     * <p>
     * This hash code must be consistent with the <code>equals</code> method.
     * defined by subclasses.
     * @return the hash code value for this metadata object.
     */
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof AttributeDefinition) {

            // fail fast on different hash codes
            if (this.hashCode() != obj.hashCode()) {
                return false;
            }

            // slower comparison
            AttributeDefinition that = (AttributeDefinition)obj;
            return ( that.code == this.code && that.getLabel().equals(this.getLabel()) );
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Compares this object to another. If the specified object is an instance of
     * the same class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).  Note:  this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return 0;
        }
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0004, this.getClass().getName()));
        }

        // Check if object cannot be compared to this one
        // (this includes checking for null ) ...
        if (!(obj instanceof AttributeDefinition)) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0005,
            		new Object[] {obj.getClass().getName(), this.getClass().getName()} ));
        }

        // Check if everything else is equal ...
        AttributeDefinition that = (AttributeDefinition)obj;
        int result = that.code - this.code;
        if ( result != 0 ) return result;
        return this.getLabel().compareTo(that.getLabel());
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString() {
        return this.label;
    }
}




