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

package com.metamatrix.common.jdbc.metadata;

import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Map;

public class ProcedureType {

    /**
     * Null values may not be allowed.
     */
    public static final ProcedureType NO_RESULT = new ProcedureType(DatabaseMetaData.procedureNoResult, "No Result"); //$NON-NLS-1$

    /**
     * Null values are definitiely allowed.
     */
    public static final ProcedureType RESULT_UNKNOWN = new ProcedureType(DatabaseMetaData.procedureResultUnknown, "Result Unknown"); //$NON-NLS-1$

    /**
     * Whether Null values are allowed is unknown.
     */
    public static final ProcedureType RETURNS_RESULT = new ProcedureType(DatabaseMetaData.procedureReturnsResult, "Returns Result"); //$NON-NLS-1$

    private int value;
    private String displayName;
    private static final Map BY_NAME = new HashMap();
    private static final Map BY_VALUE = new HashMap();

    static {
        add(NO_RESULT);
        add(RESULT_UNKNOWN);
        add(RETURNS_RESULT);
    }

    private ProcedureType(int value, String displayName) {
        this.value = value;
        this.displayName = displayName;
    }

    private static void add(ProcedureType instance) {
        BY_NAME.put(instance.getDisplayName(), instance);
        BY_VALUE.put(new Integer(instance.value), instance);
    }

    public static ProcedureType getInstance(String displayName) {
        return (ProcedureType)BY_NAME.get(displayName);
    }

    public static ProcedureType getInstance(int value) {
        return (ProcedureType)BY_VALUE.get(new Integer(value));
    }

    /**
     * Return the display name for this type.
     * @return the display name
     */
    public String getDisplayName() {
        return this.displayName;
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the MetaMatrixSessionID class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning
     *  that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        ProcedureType that = (ProcedureType)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException("Attempt to compare null"); //$NON-NLS-1$
        }

        return (this.value - that.value);
    }

    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     *   Note:  this method is consistent with
     * <code>compareTo()</code>.
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
        //if ( this.getClass().isInstance(obj) ) {
        if (obj instanceof ProcedureType) {
            ProcedureType that = (ProcedureType)obj;
            return this.value == that.value;
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Returns the hash code value for this object.
     * @return a hash code value for this object.
     */
    public final int hashCode() {
        return this.value;
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public final String toString() {
        return getDisplayName();
    }
}



