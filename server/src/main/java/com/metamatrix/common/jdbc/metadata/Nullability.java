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

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public class Nullability {

    /**
     * Null values may not be allowed.
     */
    public static final Nullability NO_NULLS = new Nullability(DatabaseMetaData.columnNoNulls, "No Nulls"); //$NON-NLS-1$

    /**
     * Null values are definitiely allowed.
     */
    public static final Nullability NULLABLE = new Nullability(DatabaseMetaData.columnNullable, "Nullable"); //$NON-NLS-1$

    /**
     * Whether Null values are allowed is unknown.
     */
    public static final Nullability UNKNOWN = new Nullability(DatabaseMetaData.columnNullableUnknown, "Unknown"); //$NON-NLS-1$

    /**
     * Null values may not be allowed in procedure
     */
    public static final Nullability PROCEDURE_NO_NULLS = new Nullability(DatabaseMetaData.procedureNoNulls, "No Nulls"); //$NON-NLS-1$

    /**
     * Null values are allowed in procedure
     */
    public static final Nullability PROCEDURE_NULLABLE = new Nullability(DatabaseMetaData.procedureNullable, "Nullable"); //$NON-NLS-1$

    /**
     * Not known whether Null values are allowed in procedure
     */
    public static final Nullability PROCEDURE_NULLABLE_UNKNOWN = new Nullability(DatabaseMetaData.procedureNullableUnknown, "Unknown"); //$NON-NLS-1$


    private int value;
    private String displayName;
    private static Map BY_NAME;
    private static Map BY_VALUE;

    private Nullability(int value, String displayName) {
        this.value = value;
        this.displayName = displayName;
        add(this);
    }

    private static void add(Nullability instance) {
        if ( BY_NAME == null ) {
            BY_NAME = new HashMap();
        }
        if ( BY_VALUE == null ) {
            BY_VALUE = new HashMap();
        }
        BY_NAME.put(instance.getDisplayName(), instance);
        BY_VALUE.put(new Integer(instance.value), instance);
    }

    public static Nullability getInstance(String displayName) {
        return (Nullability)BY_NAME.get(displayName);
    }

    public static Nullability getInstance(int value) {
        return (Nullability)BY_VALUE.get(new Integer(value));
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
        Nullability that = (Nullability)obj; // May throw ClassCastException
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0022));
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
        if (obj instanceof Nullability) {
            Nullability that = (Nullability)obj;
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



