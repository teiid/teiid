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

package com.metamatrix.common.jdbc.metadata;

import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public class ColumnType {

    /**
     * Column type is unknown
     */
    public static final ColumnType COLUMN_PSEUDO_UNKNOWN = new ColumnType(DatabaseMetaData.versionColumnUnknown, "Unknown Pseudo Column"); //$NON-NLS-1$

    /**
     * Column contains an IN Parameter
     */
    public static final ColumnType COLUMN_PSEUDO = new ColumnType(DatabaseMetaData.versionColumnPseudo, "Pseudo Column"); //$NON-NLS-1$

    /**
     * Column contains an INOUT Parameter
     */
    public static final ColumnType COLUMN_NOT_PSEUDO = new ColumnType(DatabaseMetaData.versionColumnNotPseudo, "Non-Pseudo Column"); //$NON-NLS-1$


    private int value;
    private String displayName;

    private static final Map BY_NAME = new HashMap();
    private static final Map BY_VALUE = new HashMap();

    static {
        add(COLUMN_PSEUDO_UNKNOWN);
        add(COLUMN_PSEUDO);
        add(COLUMN_NOT_PSEUDO);
    }


    private ColumnType(int value, String displayName) {
        this.value = value;
        this.displayName = displayName;
    }

    public static ColumnType getInstance(String displayName) {
        return (ColumnType)BY_NAME.get(displayName);
    }

    public static ColumnType getInstance(int value) {

        return (ColumnType)BY_VALUE.get(new Integer(value));
    }

    /**
     * Return the display name for this type.
     * @return the display name
     */
    public String getDisplayName() {
        return this.displayName;
    }


    private static void add(ColumnType instance) {
        BY_NAME.put(instance.getDisplayName(), instance);
        BY_VALUE.put(new Integer(instance.value), instance);
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
        ColumnType that = (ColumnType)obj; // May throw ClassCastException
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
        if (obj instanceof ColumnType) {
            ColumnType that = (ColumnType)obj;
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
