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

public class IndexType {

    /**
     * Identifies table statistics that are returned in conjunction with
     *  a table's index descriptions.
     */
    public static final IndexType STATISTIC = new IndexType(DatabaseMetaData.tableIndexStatistic, "Statistic"); //$NON-NLS-1$

    /**
     * Identifies an index as a clustered index.
     */
    public static final IndexType CLUSTERED = new IndexType(DatabaseMetaData.tableIndexClustered, "Clustered"); //$NON-NLS-1$

    /**
     * Identifies an index as a hashed index.
     */
    public static final IndexType HASHED = new IndexType(DatabaseMetaData.tableIndexHashed, "Hashed"); //$NON-NLS-1$

    /**
     * Identifies an index as some other style of index.
     */
    public static final IndexType OTHER = new IndexType(DatabaseMetaData.tableIndexOther, "Other"); //$NON-NLS-1$

    private int value;
    private String displayName;
    private static final Map BY_NAME = new HashMap();
    private static final Map BY_VALUE = new HashMap();

    static {
        add(STATISTIC);
        add(CLUSTERED);
        add(HASHED);
        add(OTHER);
    }

    private IndexType(int value, String displayName) {
        this.value = value;
        this.displayName = displayName;

    }

    private static void add(IndexType instance) {
        BY_NAME.put(instance.getDisplayName(), instance);
        BY_VALUE.put(new Integer(instance.value), instance);
    }

    public static IndexType getInstance(String displayName) {
        return (IndexType)BY_NAME.get(displayName);
    }

    public static IndexType getInstance(int value) {
        return (IndexType)BY_VALUE.get(new Integer(value));
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
        IndexType that = (IndexType)obj; // May throw ClassCastException
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
        if (obj instanceof IndexType) {
            IndexType that = (IndexType)obj;
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



