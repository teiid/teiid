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

package com.metamatrix.common.object;

import java.util.Comparator;

/**
 * This class serves as a name-based comparator of MetaObject instances.
 * Normal equality and comparison of MetaObject instances is solely based
 * upon the global identifier.  This class provides a means of comparing two
 * metadata entities based solely upon their full name.
 */
public class ObjectDefinitionDisplayNameComparator implements Comparator {

    public ObjectDefinitionDisplayNameComparator() {
    }

    /**
     * Compares the two arguments for order.  Returns a negative integer,
     * zero, or a positive integer as this first object's name
     * is lexicographically less than, equal to, or greater than (respectively)
     * the second object's name.  If the two objects are not instances of MetaObject,
     * this method throws a ClassCastException.
     * <p>
     * @param obj1 the first MetaObject instance to be compared
     * @param obj2 the second MetaObject instance to be compared
     * @return a negative integer, zero, or a positive integer as this first object's name
     *      is lexicographically less than, equal to, or greater than (respectively) the second object's name.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified objects' types prevent them
     *      from being compared by this comparator.
     */
    public final int compare(Object obj1, Object obj2) {
        ObjectDefinition entity1 = (ObjectDefinition) obj1;     // May throw ClassCastException
        ObjectDefinition entity2 = (ObjectDefinition) obj2;     // May throw ClassCastException
        if ( entity1 == null && entity2 == null ) {
            return 0;
        }
        if ( entity1 != null && entity2 == null ) {
            return 1;
        }
        if ( entity1 == null && entity2 != null ) {
            return -1;
        }
        int result = entity1.getDisplayName().compareTo(entity2.getDisplayName());
        return result;
    }

    /**
     * Returns true if the specified object is semantically equal to this comparator instance.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Both classes must match exactly!!!
        // Since this class has no state, there are no attributes to compare
        return ( obj != null && this.getClass() == obj.getClass() );
    }

}

