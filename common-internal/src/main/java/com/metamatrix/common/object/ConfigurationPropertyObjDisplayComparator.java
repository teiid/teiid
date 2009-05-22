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
public class ConfigurationPropertyObjDisplayComparator implements Comparator {

    public ConfigurationPropertyObjDisplayComparator() {
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
        PropertyDefinition entity1 = (PropertyDefinition) obj1;     // May throw ClassCastException
        PropertyDefinition entity2 = (PropertyDefinition) obj2;     // May throw ClassCastException
        if ( entity1 == null && entity2 == null ) {
            return 0;
        }
        
        // NOTE: The assignment of -1 and 1 are in reverse order from
        // the normal assignment because of the way the PropertiedObjectPanel
        // displays the properties.  It somehow reverses order when displaying.
        // see below also.
        if ( entity1 != null && entity2 == null ) {
            return -1;
        }
        if ( entity1 == null && entity2 != null ) {
            return 1;
        }
        
        int result = compareObjects(entity1, entity2);
        
        if (result == 0) {
            result = entity1.getDisplayName().compareTo(entity2.getDisplayName());

        }
        return result;
    }
    
    private static final int PREFERRED_VALUE = 8;
    private static final int REQUIRED_VALUE = 4;
    private static final int MODIFIABLE_VALUE = 2;

    private final int compareObjects(PropertyDefinition o1, PropertyDefinition o2) {
        int o1c = 0;
        if (o1.isRequired()) {
            o1c = o1c + REQUIRED_VALUE;
        }    
        
        if (o1.isModifiable()) {
            o1c = o1c + MODIFIABLE_VALUE;
        } 
               
        int o2c = 0;
        if (o2.isRequired()) {
            o2c = o2c + REQUIRED_VALUE;
        }    
        
        if (o2.isModifiable()) {
            o2c = o2c + MODIFIABLE_VALUE;
        }         
    
        if (o1c == o2c) {
            return 0;        
        }
        // NOTE: The assignment of -1 and 1 are in reverse order from
        // the normal assignment because of the way the PropertiedObjectPanel
        // displays the properties.  It somehow reverses order when displaying.
        
        if (o1c > o2c) {
            return -1;
        }
        return 1;

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

