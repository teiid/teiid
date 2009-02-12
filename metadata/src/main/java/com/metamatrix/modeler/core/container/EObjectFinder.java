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

package com.metamatrix.modeler.core.container;


/**
 * @since 3.1
 */
public interface EObjectFinder {
    //############################################################################################################################
	//# Methods                                                                                                                  #
	//############################################################################################################################
    
    /**
     * Find the object with the specified primary key.
     * @param key the primary key for the object
     * @return Object the object with the matching primary key, or null 
     * if no object with the specified primary
     * key could be found
     */
    Object find(Object key);
    
    /**
     * Find the object key for the given obj.
     * @param the object for which to find the key
     * @return Object the object key for the given object, or null
     * if no key for the given object
     * @author Lance Phillips
     *
     * @since 3.1
     */
    Object findKey(Object object);
}
