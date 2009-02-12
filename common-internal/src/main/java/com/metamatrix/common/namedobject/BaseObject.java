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

package com.metamatrix.common.namedobject;

public interface BaseObject extends Comparable, Cloneable {

    /**
     * Return the identifier for this object.  The returned type will be an instance of the BaseID subclass
     * which corresponds to the class of this node.
     * @return the specialized BaseID instance for this node.
     */
    BaseID getID();


    /**
     * Returns the name for this instance of the object.  If you are using
     * the dot notation for a naming conventions, this will return the last
     * node in name.
     * @return the name
     *
     * @see getFullName
     */
    String getName();

    /**
     * Returns the full name for this instance of the object.
     * @return the name
     */
    String getFullName();

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    Object clone();

  
}


