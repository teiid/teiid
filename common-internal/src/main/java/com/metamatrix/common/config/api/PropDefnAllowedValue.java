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

package com.metamatrix.common.config.api;

import com.metamatrix.common.namedobject.BaseObject;

/**
* PropDefnAllowedValue is a wrapper for the allowed value
* that is associated with a PropertyDefn.  This wrapper is required
* so that the allowed value can be tracked back to the exact
* row in the database if came from.
*/
public interface PropDefnAllowedValue extends BaseObject {

  /**
    * Returns the ComponentTypeDefnID this allowed values belongs to.
    * @returns ComponentTypeDefnID
    */

  public ComponentTypeDefnID getComponentTypeDefnID();

  /**
    * Returns the ComponentTypeID this represents the type of component this is
    * defined for.
    * @returns ComponentDefnID
    */

  public ComponentTypeID getComponentTypeID();


  /**
    * Returns the value
    * @return String value
    */
  public String getValue();

}
