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

package com.metamatrix.common.config.model;

import com.metamatrix.common.config.api.ComponentTypeDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.PropDefnAllowedValue;
import com.metamatrix.common.config.api.PropDefnAllowedValueID;
import com.metamatrix.common.namedobject.BasicObject;

/**
* BasicPropDefnAllowedValue extends PropDefnAllowedValue so that
* the editor can utilize this class to obtain non-exposed methods
*/
public class BasicPropDefnAllowedValue extends BasicObject implements PropDefnAllowedValue {

  private String value;
  private ComponentTypeDefnID typeDefnID=null;
  private ComponentTypeID componentTypeID;

  public BasicPropDefnAllowedValue(ComponentTypeDefnID typeDefnID, ComponentTypeID typeID, PropDefnAllowedValueID allowedValueID, String newValue) {
      super(allowedValueID);
      this.value = newValue;
      this.typeDefnID = typeDefnID;
      this.componentTypeID = typeID;
  }

  BasicPropDefnAllowedValue(BasicPropDefnAllowedValue copy) {
      super(copy.getID());

      value = copy.getValue();
  }

  public ComponentTypeDefnID getComponentTypeDefnID() {
      return typeDefnID;
  }

  public ComponentTypeID getComponentTypeID() {
      return componentTypeID;
  }

  public String getValue() {
      return value;
  }

  public String toString() {
      return value;
  }

  public void setValue(String newValue) {
      value = newValue;
  }

  public int getAllowedCode() {
      int value = new Integer(this.getName()).intValue();
      return value;
  }

  /**
 * Return a deep cloned instance of this object.  Subclasses must override
 *  this method.
 *  @return the object that is the clone of this instance.
 */
   public synchronized Object clone() {
    	    return new BasicPropDefnAllowedValue(this);
    }


}
