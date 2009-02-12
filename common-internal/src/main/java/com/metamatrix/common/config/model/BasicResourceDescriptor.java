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

import java.util.Properties;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;

public class BasicResourceDescriptor extends BasicComponentDefn implements ResourceDescriptor {

	

    public BasicResourceDescriptor(ConfigurationID configurationID, ResourceDescriptorID componentID, ComponentTypeID typeID) {
        super(configurationID, componentID, typeID);

    }

    public BasicResourceDescriptor(ConfigurationID configurationID, ResourceDescriptorID componentID, ComponentTypeID typeID,
                                   Properties props ) {
        super(configurationID, componentID, typeID);
        if ( props != null ) {
            super.setProperties(props);
        }
    }

    protected BasicResourceDescriptor(BasicResourceDescriptor component) {
        super(component);
    }
    
  
   
    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
        return new BasicResourceDescriptor(this);

    }  
    
    /**
     * Returns a string representing the name of the object.  This has been
     * overriden for GUI display purposes - the Console only wants to display
     * the "name" (not the "fullname") of a component object.Me
     * @return the string representation of this instance.
     */
    public String toString(){
        return this.getName();
    }      


}
