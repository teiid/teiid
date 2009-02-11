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

package com.metamatrix.common.config.model;

import java.util.Collection;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ProductServiceConfigComponentType;

public class BasicProductServiceConfigType extends BasicComponentType implements ProductServiceConfigComponentType {


    BasicProductServiceConfigType(ComponentTypeID id, ComponentTypeID parentID, ComponentTypeID superID, boolean deployable, boolean deprecated, boolean monitored) {
        super(id, parentID, superID, deployable, deprecated, monitored);
    }

    protected BasicProductServiceConfigType(BasicProductServiceConfigType type) {
        super(type);
         
    }
 
    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {

        BasicProductServiceConfigType result = null;
	    result = new BasicProductServiceConfigType(this);

        Collection defns = this.getComponentTypeDefinitions();
        result.setComponentTypeDefinitions(defns);
		return result;
        

    }
}
