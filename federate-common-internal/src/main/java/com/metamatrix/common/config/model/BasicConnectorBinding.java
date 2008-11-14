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

import java.io.Serializable;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ConnectorBindingType;



public class BasicConnectorBinding extends BasicServiceComponentDefn implements ConnectorBinding, Serializable {

    public BasicConnectorBinding(ConfigurationID configurationID, ConnectorBindingID componentID, ComponentTypeID typeID) {
        super(configurationID, componentID, typeID);
     }

    protected BasicConnectorBinding(BasicConnectorBinding component) {
        super(component);
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     *  @throws CloneNotSupportedException if this object cannot be cloned
     */
    public synchronized Object clone() throws CloneNotSupportedException {
        return new BasicConnectorBinding(this);
    }


    public String getConnectorClass() {       
        return this.getProperty(ConnectorBindingType.Attributes.CONNECTOR_CLASS);
    }

    public void setDeployedName(String name) {
    	getEditableProperties().setProperty(DEPLOYED_NAME, name);
    }
    
    public String getDeployedName() {
    	return getEditableProperties().getProperty(DEPLOYED_NAME);
    }
    //********************************************************
}

