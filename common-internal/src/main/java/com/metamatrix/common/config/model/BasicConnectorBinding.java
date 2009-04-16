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
     * Returns true if this component type supports XA transactions. 
     * If this is <code>true</code>, then @link #isOfTypeConnector()
     * will also be true.  However, @link #isOfTypeConnector() can be true
     * and this can be <code>false</code>.
     * @return boolean true if a connector type
     */
    public boolean isXASupported() {
    	String xa = this.getProperty(ConnectorBindingType.Attributes.IS_XA);
    	if (xa != null && xa.equalsIgnoreCase("true")) {
    		return true;
    	}
    	return false;
    }

    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     */
    public synchronized Object clone() {
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

