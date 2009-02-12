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

import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.namedobject.BaseID;

public abstract class BasicComponentDefn extends BasicComponentObject implements ComponentDefn, Serializable {
    private static final String SERVICE_ESSENTIAL = "metamatrix.service.essentialservice"; //$NON-NLS-1$

    private ConfigurationID configurationID;

    /**
    * Construct a BaseComponentDefn by providing the <code>ConfigurationID</code> the component
    * belongs in and the <code>ComponentDefnID</code> that identifies this component and
    * the <code>ComponentType</code> indicating the type of component.
    * @param configurationID is the ConfigurationID
    * @param componentID is the ComponentDefnId
    * @param type is the ComponentType
    */
    public BasicComponentDefn(ConfigurationID configurationID, ComponentDefnID componentID, ComponentTypeID typeID) {
        super(componentID, typeID);
        this.configurationID = configurationID;
    }

    protected BasicComponentDefn(BasicComponentDefn component) {
        super(component);
        setConfigurationID(component.getConfigurationID());
    }
    

    public ConfigurationID getConfigurationID(){
	    return configurationID;
    }


    void setConfigurationID(ConfigurationID configID){
	    configurationID = configID;
    }


    public boolean isDependentUpon(BaseID componentObjectId) {
        if (componentObjectId instanceof ConfigurationID) {
            return configurationID.equals(componentObjectId);
        }

        return false;
    }
    
    /**
     * Returns true if the component object is enabled for use.   
     * @return
     * @since 4.2
     */
    public boolean isEnabled() {
        return true;
    }

    public boolean isEssential() {
        String isessential = this.getProperty(SERVICE_ESSENTIAL);
        if (isessential != null && isessential.equalsIgnoreCase(Boolean.TRUE.toString())) {
            return true;
        }
        return false;
    
    }
}
