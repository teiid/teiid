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
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;


public class BasicVMComponentDefn extends BasicComponentDefn implements VMComponentDefn, Serializable {  
    private HostID hostID;
    
    public BasicVMComponentDefn(ConfigurationID configurationID, HostID hostID, VMComponentDefnID componentID, ComponentTypeID typeID) {
	    super(configurationID, componentID, typeID);
        this.hostID = hostID;
    }

    protected BasicVMComponentDefn(BasicVMComponentDefn component) {
        super(component);
        this.hostID = component.getHostID();
    }

    
/**
 * Return a deep cloned instance of this object.  Subclasses must override
 *  this method.
 *  @return the object that is the clone of this instance.
 */
   public synchronized Object clone() {
    	return new BasicVMComponentDefn(this);
    }
   
   public HostID getHostID() {
       return hostID;       
   }
   
   
   /**
    * Returns true if the component object is enabled for use.   
    * @return
    * @since 4.2
    */
   public boolean isEnabled() {
       String enabled = this.getProperty(VMComponentDefnType.ENABLED_FLAG);
       if (enabled == null || enabled.length() == 0) {
           return true;
       }
       return enabled.equalsIgnoreCase(Boolean.TRUE.toString());
   }   
   
   public String getPort() {
       return this.getProperty(VMComponentDefnType.SERVER_PORT);      
   }
   
   /**
    * Return the address that should be used to bind to the host 
    * @return
    * @since 4.3
    */
   public String getBindAddress() {
       String address = getProperty(VMComponentDefnType.VM_BIND_ADDRESS);
       if (address == null || address.length() == 0) {
           return ""; //$NON-NLS-1$
       }
       return address;
       
   }   
    

}

