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

package com.metamatrix.platform.admin.api.runtime;

import java.util.Collection;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class ProcessData extends ComponentData {


	/**
	 * 
	 */
	private static final long serialVersionUID = -4648970293578256936L;

	/** Collection of serviceData objects */
    private Collection<ServiceData> services;

    /** defines vm in configuration */
    private ComponentDefnID defnID;

    /** Name of host this process belongs to. */
    private String hostName;
    
    private String port;

    /**
     * Create a new instance of VMRegistryBinding.
     *
     * @param vmID Identifies VMController binding represents
     * @param vmController VMController implementation
     * @param hostName Name of host VM is running on
     */
    public ProcessData(String hostName, String processName, String port, ComponentDefnID defnID,  Collection<ServiceData> services,  boolean deployed, boolean registered) {
        super(processName, deployed, registered);
        this.hostName = hostName;
        this.defnID = defnID;
        this.services = services;
        this.port = port;
        
        computeHashCode();
    }

    /**
     * Return a list of all ServiceData objects
     *
     * @return List of ServiceData objects
     */
    public Collection<ServiceData> getServices() {

        return services;
    }


    /**
     * Return ComponentDefnID for VMController.
     *
     * @return ComponentDefnID
     */
    public ComponentDefnID getComponentDefnID() {
        return this.defnID;
    }

    /**
     * return the name of the host this process belongs to.
     */
    public String getHostName() {
        return hostName;
    }
    
    public String getPort() {
        return this.port;
    }

    private void computeHashCode() {
        hashCode = 0;
        hashCode = HashCodeUtil.hashCode(hashCode, defnID);
        hashCode = HashCodeUtil.hashCode(hashCode, getName());
        hashCode = HashCodeUtil.hashCode(hashCode, hostName);
        
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {

        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        if ( obj instanceof ProcessData ) {

            // This may look a bit strange but it is possible for a ProcessData
            // object to contain a null for both the componentDefnID and/or the
            // Process ID.
            ProcessData that = (ProcessData) obj;
            if (this.defnID == null && that.getComponentDefnID() != null) return false;
            if (that.getComponentDefnID() == null && this.defnID != null) return false;
            if (this.defnID == null && that.getComponentDefnID() == null) {
                return this.hostName.equals(that.hostName) && this.getName().equals(that.getName());
            }
            return defnID.equals(that.getComponentDefnID());
        }

        // Otherwise not comparable ...
        return false;
    }
}

