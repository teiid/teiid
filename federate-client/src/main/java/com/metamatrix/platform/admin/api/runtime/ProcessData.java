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

package com.metamatrix.platform.admin.api.runtime;

import java.util.Collection;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.core.util.HashCodeUtil;

import com.metamatrix.platform.vm.controller.VMControllerID;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class ProcessData extends ComponentData {

    /** Map of ServiceID to ServiceRegistryBindings */
    private Collection pscs;

    /** ID of VMControllerID */
    private VMControllerID processID;

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
    public ProcessData(VMControllerID processID, ComponentDefnID defnID, String hostName, Collection pscs, String processName, String port, boolean deployed, boolean registered) {
        super(processName, deployed, registered);
        this.hostName = hostName;
        this.processID = processID;
        this.defnID = defnID;
        this.pscs = pscs;
        this.port = port;
        computeHashCode();
    }

    /**
     * Return a collection of PSCData objects.
     */
    public Collection getPSCs() {
        return pscs;
    }


    /**
     * Return VMControllerID that this binding represents.
     *
     * @return VMControllerID
     */
    public VMControllerID getProcessID() {
        return processID;
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
        hashCode = HashCodeUtil.hashCode(hashCode, processID);
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
                if (this.processID == null && that.getProcessID() != null) return false;
                if (that.getProcessID() == null && this.processID != null) return false;
                return processID.equals(that.getProcessID());
            }
            return defnID.equals(that.getComponentDefnID());
        }

        // Otherwise not comparable ...
        return false;
    }
}

