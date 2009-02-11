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
import java.util.Iterator;

import com.metamatrix.common.config.api.ProductServiceConfigID;


/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class PSCData extends ComponentData {

    /** Collection of serviceData objects */
    private Collection services;

    /** defines vm in configuration */
    private PscID pscID;

    /**
     * Create a new instance of VMRegistryBinding.
     *
     * @param pscID Identifies PSC
     * @param services Collection of ServiceData objects
     * @param processName Name of process psc is running on
     */
    public PSCData(ProductServiceConfigID pscConfigID, Collection services, String processName) {
        super(pscConfigID.getName(), false, false);
        this.pscID = new PscID(pscConfigID, processName);
        this.services = services;

        // set deployed and registered flags.
        // since a psc is not a deployable or registerable object
        // then set based on services in psc
        boolean deployed = false;
        boolean registered = false;
        Iterator iter = services.iterator();
        while (iter.hasNext()) {
            ServiceData sd = (ServiceData) iter.next();
            if (sd.isDeployed()) {
                deployed = true;
            }
            if (sd.isRegistered()) {
                registered = true;
            }
            if (deployed & registered) {
                break;
            }
        }
        this.deployed = deployed;
        this.registered = registered;
    }

    /**
     * Return a list of all ServiceData objects
     *
     * @return List of ServiceData objects
     */
    public Collection getServices() {

        return services;
    }

    public PscID getPscID() {
        return pscID;
    }

    public String getProcessName() {
        return pscID.getProcessName();
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
        if ( obj instanceof PSCData ) {

            PSCData that = (PSCData) obj;
            return pscID.equals(that.getPscID());
        }

        // Otherwise not comparable ...
        return false;
    }
}

