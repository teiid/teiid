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

import java.io.Serializable;

import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.core.util.HashCodeUtil;


/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class PscID implements Serializable {

    /** defines psc in configuration */
    private ProductServiceConfigID pscConfigID;

    /** Name of process that owns this psc */
    private String processName;

    private int hashCode;

    /**
     * Create a new instance of PscID.
     *
     * @param pscConfigID Identifies PSC
     * @param processName Name of process psc is running on
     */
    public PscID(ProductServiceConfigID pscConfigID, String processName) {
        this.pscConfigID = pscConfigID;
        this.processName = processName;
        computeHashCode();
    }

    public ProductServiceConfigID getPscConfigID() {
        return pscConfigID;
    }

    public String getProcessName() {
        return this.processName;
    }

    private void computeHashCode() {
        hashCode = pscConfigID.hashCode();
        hashCode = HashCodeUtil.hashCode(hashCode, processName.hashCode());
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
        if ( obj instanceof PscID ) {

            PscID that = (PscID) obj;
            return ((pscConfigID.equals(that.getPscConfigID())) & (processName.equals(that.getProcessName())));
        }

        // Otherwise not comparable ...
        return false;
    }
    
    public String toString() {
    	StringBuffer result = new StringBuffer();
    	result.append("PSC:");  //$NON-NLS-1$
		result.append(pscConfigID.getFullName());
		result.append(processName);
		return result.toString(); 
    }
}

