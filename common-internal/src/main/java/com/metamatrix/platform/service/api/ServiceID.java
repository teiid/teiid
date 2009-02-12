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

package com.metamatrix.platform.service.api;

import com.metamatrix.platform.vm.controller.VMControllerID;
import java.io.Serializable;

public class ServiceID implements Serializable {

    private long id;
    private VMControllerID vmControllerID;

    public ServiceID(long id, VMControllerID vmControllerID) {
        this.id = id;
        this.vmControllerID = vmControllerID;
    }

    public VMControllerID getVMControllerID() {
        return this.vmControllerID;
    }

    public String getHostName() {
        return vmControllerID.getHostName();
    }

    public String toString() {
        return "ServiceID<"+id+"> " + vmControllerID; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public long getID() {
        return id;
    }

    /**
     * Returns <code>true</code> if this object is equal to the other object.
     * Equality is based on the ID value.
     *
     * @param
     * @return
     * @exception Exception An error occurred
     *
     * @see
     */
    public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		if(! this.getClass().isInstance(obj)) {
			return false;
		}

		return ((ServiceID)obj).getID() == getID();
    }

    /**
     * Get hash code for object
     * @return Hash code
     */
    public int hashCode() {
        return (int) this.id;
    }

}

