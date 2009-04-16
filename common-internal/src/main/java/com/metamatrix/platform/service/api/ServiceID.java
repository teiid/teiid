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

import java.io.Serializable;

public class ServiceID implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 1776393684657738553L;
	private long id;
    private String hostName;
    private String processName;

    public ServiceID(long id, String hostName, String processName) {
        this.id = id;
        this.hostName = hostName;
        this.processName = processName;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getProcessName() {
    	return this.processName;
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

    public String toString() {
        return "Service<"+id+"|"+this.hostName+"|"+processName+">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
}

