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

package com.metamatrix.platform.vm.controller;

import java.io.Serializable;

public class VMControllerID implements Serializable {
    
	private long id;
    private String hostName;

    public VMControllerID(long id, String hostName) {
        this.id = id;
        this.hostName = hostName.toUpperCase();
    }

    public String getHostName() {
        return hostName;
    }

    public long getID() {
        return id;
    }

    public String toString() {
        return "VMControllerID<"+id+">:"+hostName; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		if(!(obj instanceof VMControllerID)) {
			return false;
		}
		return ((VMControllerID)obj).getID() == getID();
    }

    public int hashCode() {
        return (int) this.id;
    }
}



