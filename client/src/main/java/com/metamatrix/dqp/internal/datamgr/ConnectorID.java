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

package com.metamatrix.dqp.internal.datamgr;

import java.io.Serializable;

/**
 * Used to uniquely identify a connector.
 */
public class ConnectorID implements Serializable {
    private static final long serialVersionUID = -3507451286236400399L;

    private String id;

    /**
     * Construct a new instance.
     */
    public ConnectorID(String id) {
        this.id = id;
    }

    /**
     * Return the id that identifies this connector instance.
     * @return Unique ID for the connector
     */
    public String getID() {
        return id;
    }

    /**
     * Compare two ConnectorID's for equality.
     * Return true if instanceName == instanceName and
     * serviceName == serviceName.
     * @param obj ConnectorID to compare to.
     * @return true if ConnectorID's are equal
     */
    public boolean equals(Object obj) {

    	// Quick same object test
    	if(this == obj) {
    		return true;
		}
    	
    	if (!(obj instanceof ConnectorID)) {
    		return false;
    	}

        ConnectorID other = (ConnectorID) obj;

        // compare service name
        return getID().equals(other.getID());
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    /**
     * Return a String representation of this instance.
     */
    public String toString() {
        return "ConnectorID<"+getID()+">"; //$NON-NLS-1$ //$NON-NLS-2$
    }

}

