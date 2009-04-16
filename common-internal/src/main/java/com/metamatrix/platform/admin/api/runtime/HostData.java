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
import java.util.Properties;

/**
 * This class contains information about a host that is running or deployed for this system
 */
public class HostData extends ComponentData {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7192156515568357069L;
	// Collection of ProcessData objects
    private  Collection<ProcessData> processes;
    private Properties properties;


    /**
     * Construct an instance for the given hostName
     *
     * @param hostName Name of host
     */
    public HostData(String hostName, Collection<ProcessData> processes, boolean deployed, boolean registered, Properties props) {
        super(hostName, deployed, registered);
        this.processes = processes;
        this.properties = props;
        computeHashCode();
    }

    private void computeHashCode() {
        hashCode = this.getName().toLowerCase().hashCode();
    }

    /**
     * Return a list of ProcessData objects for this host
     *
     * @return List of ProcessData objects.
     */
    public Collection getProcesses() {
        return processes;
    }
    
    public Properties getProperties() {
    	return this.properties;
    }
}

