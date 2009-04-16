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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.platform.service.api.ServiceState;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class SystemState implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 8316522636781619120L;
	private Collection<HostData> hosts;

    /**
     * Create a new instance of VMRegistryBinding.
     *
     * @param vmID Identifies VMController binding represents
     * @param vmController VMController implementation
     * @param hostName Name of host VM is running on
     */
    public SystemState(Collection hosts) {
        this.hosts = hosts;
    }

    public Collection getHosts() {
        return hosts;
    }
    
    /**
     * Returns true if any services have failed.
     */
    public boolean hasFailedService() {

        List services = getServices();

        // if any service failed to initialize or failed then return true.
        // else return false;
        Iterator iter = services.iterator();
        while (iter.hasNext()) {
            ServiceData sData = (ServiceData) iter.next();
            if(sData.getCurrentState() == ServiceState.STATE_FAILED ||
               sData.getCurrentState() == ServiceState.STATE_INIT_FAILED ||
               sData.getCurrentState() == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
                return true;
            }
        }
        return false;
    }
    
    private List getServices() {
        
        List services = new ArrayList();

         // loop thru all hosts/processes and
         // get serviceData objects.
         Iterator hosts = this.hosts.iterator();
         while (hosts.hasNext()) {
             HostData hData = (HostData) hosts.next();
             Iterator processes = hData.getProcesses().iterator();
             while (processes.hasNext()) {
                 ProcessData pData = (ProcessData) processes.next();
                 services.addAll(pData.getServices());
             }
         }
         
         return services;
        
    }
}

