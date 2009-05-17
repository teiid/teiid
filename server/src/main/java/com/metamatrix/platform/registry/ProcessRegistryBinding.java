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

package com.metamatrix.platform.registry;

import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class ProcessRegistryBinding extends RegistryBinding<ProcessManagement> {

    /** Name of vm */
    private String processName;
    
    private boolean alive;
    
    /** defines vm in configuration */
    // TODO: this is part of configuration, this should be removed from here..
    private VMComponentDefn vmComponent;
    
    public ProcessRegistryBinding(String hostName, String processName, VMComponentDefn deployedComponent, ProcessManagement vmController, MessageBus bus) {
    	super(vmController, hostName, bus);
    	this.processName = processName;
        this.vmComponent = deployedComponent;
    }

    public VMComponentDefn getDeployedComponent() {
       return this.vmComponent;
    }

    public String getProcessName() {
    	return processName;
    }
    
    public String getPort() {
        return vmComponent.getPort();
    }
    
    public boolean isAlive() {
    	return this.alive;
    }
    
    public void setAlive(boolean flag) {
    	this.alive = flag;
    }

    public String toString() {
        return "Process<" +this.getHostName()+"|"+ this.processName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public ProcessManagement getProcessController() {
    	return getBindObject();
    }
}

