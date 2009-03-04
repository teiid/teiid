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

import java.io.Serializable;

import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.server.ResourceFinder;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class ProcessRegistryBinding implements Serializable {

    /** Host that this VM belongs to. */
    private String hostName;

    /** Name of vm */
    private String processName;
    
    private boolean alive;

    /**
     * Local reference to VMController, this is transient to prevent it from
     * being sent to other vm's. Remote vm's must use the stub to access vmController
     */
    private transient ProcessManagement processController;

    /** Remote reference to VMController */
    private Object vmControllerStub;

    /** defines vm in configuration */
    // TODO: this is part of configuration, this should be removed from here..
    private VMComponentDefn vmComponent;

    private transient MessageBus messageBus;
    
    public ProcessRegistryBinding(String hostName, String processName, VMComponentDefn deployedComponent, ProcessManagement vmController, MessageBus bus) {
    	this.hostName = hostName;
    	this.processName = processName;
        this.vmComponent = deployedComponent;
        this.processController = vmController;
        this.messageBus = bus;
        this.vmControllerStub = getStub(vmController);
    }

    private Object getStub(ProcessManagement controller) {
        if (controller == null) {
            return null;
        }
        return this.messageBus.export(controller, new Class[] {ProcessManagement.class});
    }

    /**
     * Return reference for VMController.
     * If VMController is running in this VM then return local reference.
     * Else return remote reference.
     *
     * @return VMController reference
     */
    public synchronized ProcessManagement getProcessController() {

        // if vmController is null then this must be a remote vm so return the stub.
        if (this.processController != null) {
            return processController;
        }
        if (this.vmControllerStub == null) {
        	return null;
        }
    	// when exported to the remote, use remote's message bus instance.
    	MessageBus bus = this.messageBus;
    	if(bus == null) {
    		bus = ResourceFinder.getMessageBus();
    	}        
        processController = (ProcessManagement)bus.getRPCProxy(this.vmControllerStub);
        return processController;
    }

    public VMComponentDefn getDeployedComponent() {
       return this.vmComponent;
    }

    public String getHostName() {
    	return hostName;
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
        return "Process<" +this.hostName+"|"+ this.processName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}

