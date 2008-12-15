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

package com.metamatrix.platform.registry;

import java.io.Serializable;

import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMControllerID;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class VMRegistryBinding implements Serializable {

    /** ID of VMControllerID */
    private VMControllerID vmID;

    /** Host that this VM belongs to. */
    private String hostName;

    /** Name of vm */
    private String vmName;
    
    private boolean alive;

    /**
     * Local reference to VMController, this is transient to prevent it from
     * being sent to other vm's. Remote vm's must use the stub to access vmController
     */
    private transient VMControllerInterface vmController;

    /** Remote reference to VMController */
    private Object vmControllerStub;

    /** defines vm in configuration */
    // TODO: this is part of configuration, this should be removed from here..
    private VMComponentDefn vmComponent;

    private transient MessageBus messageBus;
    
    public VMRegistryBinding(String hostName, VMControllerID vmID, VMComponentDefn deployedComponent, VMControllerInterface vmController, MessageBus bus) {
    	this.hostName = hostName;
        this.vmID = vmID;
        this.vmComponent = deployedComponent;
        this.vmController = vmController;
        this.messageBus = bus;
        try {
            this.vmName = vmController.getName();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.vmControllerStub = getStub(vmController);
        
    }

    private Object getStub(VMControllerInterface controller) {
        if (controller == null) {
            return null;
        }
        return this.messageBus.export(controller, new Class[] {VMControllerInterface.class});
    }


    /**
     * Return VMControllerID that this binding represents.
     *
     * @return VMControllerID
     */
    public VMControllerID getVMControllerID() {
        return vmID;
    }


    /**
     * Return reference for VMController.
     * If VMController is running in this VM then return local reference.
     * Else return remote reference.
     *
     * @return VMController reference
     */
    public synchronized VMControllerInterface getVMController() {

        // if vmController is null then this must be a remote vm so return the stub.
        if (this.vmController != null) {
            return vmController;
        }
        if (this.vmControllerStub == null) {
        	return null;
        }
    	// when exported to the remote, use remote's message bus instance.
    	MessageBus bus = this.messageBus;
    	if(bus == null) {
    		bus = ResourceFinder.getMessageBus();
    	}        
        vmController = (VMControllerInterface)bus.getRPCProxy(this.vmControllerStub);
        return vmController;
    }

    public VMComponentDefn getDeployedComponent() {
       return this.vmComponent;
    }

    public String getHostName() {
    	return hostName;
    }

    public String getVMName() {
    	return vmName;
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
        return "VMRegistryBinding: " + this.vmName + " <" + this.vmID + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}

