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

import java.util.Collection;
import java.util.Date;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.model.BasicServiceComponentDefn;

import com.metamatrix.platform.service.api.ServiceID;

public class ServiceData extends ComponentData {

    /** Identifies service for purpose of looking up in registry */
    private ServiceID serviceID;

    /**
     * Current state of service, this is updated by the service framework
     * whenever the state changes
     */
    private int currentState;

    /** Time of the last state change */
    private Date stateChangeTime;

    /** indicates if service is an essential service */
    private boolean essential;

    private DeployedComponent deployedComponent;

    /** defines service in configuration */
    private ComponentDefnID defnID;

    /** queue names for service */
    private Collection queueNames;
    
    /** initialization Exception */
    private Throwable initError;

    /**
     * Create new ServiceRegistryInstance
     *
     * @param serviceID Identifies service
     * @param serviceName Name of service
     * @param instanceName Name of service instance
     * @param defnID Component Definition ID
     * @param deployedComponent
     * @param queueNames Collection of queue names for the service
     * @param state
     * @param time
     * @param essential, true indicates service is an essential service and cannot be shutdown if there are no other similiar services running.
     * @param deployed, true indicates service is deployed
     * @param registered, true indicates service is registered with Regisitry
     * @param initError, Exception that occured during initialization
     * @throws RegistryException if an an error occurs creating remote instance of service.
     */
    public ServiceData(ServiceID serviceID, String serviceName, String instanceName,
                                  ComponentDefnID defnID, DeployedComponent deployedComponent,
                                  Collection queueNames,
                                  int state, Date time, boolean essential, boolean deployed,
                                  boolean registered, Throwable initError) {

//        super(serviceName, deployed, registered);
        super(BasicServiceComponentDefn.getDisplayableName(deployedComponent.getName()), deployed, registered);
        this.serviceID = serviceID;
        this.defnID = defnID;
        this.deployedComponent = deployedComponent;
        this.queueNames = queueNames;
        this.currentState = state;
        this.stateChangeTime = time;
        this.essential = essential;
        this.initError = initError;
        computeHashCode();
    }

    /**
     * Return collection of queue names for the service.
     */
    public Collection getQueueNames() {
        return this.queueNames;
    }

    /**
     * Return ServiceID for this binding
     * @return ServiceID
     */
    public ServiceID getServiceID() {
        return this.serviceID;
    }

    public int getCurrentState() {
        return this.currentState;
    }

    public Date getStateChangeTime() {
        return this.stateChangeTime;
    }

    /**
     * Return ComponentDefnID for Service.
     *
     * @return ComponentDefnID
     */
    public ComponentDefnID getComponentDefnID() {
        return this.defnID;
    }

    public DeployedComponent getDeployedComponent() {
        return this.deployedComponent;
    }

    public Throwable getInitError() {
        return this.initError;
    }
    
    public boolean isEssential() {
        return essential;
    }

    private void computeHashCode() {
        hashCode = defnID.hashCode();
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
        if ( obj instanceof ServiceData ) {

            ServiceData that = (ServiceData) obj;
            return deployedComponent.equals(that.getDeployedComponent());
//            return defnID.equals(that.getComponentDefnID());
        }

        // Otherwise not comparable ...
        return false;
    }
}

