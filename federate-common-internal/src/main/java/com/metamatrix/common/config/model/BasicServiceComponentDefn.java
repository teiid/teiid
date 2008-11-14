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

package com.metamatrix.common.config.model;

import java.io.Serializable;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;



public class BasicServiceComponentDefn extends BasicComponentDefn implements ServiceComponentDefn, Serializable {

    private boolean isQueuedService;
    private String routingUUID;

    public BasicServiceComponentDefn(ConfigurationID configurationID, ServiceComponentDefnID componentID, ComponentTypeID typeID) {
        super(configurationID, componentID, typeID);
 
    }

    protected BasicServiceComponentDefn(BasicServiceComponentDefn component) {
        super(component);
        setIsQueuedService(component.isQueuedService());
        this.setRoutingUUID(component.getRoutingUUID());
    }

    public boolean isQueuedService() {
        return isQueuedService;
    }

    public void setIsQueuedService(boolean isQueued) {
        isQueuedService = isQueued;
    }

    /**
     * Returns the String globally unique routing UUID for this
     * ServiceComponentDefn if it is a Connector Binding;
     * otherwise returns null.
     * @return String routing UUID
     */
    public String getRoutingUUID(){
        return this.routingUUID;
    }

    public void setRoutingUUID(String routingUUID){
        this.routingUUID = routingUUID;
    }



    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     *  this method.
     *  @return the object that is the clone of this instance.
     *  @throws CloneNotSupportedException if this object cannot be cloned
     */
    public synchronized Object clone() throws CloneNotSupportedException {
        return new BasicServiceComponentDefn(this);

    }

    /**
     * Returns a string representing the name of the object.  This has been
     * overriden for GUI display purposes - the Console only wants to display
     * the "name" (not the "fullname") of a component object.Me
     * @return the string representation of this instance.
     */
    public String toString(){
        return getDisplayableName(this.getName());
    }

    /**
     * Kludge method which, given the kludgey automatically-generated
     * service definition name of the form "PSCName#$%_ServiceDefinitionName",
     * will return just the end of that name, aka "ServiceDefinitionName".
     * If the name does not have the "#$%_" character combination, then the
     * name will be returned unmodified
     */
    public static final String getDisplayableName(String serviceDefinitionName){
    	return serviceDefinitionName;
    }

    //********************************************************
}

