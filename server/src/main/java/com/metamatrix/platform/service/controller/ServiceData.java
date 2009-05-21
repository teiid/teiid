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

package com.metamatrix.platform.service.controller;

import java.io.Serializable;
import java.util.Date;

import com.metamatrix.platform.service.api.ServiceID;

public class ServiceData implements Serializable {
	private int state;
	private String serviceType;
	private String routingId;
	private String instanceName;
	private ServiceID id;
	private Date startTime;
	private Date stateChangeTime = new Date();
	private Throwable initException;
	private transient boolean dirty;

	public ServiceData(int state) {
		this.state = state;
	}

	public int getState() {
		return state;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	public ServiceID getId() {
		return id;
	}

	public void setId(ServiceID id) {
		this.id = id;
	}

	public Date getStartTime() {
		return startTime;
	}

	void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getStateChangeTime() {
		return stateChangeTime;
	}

	void setStateChangeTime(Date stateChangeTime) {
		this.stateChangeTime = stateChangeTime;
	}

	public Throwable getInitException() {
		return initException;
	}

	void setInitException(Throwable initException) {
		this.initException = initException;
	}
	
	public boolean isDirty() {
		return dirty;
	}
	
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
    /**
     * Update state and stateChangedTime with new state;
     * If newState == state then do nothing.
     *
     * @param int new state of service
     */
    public synchronized void updateState(int newState) {
        if (state != newState) {
        	this.state = newState;
    		this.dirty = true;
            setStateChangeTime(new Date());
        }
    }
    
    public String getRoutingId() {
		return routingId;
	}
    
    public void setRoutingId(String routingId) {
		this.routingId = routingId;
	}

}