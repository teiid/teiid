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

import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.server.ResourceFinder;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class RegistryBinding<T> implements Serializable {

    private String hostName;
    private long startTime = System.currentTimeMillis();
    private Object stub;
    private transient boolean local;
    private transient T bindObject;
    private transient MessageBus messageBus;
    
    public RegistryBinding(T bindObject, String hostName,
			MessageBus messageBus) {
		this.bindObject = bindObject;
    	this.hostName = hostName;
		this.messageBus = messageBus;
		if (this.bindObject != null) {
			this.stub = this.messageBus.export(this.bindObject, this.bindObject.getClass().getInterfaces());
		}
		this.local = true;
	}

    public synchronized T getBindObject() {
    	if (this.bindObject != null) {
    		return bindObject;
    	}
    	if (this.stub == null) {
    		return null;
    	}
    	// when exported to the remote, use remote's message bus instance.
    	this.bindObject = (T)this.getMessageBus().getRPCProxy(this.stub);
    	return this.bindObject;
    }
    
    public synchronized void invalidateBindObject() {
		this.bindObject = null;
    	if (this.stub != null) {
    		this.getMessageBus().unExport(stub);
    		this.stub = null;
    	}
    }

    public String getHostName() {
    	return hostName;
    }

    public long getStartTime() {
		return startTime;
	}
    
    public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
    
    public boolean isLocal() {
		return local;
	}

	private MessageBus getMessageBus() {
		if (this.messageBus == null) {
			this.messageBus = ResourceFinder.getMessageBus();
		}
		return messageBus;
	}
}

