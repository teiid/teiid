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

import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.pooling.api.ResourcePoolMgr;



/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class ResourcePoolMgrBinding implements Serializable {

    /** ID of ResourcePoolMgr */
    private ResourcePoolMgrID rpmID;

    /**
     * Local reference to ResourcePoolMgr, this is transient to prevent it from
     * being sent to other vm's. Remote vm's must use the stub to access resourcePoolMgr
     */
    private transient ResourcePoolMgr resourcePoolMgr;

    /** Remote reference to ResourcePoolMgr */
    private Object resourcePoolMgrStub;

    private MessageBus messageBus;
    
    /**
     * Create a new instance of ResourcePoolMgr.
     *
     * @param rpmID Identifies ResourcePoolMgr binding represents
     * @param resourcePoolMgr ResourcePoolMgr implementation
     */
    public ResourcePoolMgrBinding(ResourcePoolMgrID rpmID, ResourcePoolMgr resourcePoolMgr, MessageBus bus) {

        this.rpmID = rpmID;
        this.resourcePoolMgr = resourcePoolMgr;
        this.messageBus = bus;
        this.resourcePoolMgrStub = getStub(resourcePoolMgr);
        
    }

    private Object getStub(ResourcePoolMgr poolMgr) {
        if (poolMgr == null) {
            return null;
        }

        return this.messageBus.export(poolMgr, new Class[] {ResourcePoolMgr.class});
    }


    /**
     * Return ResourcePoolMgrID that this binding represents.
     *
     * @return ResourcePoolMgrID
     */
    public ResourcePoolMgrID getID() {
        return rpmID;
    }

    /**
     * Return reference for ResourcePoolMgr.
     * If ResourcePoolMgr is running in this VM then return local reference.
     * Else return remote reference.
     *
     * @return ResourcePoolMgr reference
     */
    public synchronized ResourcePoolMgr getResourcePoolMgr() {
        if (this.resourcePoolMgr != null) {
            return resourcePoolMgr;
        }
        if (this.resourcePoolMgrStub == null) {
        	return null;
        }
        this.resourcePoolMgr = (ResourcePoolMgr)this.messageBus.getRPCProxy(resourcePoolMgrStub);
        return this.resourcePoolMgr;
    }


    public String toString() {
        return "ResourcePoolMgrBinding: <" + this.rpmID + ">"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}

