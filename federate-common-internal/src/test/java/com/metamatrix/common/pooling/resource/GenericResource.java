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

package com.metamatrix.common.pooling.resource;

import com.metamatrix.common.pooling.NoOpResourceInterface;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.impl.BaseResource;

public class GenericResource extends BaseResource implements NoOpResourceInterface {

    private NoOpResourceInterface resource;
//    private String name;

    boolean alive= false;
    public GenericResource(NoOpResourceInterface resourceObject) {
        super();
        this.resource = resourceObject;

    }

 /**
 * Call isAlive to determine if the resource  is operating normally.  
 * A return of  <code>false</code> indicates the resource should not be used.
 * @return boolean true if the resource is operating normally 
 */
    public boolean checkIsResourceAlive() {
        return alive;
    }

    /**
     * Perform open on the resource object to make it avaialable for use.
     * @throws ResourcePoolException if an error occurs starting resource.
     */
    protected void performInit() throws ResourcePoolException {
          alive = true;
    }

 

    /**
     * Perform shutdown on the resource object and release all resources.
     */

    protected void performShutDown() {
        alive = false;
    }

    public void close() throws ResourcePoolException {
        this.closeResource();
    }

    public Object getObject() {
        return this.resource;
    }

    public String toString() {
        return resource.toString();
    }

}


