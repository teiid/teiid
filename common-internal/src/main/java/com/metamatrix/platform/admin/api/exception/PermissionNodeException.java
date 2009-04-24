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

package com.metamatrix.platform.admin.api.exception;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class PermissionNodeException extends MetaMatrixAdminException {
    
    // The missing resource name
    private String resourceName;
    
    /**
     * Get the name of the resource that was not found.
     * @return the resource name.
     */
    public String getResourceName() {
        return resourceName;
    }
    
    /**
     * No-arg CTOR
     */
    public PermissionNodeException(  ) {
        super(  );
    }
    

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public PermissionNodeException( String message, String resource ) {
        super( message );
        this.resourceName = resource;
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public PermissionNodeException( String code, String message, String resource ) {
        super( code, message );
        this.resourceName = resource;
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public PermissionNodeException( Throwable e, String message, String resource ) {
        super( e, message );
        this.resourceName = resource;
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public PermissionNodeException( Throwable e, String code, String message, String resource ) {
        super( e, code, message );
        this.resourceName = resource;
    }
    
    /**
     * Override getMessage to provide the resourceName.
     * @return The exception message with the resource name it pertains to.
     */
    public String getMessage() {
        return super.getMessage() + " Resource: " + this.resourceName; //$NON-NLS-1$
    }
}
