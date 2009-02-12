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


public class PermissionNodeNotFoundException extends PermissionNodeException {

    /**
     * No-arg CTOR
     */
    public PermissionNodeNotFoundException(  ) {
        super(  );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public PermissionNodeNotFoundException( String message, String resource ) {
        super( message, resource );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public PermissionNodeNotFoundException( String code, String message, String resource ) {
        super( code, message, resource );
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public PermissionNodeNotFoundException( Throwable e, String message, String resource ) {
        super( e, message, resource );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public PermissionNodeNotFoundException( Throwable e, String code, String message, String resource ) {
        super( e, code, message, resource );
    }
}
