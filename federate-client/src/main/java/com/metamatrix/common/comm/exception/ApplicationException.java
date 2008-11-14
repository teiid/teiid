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

package com.metamatrix.common.comm.exception;

import com.metamatrix.core.MetaMatrixCoreException;

/**
 * Represents an exception that occurred in the application.  This is 
 * not a failure in the communication transport or framework, rather this
 * is an application specific error, usually something that is specific to an incoming
 * message to the server.
 */
public class ApplicationException extends MetaMatrixCoreException {

    /**
     * No-Arg Constructor
     */
    public ApplicationException(  ) {
        super( );
    }

    /**
     * @param message
     */
    public ApplicationException(String message) {
        super(message);
    }

    /**
     * @param e
     */
    public ApplicationException(Throwable e) {
        super(e);
    }

    /**
     * @param e
     * @param message
     */
    public ApplicationException(Throwable e, String message) {
        super(e, message);
    }
}
