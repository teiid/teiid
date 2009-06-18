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

package com.metamatrix.common.application.exception;

import com.metamatrix.core.MetaMatrixCoreException;

/**
 * This exception is thrown when an application cannot be initialized, typically
 * due to an invalid initialization property value or the inability to 
 * start some subsystem due to an external error.
 */
public class ApplicationInitializationException extends MetaMatrixCoreException {

    /**
     * No-Arg Constructor
     */
    public ApplicationInitializationException(  ) {
        super( );
    }

    /**
     * Construct an exception
     * @param message
     */
    public ApplicationInitializationException(String message) {
        super(message);
    }


    /**
     * Construct an exception
     * @param e
     */
    public ApplicationInitializationException(Throwable e) {
        super(e);
    }

    /**
     * Construct an exception
     * @param e
     * @param message
     */
    public ApplicationInitializationException(Throwable e, String message) {
        super(e, message);
    }

	public ApplicationInitializationException(Throwable e, String errorCode, String msg) {
		super(e, errorCode, msg);
	}
	
	public ApplicationInitializationException(String errorCode, String msg) {
		super(errorCode, msg);
	}

} // END CLASS

