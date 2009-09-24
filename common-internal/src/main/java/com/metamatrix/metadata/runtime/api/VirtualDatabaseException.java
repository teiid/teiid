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

package com.metamatrix.metadata.runtime.api;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
/**
 * The base exception from which all Runtime Metadata Exceptions extend.
 */
public class VirtualDatabaseException extends MetaMatrixProcessingException {

    public static final String NO_MODELS = "1"; //$NON-NLS-1$
    public static final String MODEL_NON_DEPLOYABLE_STATE = "2";  //$NON-NLS-1$
    public static final String VDB_NON_DEPLOYABLE_STATE = "3";  //$NON-NLS-1$

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public VirtualDatabaseException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public VirtualDatabaseException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public VirtualDatabaseException( String code, String message ) {
        super( code, message );
    }

    /**
     * Construct an instance from an exception to chain to this one.
     *
     * @param e An exception to nest within this one
     */
    public VirtualDatabaseException(Exception e) {
        super(e);
    }    
    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param code A code denoting the exception
     * @param e An exception to nest within this one
     */
    public VirtualDatabaseException( Exception e, String message ) {
        super( e, message );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public VirtualDatabaseException( Exception e, String code, String message ) {
        super( e, code, message );
    }
}

