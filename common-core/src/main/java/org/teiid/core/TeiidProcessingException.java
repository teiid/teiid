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

package org.teiid.core;


/**
 * This exception is a superclass for exceptions that are thrown during 
 * processing as a result of user input.  This exception is the result
 * of handling a user request, not the result of an internal error.
 */
public class TeiidProcessingException extends TeiidException {

	private static final long serialVersionUID = -4013536109023540872L;

	/**
     * No-arg Constructor
     */
    public TeiidProcessingException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TeiidProcessingException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TeiidProcessingException( Throwable e ) {
		super( e );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public TeiidProcessingException(BundleUtil.Event code, Throwable t, String message ) {
        super(code, t, message );
    }
    
    public TeiidProcessingException(BundleUtil.Event code, final String message) {
        super(code, message);
    }     
    
    public TeiidProcessingException(BundleUtil.Event code, Throwable t) {
        super(code, t);
    }     

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public TeiidProcessingException( Throwable e, String message ) {
        super( e, message );
    }

} // END CLASS

