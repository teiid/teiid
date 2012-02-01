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
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class TeiidComponentException extends TeiidException {

	private static final long serialVersionUID = 5853804556425201591L;

    public TeiidComponentException(  ) {
        super(  );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TeiidComponentException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TeiidComponentException( Throwable e ) {
		super( e );
    }

    public TeiidComponentException(BundleUtil.Event code, final String message) {
        super(code, message);
    }    
    
    public TeiidComponentException(BundleUtil.Event code, Throwable e, final String message) {
        super(code, e, message);
    }    
    
    public TeiidComponentException(BundleUtil.Event code, Throwable e) {
        super(code, e);
    }     

    public TeiidComponentException( Throwable e, String message ) {
        super( e, message );
    }

} // END CLASS

