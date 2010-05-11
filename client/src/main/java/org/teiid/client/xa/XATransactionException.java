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

package org.teiid.client.xa;

import javax.transaction.xa.XAException;

import org.teiid.core.TeiidProcessingException;


/**
 * Exception which occurs if an error occurs within the server that is
 * XA transaction-related.
 */
public class XATransactionException extends TeiidProcessingException {
	private static final long serialVersionUID = 5685144848609237877L;
	private int errorCode = XAException.XAER_RMERR;
    
    /**
     * No-Arg Constructor
     */
    public XATransactionException(  ) {
        super( );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public XATransactionException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public XATransactionException( Throwable e ) {
		super( e );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public XATransactionException( int code, String message ) {
        super( message );
        this.errorCode = code;
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     */
    public XATransactionException( Throwable e, String message ) {
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
    public XATransactionException( Throwable e, int code, String message ) {
        super( e, message );
        this.errorCode = code;
    }
    
    public XATransactionException( Throwable e, int code ) {
        super( e );
        this.errorCode = code;
    }
    
    public XAException getXAException() {
        Throwable actualException = getCause();
        if (actualException instanceof XAException) {
            return (XAException)actualException;
        }
        return new XAException(errorCode);
    }

} // END CLASS

