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

package org.teiid.translator;

import org.teiid.core.TeiidException;

/**
 * An exception the connector writer can return in case of an 
 * error while using the connector.
 */
public class TranslatorException extends TeiidException{

	private static final long serialVersionUID = -5980862789340592219L;

	/**
     * No-arg constructor required by Externalizable semantics.
     */
    public TranslatorException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TranslatorException( String message ) {
        super( message );
    }
    
    public TranslatorException( String errorCode, String message ) {
        super( errorCode, message);
    }
    
    public TranslatorException( int errorCode, String message ) {
        super(message, Integer.toString(errorCode));
    } 
    
    public TranslatorException(Throwable e, int errorCode, String message ) {
        super(e, Integer.toString(errorCode), message);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param code A code denoting the exception
     * @param e An exception to nest within this one
     */
    public TranslatorException( Throwable e, String message ) {
        super(e, message);
    }  
    
    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TranslatorException( Throwable e ) {
        super( e );
    }  

}
