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

package org.teiid.core.types;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;

/**
 * The exception is thrown when an error occurs during data transformation,
 * typically a formatting error or an invalid input type.
 */
public class TransformationException extends TeiidProcessingException {

	private static final long serialVersionUID = -4112567582638012800L;

	/**
     * No-Arg Constructor
     */
    public TransformationException(  ) {
        super( );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TransformationException( String message ) {
        super(message);
    }

    public TransformationException(Exception e) {
        super(e);
    }
    
    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param code A code denoting the exception
     * @param e An exception to nest within this one
     */
    public TransformationException( Exception e, String message ) {
        super( e, message );
    }
    public TransformationException(BundleUtil.Event event, String message) {
        super(event, message);
    }
    public TransformationException(BundleUtil.Event event, Throwable t, String message) {
        super(event, t, message);
    }    
    
}

