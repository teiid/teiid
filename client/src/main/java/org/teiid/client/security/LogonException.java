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

package org.teiid.client.security;

import org.teiid.core.BundleUtil;


/**
 * This exception is thrown when an attempt to log in to obtain a session has failed.
 * Possible reasons include but are not limited to:
 * <p>
 * <li>The limit on the number of sessions for the user has been reached, and a new session for the user could not be established;</li>
 * <li>An account for the user does not exist, has been frozen or has been removed; and</li>
 * <li>The credentials that were supplied did not authenticate the user.</li> 
 */
public class LogonException extends TeiidSecurityException {

	private static final long serialVersionUID = -4407245748107257061L;

	/**
     * No-Arg Constructor
     */
    public LogonException(  ) {
        super( );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public LogonException( BundleUtil.Event event, String message ) {
        super(event, message);
    }
    
    public LogonException( BundleUtil.Event event, Throwable t, String message ) {
        super(event, t, message );
    }    

    public LogonException(Throwable t) {
        super(t);
    }
}

