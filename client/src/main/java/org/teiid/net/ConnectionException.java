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

package org.teiid.net;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidException;

/**
 * This exception indicates that an error has occurred during connection.  There
 * are many possible reasons for this, but the most likely is a problem with
 * connection parameters.  
 */
public class ConnectionException extends TeiidException {
	private static final long serialVersionUID = -5647655775983865084L;

	/**
     * No-Arg Constructor
     */
    public ConnectionException(  ) {
        super( );
    }

    /**
     * @param message
     */
    public ConnectionException(String message) {
        super(message);
    }

    /**
     * @param e
     */
    public ConnectionException(Throwable e) {
        super(e);
    }

    /**
     * @param e
     * @param message
     */
    public ConnectionException(Throwable e, String message) {
        super(e, message);
    }
    
    public ConnectionException(BundleUtil.Event event, Throwable e, String message) {
        super(event, e, message);
    }    
}
