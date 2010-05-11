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

import org.teiid.core.TeiidException;

/**
 * An error occurred in communication between client and server.  This 
 * error may or may not be recoverable.  Generally the communication 
 * transport should be able to tell the difference and recover if possible.
 */
public class CommunicationException extends TeiidException {
    /**
     * No-Arg Constructor
     */
    public CommunicationException(  ) {
        super( );
    }

    /**
     * @param message
     */
    public CommunicationException(String message) {
        super(message);
    }

    /**
     * @param e
     */
    public CommunicationException(Throwable e) {
        super(e);
    }

    /**
     * @param e
     * @param message
     */
    public CommunicationException(Throwable e, String message) {
        super(e, message);
    }
}
