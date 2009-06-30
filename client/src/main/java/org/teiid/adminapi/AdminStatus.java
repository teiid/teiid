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

package org.teiid.adminapi;


/**
 * Base interface of admin status objects. 
 * Status objects are returned by some admin methods to indicate warnings or additional information,
 * that doesn't belong in an Exception.
 * 
 * @since 4.3
 */
public interface AdminStatus {

    
    /**
     * Status code indicating an unknown status
     */
    public final static int CODE_UNKNOWN = -1;
    /**
     * Status code indicating that the operation succeeded.
     */
    public final static int CODE_SUCCESS = 0;
    
    /**
     * Warning status code indicating that an object could not be decrypted.
     */
    public final static int CODE_DECRYPTION_FAILED = -101;
    
    
    
    
    /**
     * Get the status code.
     * This will be one of the status codes specified by the constants <code>AdminStatus.CODE_*</code>.
     * @return String the unique Identifier
     * @since 4.3
     */
    int getCode();

    /**
     * Get the status message.
     * @return String Name
     * @since 4.3
     */
    String getMessage();

}
