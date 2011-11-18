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
 * An <code>AdminComponentException</code> is thrown when an error occurs as a
 * result of an internal component error.
 *
 * <p>This exception class is capable of containing multiple exceptions.  See
 * {@link AdminException} for details.
 */
public final class AdminComponentException extends AdminException {

	private static final long serialVersionUID = 7117721841515240902L;


    public AdminComponentException() {
        super();
    }

    /**
     * Construct with a message.
     * @param msg the error message.
     * @since 4.3
     */
    public AdminComponentException(String msg) {
        super(msg);
    }
    
    public AdminComponentException(Throwable cause) {
    	super(cause);
    }

    /**
     * Construct with an optional error code and a message.
     * @param code an optional error code
     * @param msg the error message.
     * @since 4.3
     */
    public AdminComponentException(int code, String msg) {
        super(code, msg);
    }
    
    public AdminComponentException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AdminComponentException(int code, String msg, Throwable cause) {
        super(code, msg, cause);
    }

}
