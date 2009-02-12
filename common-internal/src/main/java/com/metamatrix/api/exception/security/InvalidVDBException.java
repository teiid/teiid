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

package com.metamatrix.api.exception.security;

import com.metamatrix.api.exception.MetaMatrixProcessingException;

/**
 * Indicates that an invalid VDB was requested for logon.  This could be for
 * several reasons: invalid VDB name, invalid VDB version, VDB is not active,
 * etc.  
 */
public class InvalidVDBException extends MetaMatrixProcessingException {

    /**
     * 
     */
    public InvalidVDBException() {
        super();
    }

    /**
     * @param message
     */
    public InvalidVDBException(String message) {
        super(message);
    }

    /**
     * @param e
     */
    public InvalidVDBException(Throwable e) {
        super(e);
    }

    /**
     * @param code
     * @param message
     */
    public InvalidVDBException(String code, String message) {
        super(code, message);
    }

    /**
     * @param e
     * @param message
     */
    public InvalidVDBException(Throwable e, String message) {
        super(e, message);
    }

    /**
     * @param e
     * @param code
     * @param message
     */
    public InvalidVDBException(Throwable e, String code, String message) {
        super(e, code, message);
    }

}
