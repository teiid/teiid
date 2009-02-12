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

package com.metamatrix.console.util;

import com.metamatrix.api.exception.MetaMatrixException;

/**
 * Wrapper class intended to represent any exception thrown by an API method which
 * would appear to indicate that the Console sent an illegal request to the
 * server.  Included are InvalidRequestIDException, InvalidSessionException,
 * and InvalidDataSourceException.  Note that ComponentNotFoundException is
 * thrown separately.
 */
public class InvalidRequestException extends MetaMatrixException {
    
    /**
     * No-arg costructor required by Externalizable semantics
     */
    public InvalidRequestException() {
        super();
    }
    
    public InvalidRequestException(Exception ex) {
        super(ex, "Invalid request by Console.");
    }

    public String toString() {
        String str = "InvalidRequestException: " + super.toString();
        return str;
    }
}

