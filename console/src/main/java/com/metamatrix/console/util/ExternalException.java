/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
 * Wrapper class intended to represent any exception apparently caused by the
 * server or anything accessed through the server not producing an expected result.
 */
public class ExternalException extends MetaMatrixException {

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public ExternalException() {
        super();
    }
    
    public ExternalException(Exception ex) {
        super(ex, "External Exception");
    }

    public ExternalException(String msg, Exception ex) {
        super(ex, msg);
    }
    
    public String toString() {
        String str = "ExternalException: " + super.toString();
        return str;
    }
}
