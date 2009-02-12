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

package com.metamatrix.core.id;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixCoreException;

/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class InvalidIDException extends MetaMatrixCoreException {
    private static final String INVALID_ID_MESSAGE = CorePlugin.Util.getString("InvalidIDException.Invalid_ID_1"); //$NON-NLS-1$

    /**
     * No-Arg Constructor
     */
    public InvalidIDException(  ) {
        super( );
    }


    /**
     * Construct an instance of InvalidIDException.
     * @param message
     */
    public InvalidIDException(String message) {
        super(message);
    }

    /**
     * Construct an instance of InvalidIDException.
     * @param e
     */
    public InvalidIDException(Throwable e) {
        super(e,INVALID_ID_MESSAGE);
    }


    /**
     * Construct an instance of InvalidIDException.
     * @param e
     * @param message
     */
    public InvalidIDException(Throwable e, String message) {
        super(e, message);
    }

}
