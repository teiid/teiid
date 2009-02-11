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

/*
 * Date: Mar 5, 2003
 * Time: 5:24:57 PM
 */
package com.metamatrix.common.util.exception;

import java.sql.SQLException;

/**
 * SQLExceptionUnroller.
 * <br>A utility to unroll a chained <code>SQLException</code>.</br>
 * <p>This utility is used to replace proprietary exceptions that may
 * not unmarshall in other VMs that do not have the proprietary libraries.</p>
 * 
 * TODO: test causes and next exceptions for non-Java then unroll
 */
public class SQLExceptionUnroller {

    /**
     * Unroll a chain of possibly proprietary <code>SQLException</code>s
     * and create a chain of of generic {@link java.sql.SQLException SQLExceptions}
     * with the original message (reason), SQLState, and vendorCode of the
     * <code>SQLException</code>s in the chain.
     * @param theException The original <code>SQLException</code> possibly
     * with a chain of child exceptions.
     * @return A <code>SQLException</code> mirroring the original
     * exception chain.
     */
    public static SQLException unRollException(final SQLException theException) {
        // Get msg, SQLState and vendorCode from top-level exception
        String SQLState = theException.getSQLState();
        int vendorCode = theException.getErrorCode();
        final SQLException outException = new SQLException(
				(SQLException.class != theException.getClass() ? (theException.getClass().getName() + ": ") : ("")) + theException.getMessage(), SQLState, vendorCode); //$NON-NLS-1$ //$NON-NLS-2$
        outException.setStackTrace(theException.getStackTrace());
        
        // Continue with any chained exceptions
        SQLException ei = theException.getNextException();
        if (ei != null) {
        	outException.setNextException(unRollException(ei));
        }
        return outException;
    }
    
}
