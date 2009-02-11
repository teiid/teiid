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

package com.metamatrix.jdbc;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;


/** 
 * @since 4.3
 */
public class EmbeddedSQLException extends SQLException {
    private Throwable parentException = null;
    private String message = null;
    
    public EmbeddedSQLException(Throwable e) {
        this.parentException = e;
    }
    
    public EmbeddedSQLException(Throwable e, String msg) {
        this.parentException = e;
        this.message = msg;
    }
    
    public EmbeddedSQLException(String msg) {
        this.message = msg;
    }

    /** 
     * @see java.lang.Throwable#getMessage()
     * @since 4.3
     */
    public String getMessage() {
        if (message != null && parentException != null) {
            return message + " source:"+parentException.getMessage(); //$NON-NLS-1$
        }
        else if (message != null && parentException == null ){
            return message;
        }
        return parentException.getMessage();
    }

    /** 
     * @see java.lang.Throwable#printStackTrace()
     * @since 4.3
     */
    public void printStackTrace() {
        if (parentException != null) {
            parentException.printStackTrace();
        }
        else {
            super.printStackTrace();
        }
    }

    /** 
     * @see java.lang.Throwable#printStackTrace(java.io.PrintStream)
     * @since 4.3
     */
    public void printStackTrace(PrintStream s) {
        if (parentException != null) {
            parentException.printStackTrace(s);
        }
        else {
            super.printStackTrace(s);
        }
    }

    /** 
     * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
     * @since 4.3
     */
    public void printStackTrace(PrintWriter s) {
        if (parentException != null) {
            parentException.printStackTrace(s);
        }
        else {
            super.printStackTrace(s);
        }
    }

}
