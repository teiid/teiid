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

package com.metamatrix.common.classloader;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * Wraps exceptions in a RuntimeException.
 * This class is here so the class loading driver package will not have any dependencies on other MetaMatrix packages.
 */
public class NestedException extends RuntimeException {
    private static final String CAUSED_BY = "Caused by: "; //$NON-NLS-1$
    private Exception cause;
    
    public NestedException(Exception exception) {
        this.cause = exception;
    }

    /* 
     * @see java.lang.Throwable#getMessage()
     */
    public String getMessage() {
        return super.getMessage() + " - " + cause.getMessage(); //$NON-NLS-1$
    }

    private void printCausedBy(PrintStream s) {
        s.print(CAUSED_BY);
    }
    
    private void printCausedBy(PrintWriter s) {
        s.print(CAUSED_BY);
    }
    
    /* 
     * @see java.lang.Throwable#printStackTrace()
     */
    public void printStackTrace() {
        super.printStackTrace();
        printCausedBy(System.out);
        cause.printStackTrace();
    }

    /* 
     * @see java.lang.Throwable#printStackTrace(java.io.PrintStream)
     */
    public void printStackTrace(PrintStream s) {
        super.printStackTrace(s);
        printCausedBy(s);
        cause.printStackTrace(s);
    }

    /* 
     * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
     */
    public void printStackTrace(PrintWriter s) {
        super.printStackTrace(s);
        printCausedBy(s);
        cause.printStackTrace(s);
    }
}
