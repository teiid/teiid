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

package com.metamatrix.core.log;


/**
 * Implementation of {@link Logger} that does nothing.  An instance of this class is often useful
 * when a reference to a {@link Logger} is always expected to be non-null, but no logging is desired.
 */
public class NullLogger implements Logger {

    /**
     * @see com.metamatrix.core.log.Logger#log(int, java.lang.String)
     */
    public void log(int severity, String message) {

    }

    /**
     * @see com.metamatrix.core.log.Logger#log(int, java.lang.Throwable, java.lang.String)
     */
    public void log(int severity, Throwable t, String message) {

    }

}
