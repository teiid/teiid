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

package com.metamatrix.platform.service.api;

public interface ServiceState {
    public static final int STATE_NOT_INITIALIZED = 0;
    public static final int STATE_OPEN = 1;
    public static final int STATE_CLOSED = 2;
    public static final int STATE_FAILED = 3;
    public static final int STATE_INIT_FAILED = 4;
    public static final int STATE_NOT_REGISTERED = 5;
    public static final int STATE_DATA_SOURCE_UNAVAILABLE = 6;
    
    public final static String[] stateAsString = {"Not_Initialized", //$NON-NLS-1$
        "Running", //$NON-NLS-1$
        "Closed", //$NON-NLS-1$
        "Failed", //$NON-NLS-1$
        "Init_Failed", //$NON-NLS-1$
        "Not_Registered",  //$NON-NLS-1$
        "Data_Source_Unavailable"}; //$NON-NLS-1$    
}
