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

package com.metamatrix.platform.security.util;

import com.metamatrix.common.util.LogCommonConstants;

public interface LogSecurityConstants extends
                                     LogCommonConstants {

    // **********************************************************************
    // PLEASE NOTE:!!!!!!!!!!!!!!!!!
    // All constants defined here should also be defined in
    // com.metamatrix.common.util.LogContextsUtil
    // **********************************************************************

    // Contexts
    public static final String CTX_CLIENT_MONITOR = "CLIENT_MONITOR"; //$NON-NLS-1$
    public static final String CTX_SESSION_MONITOR = "SESSION_MONITOR"; //$NON-NLS-1$
    public static final String CTX_SESSION_CLEANUP = "SESSION_CLEANUP"; //$NON-NLS-1$
    public static final String CTX_SESSION = "SESSION"; //$NON-NLS-1$
    public static final String CTX_SESSION_CACHE = "SESSION_CACHE"; //$NON-NLS-1$
    public static final String CTX_MEMBERSHIP = "MEMBERSHIP"; //$NON-NLS-1$
    public static final String CTX_AUTHORIZATION = "AUTHORIZATION"; //$NON-NLS-1$
    public static final String CTX_AUDIT = "AUDIT"; //$NON-NLS-1$

}
