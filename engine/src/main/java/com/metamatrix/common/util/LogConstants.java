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

package com.metamatrix.common.util;

public interface LogConstants {
	// add the new contexts to the Log4JUtil.java class, for configuration purpose
	public static final String CTX_CONFIG = "CONFIG"; //$NON-NLS-1$
	public static final String CTX_COMMUNICATION = "COMMUNICATION"; //$NON-NLS-1$
    public static final String CTX_POOLING = "RESOURCE_POOLING"; //$NON-NLS-1$
	public static final String CTX_SESSION = "SESSION"; //$NON-NLS-1$
	public static final String CTX_MEMBERSHIP = "MEMBERSHIP"; //$NON-NLS-1$
	public static final String CTX_AUTHORIZATION = "AUTHORIZATION"; //$NON-NLS-1$
	public static final String CTX_AUTHORIZATION_ADMIN_API = "AUTHORIZATION_ADMIN_API"; //$NON-NLS-1$
	public static final String CTX_SERVER= "Server"; //$NON-NLS-1$
	public static final String CTX_ADMIN = "ADMIN"; //$NON-NLS-1$	
}
