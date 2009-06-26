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

package com.metamatrix.dqp.util;

public interface LogConstants {
	// add the new contexts to the Log4JUtil.java class, for configuration purpose
	public static final String CTX_DQP = "DQP"; //$NON-NLS-1$
    public static final String CTX_CONNECTOR = "CONNECTOR"; //$NON-NLS-1$
    public static final String CTX_BUFFER_MGR = "BUFFER_MGR"; //$NON-NLS-1$
    public static final String CTX_STORAGE_MGR = "STORAGE_MGR"; //$NON-NLS-1$
    public static final String CTX_TXN_LOG = "TXN_LOG"; //$NON-NLS-1$
    public static final String CTX_EXTENSION_SOURCE = "EXTENSION_MODULE"; //$NON-NLS-1$
	public static final String CTX_COMMANDLOGGING = "COMMAND_LOG"; //$NON-NLS-1$
	public static final String CTX_AUDITLOGGING = "AUDIT_LOG"; //$NON-NLS-1$
	public static final String CTX_ADMIN_API = "ADMIN_API"; //$NON-NLS-1$
	public static final String CTX_QUERY_SERVICE = "QUERY_SERVICE"; //$NON-NLS-1$
}
