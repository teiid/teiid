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

package org.teiid.logging;


public interface LogConstants {
	// add the new contexts to the Log4JUtil.java class, for configuration purpose
	public static final String CTX_SECURITY = "org.teiid.SECURITY"; //$NON-NLS-1$
	public static final String CTX_TRANSPORT = "org.teiid.TRANSPORT"; //$NON-NLS-1$
	public static final String CTX_QUERY_PLANNER = "org.teiid.PLANNER"; //$NON-NLS-1$
	public static final String CTX_DQP = "org.teiid.PROCESSOR"; //$NON-NLS-1$
	public static final String CTX_CONNECTOR = "org.teiid.CONNECTOR"; //$NON-NLS-1$
	public static final String CTX_BUFFER_MGR = "org.teiid.BUFFER_MGR"; //$NON-NLS-1$
	public static final String CTX_TXN_LOG = "org.teiid.TXN_LOG"; //$NON-NLS-1$
	public static final String CTX_COMMANDLOGGING = "org.teiid.COMMAND_LOG"; //$NON-NLS-1$
	public static final String CTX_AUDITLOGGING = "org.teiid.AUDIT_LOG"; //$NON-NLS-1$
	public static final String CTX_ADMIN_API = "org.teiid.ADMIN_API"; //$NON-NLS-1$
	public static final String CTX_RUNTIME = "org.teiid.RUNTIME"; //$NON-NLS-1$
	public static final String CTX_ODBC = "org.teiid.ODBC"; //$NON-NLS-1$
	public static final String CTX_ODATA = "org.teiid.ODATA"; //$NON-NLS-1$
	
	// Query contexts
	public static final String CTX_FUNCTION_TREE = CTX_QUERY_PLANNER + ".FUNCTION_TREE"; //$NON-NLS-1$
	public static final String CTX_QUERY_RESOLVER = CTX_QUERY_PLANNER + ".RESOLVER"; //$NON-NLS-1$
	public static final String CTX_XML_PLANNER = CTX_QUERY_PLANNER + ".XML_PLANNER"; //$NON-NLS-1$
	public static final String CTX_XML_PLAN = CTX_DQP + ".XML_PLAN"; //$NON-NLS-1$
	public static final String CTX_MATVIEWS = CTX_DQP + ".MATVIEWS"; //$NON-NLS-1$
	
	public static final String CTX_WS = LogConstants.CTX_CONNECTOR + ".WS"; //$NON-NLS-1$
}
