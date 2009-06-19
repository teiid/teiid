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

package com.metamatrix.query.util;


public interface LogConstants {

//**********************************************************************
//PLEASE NOTE:!!!!!!!!!!!!!!!!!
//All constants defined here should also be defined in
//com.metamatrix.common.util.LogContextsUtil
//**********************************************************************

	// Query contexts
    public static final String CTX_FUNCTION_TREE = "FUNCTION_TREE"; //$NON-NLS-1$
    public static final String CTX_QUERY_PLANNER = "QUERY_PLANNER"; //$NON-NLS-1$
    public static final String CTX_QUERY_RESOLVER = "QUERY_RESOLVER"; //$NON-NLS-1$
    public static final String CTX_XML_PLANNER = "XML_QUERY_PLANNER"; //$NON-NLS-1$
    public static final String CTX_XML_PLAN = "XML_PLAN"; //$NON-NLS-1$
	
}
