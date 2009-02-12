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

package com.metamatrix.core.vdb;


public interface VDBStatus {
    public static final short INCOMPLETE = 1;
    public static final short INACTIVE = 2;
    public static final short ACTIVE = 3;
    public static final short DELETED = 4;
    public static final short ACTIVE_DEFAULT = 3;
    
    final static String[] VDB_STATUS_NAMES = {
    	"Incomplete", //$NON-NLS-1$
        "Inactive",   //$NON-NLS-1$
        "Active", 	  //$NON-NLS-1$
        "Deleted", 	  //$NON-NLS-1$
        "Active-Default"};   //$NON-NLS-1$    
}
