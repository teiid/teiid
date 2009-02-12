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

package com.metamatrix.server.util;

/**
 * Class that defines property constants for metaMatrix
 */
public final class SecurityRoles {

    public static class Name {
        public static final String SUPER_ADMIN          = "Super Admin"; //$NON-NLS-1$
        public static final String DEPLOYMENT_ADMIN     = "Deployment Admin"; //$NON-NLS-1$
        public static final String SYSTEM_ADMIN         = "System Admin"; //$NON-NLS-1$
        public static final String MONITOR_ADMIN        = "Monitor Admin"; //$NON-NLS-1$
        public static final String DATA_ADMIN           = "Data Admin"; //$NON-NLS-1$
        public static final String REMOTE_ADMIN         = "Remote Admin"; //$NON-NLS-1$
        public static final String USER_ADMIN           = "User Admin"; //$NON-NLS-1$
        public static final String GENERAL_ADMIN        = "General Admin"; //$NON-NLS-1$
    }

}
