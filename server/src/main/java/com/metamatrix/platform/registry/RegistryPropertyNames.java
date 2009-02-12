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

package com.metamatrix.platform.registry;

public class RegistryPropertyNames {

    // properties need to connect to orb when running rmi-iiop
    public static final String ORB_INITIAL_PORT = "appserver.orb.initialPort"; //$NON-NLS-1$
    public static final String ORB_INITIAL_HOST = "appserver.orb.initialHost"; //$NON-NLS-1$
    
    
    public static final String APP_SERVER_URL = "metamatrix.appserver.URL"; //$NON-NLS-1$
    public static final String APP_SERVER_CONTEXT_FACTORY = "metamatrix.appserver.contextFactory"; //$NON-NLS-1$
    
    public static final String APP_SERVER_USERNAME = "metamatrix.appserver.Username"; //$NON-NLS-1$
    public static final String APP_SERVER_PASSWORD = "metamatrix.appserver.Password"; //$NON-NLS-1$
    
    public static final String APP_SERVER_PLATFORM = "metamatrix.appserver.Platform"; //$NON-NLS-1$
    

}

