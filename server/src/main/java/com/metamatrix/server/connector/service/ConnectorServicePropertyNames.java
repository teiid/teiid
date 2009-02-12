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

/*
 * Date: Sep 25, 2003
 * Time: 4:38:45 PM
 */
package com.metamatrix.server.connector.service;

/**
 * ConnectorServicePropertyNames.
 */
public class ConnectorServicePropertyNames {

    /**
     * Don't allow instantiation.
     */
    private ConnectorServicePropertyNames() {
    }

    /** Metadata service - provides acecss to runtime metadata */
    public static final String METADATA_SERVICE = "dqp.metadata"; //$NON-NLS-1$
    
    public static final String CONNECTOR_CLASS_PATH = "ConnectorClassPath"; //$NON-NLS-1$
}
