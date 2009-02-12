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

package com.metamatrix.common.config.api;

public interface SearchbaseConnectorComponentType {
    public static final String COMPONENT_TYPE_NAME = "SearchbaseConnector"; //$NON-NLS-1$

  /**
  * These connection properties used by the published searchbase
  * match those of com.metamatrix.connector.jdbc.JDBCPropertyNames.
  * The searchbase cannot reference JDBCPropertyNames because
  * common package cannot be dependent on the connector package.
  */
    public static final String DRIVER_CLASS = "Driver"; //$NON-NLS-1$
    public static final String PROTOCOL = "Protocol"; //$NON-NLS-1$
    public static final String DATABASE = "Database"; //$NON-NLS-1$
    public static final String USERNAME = "User"; //$NON-NLS-1$
    public static final String PASSWORD = "Password"; //$NON-NLS-1$


}
