/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.client;

import java.io.Serializable;


/** 
 * Represents all information needed to connect to the server and create a session.
 * @since 4.3
 */
public interface ConnectionInfo {

    /**
     * This is not a JDBC url - just the mm://... part, maybe multiple hosts for failover
     * @param url The server url (hosts and ports)
     */
    void setServerUrl(String url);
    
    /**
     * Set user name 
     * @param user User name
     * @since 4.3
     */
    void setUser(String user);
    
    /**
     * Set password 
     * @param password Password
     * @since 4.3
     */
    void setPassword(String password);
    
    /**
     * Set the trusted payload 
     * @param trustedPayload
     * @since 4.3
     */
    void setTrustedPayload(Serializable trustedPayload);
 
    /**
     * Set the VDB name 
     * @param vdbName VDB name
     * @since 4.3
     */
    void setVDBName(String vdbName);
    
    /**
     * Set the VDB version (optional) 
     * @param vdbVersion VDB version, newest if not specified
     * @since 4.3
     */
    void setVDBVersion(String vdbVersion);
    
    /**
     * Set other optional property 
     * @param propName Property name
     * @param propValue Property value
     * @since 4.3
     */
    void setOptionalProperty(String propName, Object propValue);
    
}
