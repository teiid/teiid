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

package com.metamatrix.soap.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This is the interface that the Data Service feature uses to get Connections.
 */
public interface ConnectionSource {

    /*
     * These property keys are intended to be used to denote property values in the Properties object passed to the getConnection ()
     * method of this interface.
     */

    /**
     * This is a key that is used for the JDBC Connection URL for an MM Server.
     */
    public static final String SERVER_URL = "ServerURL"; //$NON-NLS-1$

    /**
     * This is a property key that is used for the username used to connect to an MM Server.
     */
    public static final String USERNAME = "Username"; //$NON-NLS-1$

    /**
     * This is a property key that is used for the password used to connect to an MM Server.
     */
    public static final String PASSWORD = "Password"; //$NON-NLS-1$

    public Connection getConnection(Properties connectionProperties) throws SQLException;
}
