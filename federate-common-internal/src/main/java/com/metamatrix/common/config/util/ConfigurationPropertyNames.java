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

package com.metamatrix.common.config.util;

public interface ConfigurationPropertyNames {

    static final String APPLICATION_CREATED_BY = "ApplicationCreatedBy"; //$NON-NLS-1$
    static final String APPLICATION_VERSION_CREATED_BY = "ApplicationVersion"; //$NON-NLS-1$
    static final String USER_CREATED_BY = "UserCreatedBy"; //$NON-NLS-1$
    static final String CONFIGURATION_VERSION = "ConfigurationVersion"; //$NON-NLS-1$
    static final String METAMATRIX_SYSTEM_VERSION = "MetaMatrixSystemVersion"; //$NON-NLS-1$
    static final String TIME = "Time"; //$NON-NLS-1$

    // at 4.2 is where the configuration format changes, so anything prior
    // to this version will use the old (3.0) import/export utility
    static final String MM_CONFIG_4_2_VERSION = "4.2"; //$NON-NLS-1$
    static final String MM_CONFIG_3_0_VERSION = "3.0"; //$NON-NLS-1$
    
    
    // NOTE: if the latest config version is changed, then the com.metamatrix.vdb.internal.def.VDBDefPropertyNames 
    // needs to be change, which will impact the importing of .DEF files.
    static final double MM_LATEST_CONFIG_VERSION = 4.2;
    
//    interface ConnectorTypeName {
//
//        static final String ORACLE_CONNECTOR = "Oracle JDBC Connector"; //$NON-NLS-1$
//        static final String DB2_HIT_CONNECTOR = "DB2 Hit Driver JDBC Connectory"; //$NON-NLS-1$
//        static final String DB2_IBM_CONNECTOR = "DB2 IBM Driver JDBC Connector"; //$NON-NLS-1$
//        static final String SYBASE_CONNECTOR = "Sybase JConnect JDBC Connector"; //$NON-NLS-1$
//        static final String MSSQL_CONNECTOR = "SQL Server JDBC Connector"; //$NON-NLS-1$
//
//    }
    
}
