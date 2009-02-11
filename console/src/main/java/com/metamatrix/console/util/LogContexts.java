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

//**************************************************************************//
package com.metamatrix.console.util;

/**
 * LogContexts defines a set of contexts for using the LogManager.
 */
public interface LogContexts {

    // Generic application-related constants
    public static final String GENERAL        = "GENERAL";
    public static final String INITIALIZATION = "INITIALIZATION";
    public static final String CONFIG         = "CONFIGURATION";
    public static final String PREFS          = "USER_PREFERENCES";

    // Functionality-related constants
    public static final String SUMMARY        = "SUMMARY";
    public static final String SESSIONS       = "SESSIONS";
    public static final String SYSTEMLOGGING  = "SYSTEMLOGGING";
    public static final String PROPERTIES     = "PROPERTIES";
    public static final String PSCDEPLOY      = "PSC-DEPLOYMENT";
    public static final String USERS          = "USERS";
    public static final String ROLES          = "ROLES";
    public static final String RUNTIME        = "RUNTIME";
    public static final String LOG_SETTINGS   = "LOG_SETTINGS";

    // MetaMatrix Server panels
    public static final String VIRTUAL_DATABASE   = "VIRTUAL_DATABASE";
    public static final String QUERIES            = "QUERIES";
    public static final String TRANSACTIONS       = "TRANSACTIONS";
    public static final String CONNECTORS         = "CONNECTORS";
    public static final String CONNECTOR_BINDINGS = "CONNECTOR_BINDINGS";
    public static final String ENTITLEMENTS       = "ROLES";
    public static final String METADATA_ENTITLEMENTS = "METADATA_ROLES";
    public static final String EXTENSION_SOURCES  = "EXTENSION_SOURCES";
    public static final String RESOURCE_POOLS     = "RESOURCE_POOLS";
    public static final String RESOURCES			= "RESOURCES";

    public static Object[] logMessageContexts = { GENERAL,
                                                  INITIALIZATION,
                                                  CONFIG,
                                                  PREFS,
                                                  SUMMARY,
                                                  SESSIONS,
                                                  SYSTEMLOGGING,
                                                  PROPERTIES,
                                                  PSCDEPLOY,
                                                  USERS,
                                                  ROLES,
                                                  RUNTIME,
                                                  QUERIES,
                                                  TRANSACTIONS,
                                                  CONNECTOR_BINDINGS,
                                                  ROLES,
                                                  METADATA_ENTITLEMENTS,
                                                  EXTENSION_SOURCES,
                                                  RESOURCE_POOLS,
                                                  RESOURCES
                                                };

    public static Object[] logMessageLevels = { " 0 - None",
                                                 " 1 - Critical",
                                                 " 2 - Error",
                                                 " 3 - Warning  (Default)",
                                                 " 4 - Information",
                                                 " 5 - Detail",
                                                 " 6 - Trace  (Verbose)",
                                              };
}
