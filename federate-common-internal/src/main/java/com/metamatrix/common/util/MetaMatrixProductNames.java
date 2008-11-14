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

package com.metamatrix.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.core.util.MetaMatrixProductVersion;

/**
 * <p>
 * This class records the official names of the various MetaMatrix products
 * and subsystems (each product has one or more subsystem).  This class
 * is composed of embedded inner classes which reflect the embedding of
 * subsystems within a product.  Note that this class presents the known
 * product names; a MetaMatrix system may not necessarily have all products
 * installed and available for administration.
 * </p><p>
 * This class also records the String classname of each Admin API
 * Facade Impl subsystem (which is needed by the Admin API
 * and is not intended for client use).  Use {@link com.metamatrix.platform.admin.api.AdminAPIConnection}
 * to dynamically retrieve the list of installed products and subsystems.
 * </p>
 */
public final class MetaMatrixProductNames extends MetaMatrixProductVersion {

    private static Map productsToSubsystems;
    private static Map subsystemsToAPIClassname;

    public static class Platform {
        public static final String PRODUCT_NAME = PLATFORM_TYPE_NAME;
        public static class SubSystemNames {
            public static final String CONFIGURATION = "Configuration"; //$NON-NLS-1$
            public static final String RUNTIME_STATE = "Runtime State"; //$NON-NLS-1$
            public static final String MEMBERSHIP = "Membership"; //$NON-NLS-1$
            public static final String SESSION = "Session"; //$NON-NLS-1$
            public static final String AUTHORIZATION = "Authorization"; //$NON-NLS-1$
            public static final String EXTENSION_SOURCE = "Extension Source"; //$NON-NLS-1$
        }
        public static class SubSystemAPIClassnames {
            public static final String CONFIGURATION = "com.metamatrix.platform.admin.apiimpl.ConfigurationAdminAPIFacade"; //$NON-NLS-1$
            public static final String RUNTIME_STATE = "com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIFacade"; //$NON-NLS-1$
            public static final String MEMBERSHIP = "com.metamatrix.platform.admin.apiimpl.MembershipAdminAPIFacade"; //$NON-NLS-1$
            public static final String SESSION = "com.metamatrix.platform.admin.apiimpl.SessionAdminAPIFacade"; //$NON-NLS-1$
            public static final String AUTHORIZATION = "com.metamatrix.platform.admin.apiimpl.AuthorizationAdminAPIFacade"; //$NON-NLS-1$
            public static final String EXTENSION_SOURCE = "com.metamatrix.platform.admin.apiimpl.ExtensionSourceAdminAPIFacade"; //$NON-NLS-1$
        }
    }

    public static class MetaMatrixServer {
        public static final String PRODUCT_NAME = METAMATRIX_SERVER_TYPE_NAME;
        public static class SubSystemNames {
            public static final String QUERY = "Query"; //$NON-NLS-1$
            public static final String TRANSACTION = "Transaction"; //$NON-NLS-1$
            public static final String CONNECTOR = "Connector"; //$NON-NLS-1$
            public static final String RUNTIME_METADATA = "Runtime MetaData"; //$NON-NLS-1$
        }
        public static class SubSystemAPIClassnames {
            public static final String QUERY = "com.metamatrix.server.admin.apiimpl.QueryAdminAPIFacade"; //$NON-NLS-1$
            public static final String TRANSACTION = "com.metamatrix.server.admin.apiimpl.TransactionAdminAPIFacade"; //$NON-NLS-1$
            public static final String CONNECTOR = "com.metamatrix.server.admin.apiimpl.ConnectorManagementAdminAPIFacade"; //$NON-NLS-1$
            public static final String RUNTIME_METADATA = "com.metamatrix.server.admin.apiimpl.RuntimeMetadataAdminAPIFacade"; //$NON-NLS-1$
        }
    }

    public static class MetaDataServer {
        public static final String PRODUCT_NAME = METADATA_SERVER_TYPE_NAME;
        public static class SubSystemNames {
            public static final String METADATA_DIRECTORY = "MetaData Directory"; //$NON-NLS-1$
        }
        public static class SubSystemAPIClassnames {
            public static final String METADATA_DIRECTORY = "com.metamatrix.metadata.server.admin.apiimpl.MetaDataDirectoryAdminAPIFacade"; //$NON-NLS-1$
        }
    }

    public static class ConnectorProduct {
        public static final String PRODUCT_NAME = CONNECTOR_PRODUCT_TYPE_NAME;
        public static final String JDBC = "Connector/JDBC"; //$NON-NLS-1$
        public static final String TEXT = "Connector/Text"; //$NON-NLS-1$
        public static final String LIBRADOS = "Connector/Librados"; //$NON-NLS-1$
        public static final String CUSTOM = "Connector/Custom"; //$NON-NLS-1$
        public static final String XML = "Connector/XML"; //$NON-NLS-1$
        public static final String DATASOURCES = "Sources"; //$NON-NLS-1$
    }

    public static class DQP_Product {
        public static final String PRODUCT_NAME = "Query Engine"; //$NON-NLS-1$
        public static final String TRANSACTIONS = "Query/Transactions"; //$NON-NLS-1$
        public static final String UPDATES = "Query/Updates"; //$NON-NLS-1$

        public static final String CONNECTOR_JDBC_ = "Connector/JDBC"; //$NON-NLS-1$
        public static final String CONNECTOR_TEXT_ = "Connector/Text"; //$NON-NLS-1$
        public static final String CONNECTOR_LIBRADOS_ = "Connector/Librados"; //$NON-NLS-1$
    }

    public static class Views {
        public static final String RELATIONAL_VIEWS = "Views/Relational"; //$NON-NLS-1$
        public static final String XML_VIEWS = "Views/XML"; //$NON-NLS-1$
    }
    
    public static class IntegrationAPI {
        public static final String JDBC = "Integration API/JDBC"; //$NON-NLS-1$
        public static final String ODBC = "Integration API/ODBC"; //$NON-NLS-1$
    }

    /**
     * Returns the product names defined by this class as a Collection of
     * Strings.
     * @return Collection of String product names
     */
    public static Collection getProductNames(){
        if (productsToSubsystems == null){
            initializeMap();
        }
        return productsToSubsystems.keySet();
    }

    /**
     * Returns the subsystem names for a product as a Collection of
     * Strings.
     * @param productName name of product for which subsystem names are sought
     * @return Collection of String subsystem names for the product, or null
     * if the productName is invalid
     */
    public static Collection getProductSubsystemNames(final String productName){
        if (productsToSubsystems == null){
            initializeMap();
        }
        return (Collection)productsToSubsystems.get(productName);
    }

    /**
     * Returns the classname of the Admin API Facade Implementation for a
     * given subsystem
     * @param subsystemName name for which Admin API Facade Impl classname is
     * sought
     * @return String classname of the Admin API Facade Impl for the subsystem
     * passed in, or null if the parameter is invalid
     */
    public static String getAPIClassnameForSubsystemName(final String subsystemName){
        if (subsystemsToAPIClassname == null){
            initializeClassnameMap();
        }
        return (String)subsystemsToAPIClassname.get(subsystemName);
    }

    /**
     * private Constructor, this class should never be instantiated
     */
    private MetaMatrixProductNames(){
    }

    /**
     * This Map maps a String product name to a List
     * of String subsystem names
     */
    private static void initializeMap(){
        productsToSubsystems = new HashMap();

        //platform
        int subsystemCount = 6;
        ArrayList aList = new ArrayList(subsystemCount);
        aList.add(Platform.SubSystemNames.CONFIGURATION);
        aList.add(Platform.SubSystemNames.RUNTIME_STATE);
        aList.add(Platform.SubSystemNames.MEMBERSHIP);
        aList.add(Platform.SubSystemNames.SESSION);
        aList.add(Platform.SubSystemNames.AUTHORIZATION);
        aList.add(Platform.SubSystemNames.EXTENSION_SOURCE);
        productsToSubsystems.put(Platform.PRODUCT_NAME, aList);

        //MM server
        subsystemCount = 4;
        aList = new ArrayList(subsystemCount);
        aList.add(MetaMatrixServer.SubSystemNames.CONNECTOR);
        aList.add(MetaMatrixServer.SubSystemNames.QUERY);
        aList.add(MetaMatrixServer.SubSystemNames.RUNTIME_METADATA);
        aList.add(MetaMatrixServer.SubSystemNames.TRANSACTION);
        productsToSubsystems.put(MetaMatrixServer.PRODUCT_NAME, aList);

        //MetaData server
        subsystemCount = 1;
        aList = new ArrayList(subsystemCount);
        aList.add(MetaDataServer.SubSystemNames.METADATA_DIRECTORY);
        productsToSubsystems.put(MetaDataServer.PRODUCT_NAME, aList);

        productsToSubsystems.put(ConnectorProduct.PRODUCT_NAME, Collections.EMPTY_LIST);
    }

    private static void initializeClassnameMap(){
        subsystemsToAPIClassname = new HashMap();
        subsystemsToAPIClassname.put(Platform.SubSystemNames.CONFIGURATION, Platform.SubSystemAPIClassnames.CONFIGURATION);
        subsystemsToAPIClassname.put(Platform.SubSystemNames.RUNTIME_STATE, Platform.SubSystemAPIClassnames.RUNTIME_STATE);
        subsystemsToAPIClassname.put(Platform.SubSystemNames.MEMBERSHIP, Platform.SubSystemAPIClassnames.MEMBERSHIP);
        subsystemsToAPIClassname.put(Platform.SubSystemNames.SESSION, Platform.SubSystemAPIClassnames.SESSION);
        subsystemsToAPIClassname.put(Platform.SubSystemNames.AUTHORIZATION, Platform.SubSystemAPIClassnames.AUTHORIZATION);
        subsystemsToAPIClassname.put(Platform.SubSystemNames.EXTENSION_SOURCE, Platform.SubSystemAPIClassnames.EXTENSION_SOURCE);
        subsystemsToAPIClassname.put(MetaMatrixServer.SubSystemNames.QUERY, MetaMatrixServer.SubSystemAPIClassnames.QUERY);
        subsystemsToAPIClassname.put(MetaMatrixServer.SubSystemNames.TRANSACTION, MetaMatrixServer.SubSystemAPIClassnames.TRANSACTION);
        subsystemsToAPIClassname.put(MetaMatrixServer.SubSystemNames.CONNECTOR, MetaMatrixServer.SubSystemAPIClassnames.CONNECTOR);
        subsystemsToAPIClassname.put(MetaMatrixServer.SubSystemNames.RUNTIME_METADATA, MetaMatrixServer.SubSystemAPIClassnames.RUNTIME_METADATA);
        subsystemsToAPIClassname.put(MetaDataServer.SubSystemNames.METADATA_DIRECTORY, MetaDataServer.SubSystemAPIClassnames.METADATA_DIRECTORY);
    }

}

