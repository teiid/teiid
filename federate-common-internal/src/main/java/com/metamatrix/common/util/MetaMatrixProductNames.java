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
    }

    public static class MetaMatrixServer {
        public static final String PRODUCT_NAME = METAMATRIX_SERVER_TYPE_NAME;
        public static class SubSystemNames {
            public static final String QUERY = "Query"; //$NON-NLS-1$
            public static final String TRANSACTION = "Transaction"; //$NON-NLS-1$
            public static final String CONNECTOR = "Connector"; //$NON-NLS-1$
            public static final String RUNTIME_METADATA = "Runtime MetaData"; //$NON-NLS-1$
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

        productsToSubsystems.put(ConnectorProduct.PRODUCT_NAME, Collections.EMPTY_LIST);
    }

}

