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

import java.util.ArrayList;
import java.util.Collection;

public class ProductServiceConfigID extends ComponentDefnID  {


    /**
     * The name of the built-in, standard PSC of the
     * MetaMatrix Server product
     */
    public static final String STANDARD_METAMATRIX_SERVER_PSC = "MetaMatrixServerFull"; //$NON-NLS-1$

    /**
     * The name of the built-in, Query Engine only PSC of the
     * MetaMatrix Server product
     */
    public static final String METAMATRIX_SERVER_QUERY_ENGINE_PSC = "QueryEngine"; //$NON-NLS-1$

    /**
     * The name of the built-in, standard PSC of the
     * MetaData Server product
     */
    public static final String STANDARD_METADATA_SERVER_PSC   = "MetaBaseServerStandard"; //$NON-NLS-1$

    /**
     * The name of the built-in, standard PSC of the
     * Platform product
     */
    public static final String STANDARD_PLATFORM_PSC = "PlatformStandard"; //$NON-NLS-1$

    /**
     * The name of the built-in, standard PSC of the
     * Connector product
     */
    public static final String STANDARD_CONNECTOR_PSC = "MMProcessPSC"; //$NON-NLS-1$

    /**
     * The Collection of the names of the four build-in, standard
     * PSCs, one for each product type (@see {@link #ALL_PRODUCT_TYPES}).
     */
    public static final Collection ALL_STANDARD_PSC_NAMES;


    static{
        ALL_STANDARD_PSC_NAMES = new ArrayList(4);
        ALL_STANDARD_PSC_NAMES.add(STANDARD_METAMATRIX_SERVER_PSC);
        ALL_STANDARD_PSC_NAMES.add(METAMATRIX_SERVER_QUERY_ENGINE_PSC);
        ALL_STANDARD_PSC_NAMES.add(STANDARD_METADATA_SERVER_PSC);
        ALL_STANDARD_PSC_NAMES.add(STANDARD_PLATFORM_PSC);
//        ALL_STANDARD_PSC_NAMES.add(STANDARD_CONNECTOR_PSC);
    }




    public ProductServiceConfigID(ConfigurationID configID, String name) {
        super(configID, name);
    }

    public ProductServiceConfigID(String fullName) {
        super(fullName);
    }
}





