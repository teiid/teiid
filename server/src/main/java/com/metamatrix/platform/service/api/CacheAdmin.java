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

package com.metamatrix.platform.service.api;

import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * Interface for an object that allows the administration of caches.
 */
public interface CacheAdmin {

    public static final String CODE_TABLE_CACHE = "CodeTableCache"; //$NON-NLS-1$
    public static final String PREPARED_PLAN_CACHE = "PreparedPlanCache"; //$NON-NLS-1$
    public static final String QUERY_SERVICE_RESULT_SET_CACHE = "QueryServiceResultSetCache"; //$NON-NLS-1$
    public static final String CONNECTOR_RESULT_SET_CACHE = "ConnectorResultSetCache"; //$NON-NLS-1$

    /**
     * Get names of all caches in this object
     * @return Map<String, String> where each key is the name of a cache and each value is
     * the type, as defined by constants in this interface.
     * @throws MetaMatrixComponentException If an error occurs
     */
    Map getCaches() throws MetaMatrixComponentException;
    
    /**
     * Clear the named cached using properties to set options if necessary
     * @param name Name of the cache to clear
     * @param props Optional additional properties which may vary by cache type
     * @throws MetaMatrixComponentException If an error occurs
     */
    void clearCache(String name, Properties props) throws MetaMatrixComponentException;
    
    
}

