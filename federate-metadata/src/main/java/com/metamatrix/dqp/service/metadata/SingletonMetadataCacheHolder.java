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

package com.metamatrix.dqp.service.metadata;

import java.net.URL;

import com.metamatrix.core.MetaMatrixCoreException;


/** 
 * Singletom used in the server to look up cache for QueryeMetadta.
 * @since 4.2
 */
public class SingletonMetadataCacheHolder {
    
    private static QueryMetadataCache metadataCache;

    /** 
     * Singleton, prevent construction.
     * @since 4.2
     */
    private SingletonMetadataCacheHolder() {
        super();
    }

    /**
     * Return the QueryMetadataCache given a url to the system vdb. If the the cache already exists on this
     * singleton it is returned else an new instance of it is created for the given system vdb.
     * @param systemVdbUrl The URL to the system vdb.
     * @return QueryMetadataCache
     */
    public static synchronized QueryMetadataCache getMetadataCache(final URL systemVdbUrl) throws MetaMatrixCoreException {
        if(!hasCache()) {
            metadataCache = new QueryMetadataCache(systemVdbUrl);
        }
        return metadataCache;
    }

    /**
     * Return the QueryMetadataCache given the contents of system vdb. If the the cache already exists on this
     * singleton it is returned else an new instance of it is created for the given system vdb.
     * @param systemVdbContent The contents of the system vdb.
     * @return QueryMetadataCache
     */
    public static synchronized QueryMetadataCache getMetadataCache(final byte[] systemVdbContent) throws MetaMatrixCoreException {
        if(!hasCache()) {
            metadataCache = new QueryMetadataCache(systemVdbContent);
        }
        return metadataCache;
    }
    
    /**
     * Return any existing cached instance of QueryMetadataCache.
     * May return null.
     * @return QueryMetadataCache
     * @since 4.2
     */
    public static QueryMetadataCache getMetadataCache() {
        if(hasCache()) {
            return metadataCache;
        }
        return null;
    }

    /**
     * Check if this singleton has already cahed the QueryMetadataCache. 
     * @return true if it has a QueryMetadataCache.
     * @since 4.2
     */
    public static boolean hasCache() {
        if(metadataCache != null && metadataCache.isValid()) {
            return true;
        }
        return false;
    }
}