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

package com.metamatrix.connector.metadata.index;

import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.transformation.metadata.QueryMetadataContext;


/** 
 * Context for vdb and index information needed by MetadataConnectorMetadata.
 * @since 4.3
 */
public class VdbMetadataContext extends QueryMetadataContext {

    private String vdbName;
    private String vdbVersion;
    private VDBService vdbService;

    /** 
     * VdbMetadataContext
     * @param indexSelector The indexSelector to set.
     * @since 4.2
     */
    public VdbMetadataContext() {}

    /** 
     * VdbMetadataContext
     * @param indexSelector The indexSelector to set.
     * @since 4.2
     */
    public VdbMetadataContext(final IndexSelector indexSelector) {
        super(indexSelector);
    }
    
    
    /** 
     * @return Returns the vdbName.
     * @since 4.3
     */
    public String getVdbName() {
        return this.vdbName;
    }

    
    /** 
     * @param vdbName The vdbName to set.
     * @since 4.3
     */
    public void setVdbName(String vdbName) {
        this.vdbName = vdbName;
    }

    public VDBService getVdbService() {
        return this.vdbService;
    }

    
    /** 
     * @param vdbService The vdbService to set.
     * @since 4.3
     */
    public void setVdbService(VDBService vdbService) {
        this.vdbService = vdbService;
    }

    
    /** 
     * @return Returns the vdbVersion.
     * @since 4.3
     */
    public String getVdbVersion() {
        return this.vdbVersion;
    }

    
    /** 
     * @param vdbVersion The vdbVersion to set.
     * @since 4.3
     */
    public void setVdbVersion(String vdbVersion) {
        this.vdbVersion = vdbVersion;
    }
}
