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

package org.teiid.connector.metadata;

import java.util.Collection;
import java.util.Map;

import org.teiid.metadata.CompositeMetadataStore;

import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.service.VDBService;

/**
 * Adapter to present metadata from a VDB file as an IObjectSource.
 */
public class IndexFile implements IObjectSource {
    
    // metadata instance used for querying indexfiles
    private final MetadataConnectorMetadata queryTransformationMetadata;
    
    /**
     * Constructor IndexFile
     * @param indexSelector Selector pointing to vdbs with index files
     * @param vdbName The name of the vdb the user logged on with
     * @param vdbVersion The version of the vdb the user logged on with
     * @param vdbService The service object used to lookup additional info about vdb like visibility
     * may be null, will assume default settings if null.
     * @since 4.3
     */
    public IndexFile(final CompositeMetadataStore indexSelector, final String vdbName, final String vdbVersion, final VDBService vdbService) {
        ArgCheck.isNotNull(indexSelector);
        ArgCheck.isNotNull(vdbName);
        ArgCheck.isNotNull(vdbVersion);

        // construct a context object used to pass onto metadata
        VdbMetadataContext context = new VdbMetadataContext();
        context.setVdbName(vdbName);
        context.setVdbVersion(vdbVersion);
        context.setVdbService(vdbService);

        // construct the metadata instance
        this.queryTransformationMetadata = new MetadataConnectorMetadata(context, indexSelector);
    }

    /** 
     * @see com.metamatrix.connector.metadata.internal.IObjectSource#getObjects(java.lang.String, java.util.Map)
     * @since 4.3
     */
    public Collection getObjects(String tableName, Map criteria) {
        ArgCheck.isNotNull(tableName);
        ArgCheck.isNotNull(criteria);        
        return queryTransformationMetadata.getObjects(tableName, criteria);
    }    
}