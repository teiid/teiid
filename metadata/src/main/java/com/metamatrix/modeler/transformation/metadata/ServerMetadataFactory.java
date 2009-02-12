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

package com.metamatrix.modeler.transformation.metadata;

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 * TransformationMetadataFactory
 */
public class ServerMetadataFactory {

    private static final ServerMetadataFactory INSTANCE = new ServerMetadataFactory();

    protected ServerMetadataFactory() {}

    public static ServerMetadataFactory getInstance() {
        return INSTANCE;
    }

	/**
     * Return a reference to a {@link QueryMetadataInterface} implementation, the metadata
     * is assumed not to change.
     * @param context Object containing the info needed to lookup metadta.
     * @return the QueryMetadataInterface implementation; never null
     */
    public QueryMetadataInterface getServerMetadata(final IndexSelector selector) {
        QueryMetadataContext context = new QueryMetadataContext(selector);
        return getServerMetadata(context);
    }    

    /**
     * Return a reference to a {@link QueryMetadataInterface} implementation, the metadata
     * is assumed not to change.
     * @param context Object containing the info needed to lookup metadta.
     * @return the QueryMetadataInterface implementation; never null
     */
    QueryMetadataInterface getServerMetadata(final QueryMetadataContext context) {
        ArgCheck.isNotNull(context);
        // Create the QueryMetadataInterface implementation to use
        // for query validation and resolution
        return new ServerRuntimeMetadata(context);
    }
    
    /**
     * Create a {@link QueryMetadataInterface} implementation that maintains a local cache
     * of metadata. For server the state of the metadata should not change anyway.
     * @param selector The indexselector used to lookup index files.
     * @return a new QueryMetadataInterface implementation; never null
     */
    public QueryMetadataInterface createCachingServerMetadata(final IndexSelector selector) {
        QueryMetadataContext context = new QueryMetadataContext(selector);
        return createCachingServerMetadata(context);
    }    

    /**
     * Create a {@link QueryMetadataInterface} implementation that maintains a local cache
     * of metadata. For server the state of the metadata should not change anyway.
     * @param context Object containing the info needed to lookup metadta.
     * @return a new QueryMetadataInterface implementation; never null
     */
    QueryMetadataInterface createCachingServerMetadata(final QueryMetadataContext context) {
        final TransformationMetadata metadata = (TransformationMetadata)getServerMetadata(context);
        return new TransformationMetadataFacade(metadata);
    }

}