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

package com.metamatrix.modeler.transformation.metadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.index.IEntryResult;
import com.metamatrix.internal.core.index.Index;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.internal.core.index.SimpleIndexUtil;

/**
 * Metadata implementation used by server to resolve queries.
 * 
 */
public class ServerRuntimeMetadata extends TransformationMetadata {

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    /**
     * ServerRuntimeMetadata constructor
     * @param context Object containing the info needed to lookup metadta.
     */    
    public  ServerRuntimeMetadata(final QueryMetadataContext context) {
        super(context);
    }

    //==================================================================================
    //                   O V E R R I D D E N   M E T H O D S
    //==================================================================================

    /**
     * Return the array of MtkIndex instances representing core indexes for the
     * specified record type
     * @param recordType The type of record to loop up the indexes that conyains it
     * @param selector The indexselector that has access to indexes
     * @return The array if indexes
     * @throws QueryMetadataException
     */
    protected Index[] getIndexes(final char recordType, final IndexSelector selector) throws MetaMatrixComponentException {
        // The the index file name for the record type
        try {
            final String indexName = SimpleIndexUtil.getIndexFileNameForRecordType(recordType);
            return SimpleIndexUtil.getIndexes(indexName, selector);            
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, RuntimeMetadataPlugin.Util.getString("TransformationMetadata.Error_trying_to_obtain_index_file_using_IndexSelector_1",selector)); //$NON-NLS-1$
        }
    }

    /** 
     * @see com.metamatrix.modeler.transformation.metadata.TransformationMetadata#queryIndex(com.metamatrix.core.index.impl.Index[], char[], boolean, boolean)
     * @since 4.2
     */
    protected IEntryResult[] queryIndex(final Index[] indexes,
                                        char[] pattern,
                                        boolean isPrefix,
                                        boolean returnFirstMatch) throws MetaMatrixComponentException {
        try {
            return super.queryIndex(indexes, pattern, isPrefix, returnFirstMatch);
        } catch(MetaMatrixComponentException e) {
            if(!this.getIndexSelector().isValid()) {
                throw new MetaMatrixComponentException(RuntimeMetadataPlugin.Util.getString("ServerRuntimeMetadata.invalid_selector")); //$NON-NLS-1$
            }
            throw e;
        }
    }
}