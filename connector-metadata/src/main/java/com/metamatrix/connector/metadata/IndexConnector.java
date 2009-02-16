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

package com.metamatrix.connector.metadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ConnectorAnnotations.ConnectionPooling;
import com.metamatrix.connector.metadata.adapter.ObjectConnector;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.metadata.IndexSelectorSource;

/**
 * Connector whose source is metadata index files stored in vdbs aviailable to
 * the user logged in.
 */
@ConnectionPooling(enabled=false)
public class IndexConnector extends ObjectConnector {
    
    /** 
     * @see com.metamatrix.connector.metadata.adapter.ObjectConnector#getObjectSource(com.metamatrix.connector.api.ConnectorEnvironment, com.metamatrix.connector.api.ExecutionContext)
     */
    protected IObjectSource getMetadataObjectSource(final ExecutionContext context) throws ConnectorException {
        ConnectorEnvironmentImpl internalEnvironment = (ConnectorEnvironmentImpl) this.getEnvironment();
        // lookup indesService
        IndexSelectorSource metadataService = (IndexSelectorSource) internalEnvironment.findResource(DQPServiceNames.METADATA_SERVICE);
        
        try {
			return metadataService.getMetadataObjectSource(context.getVirtualDatabaseName(), context.getVirtualDatabaseVersion());
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e, MetadataConnectorPlugin.Util.getString("IndexConnector.indexSelector_not_available")); //$NON-NLS-1$			
		}
    }
    
}
