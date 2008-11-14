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

package com.metamatrix.connector.metadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.connector.metadata.adapter.ObjectConnector;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory;
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.connector.sysadmin.util.SysAdminUtil;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.metadata.IndexSelectorSource;

/**
 * Connector whose source is metadata index files stored in vdbs aviailable to
 * the user logged in.
 */
public class IndexConnector extends ObjectConnector {
    
    private ISysAdminConnectionFactory adminFactory = null;
    
    public void initialize(final ConnectorEnvironment environment) throws ConnectorException {
        super.initialize(environment);
        
        // Only if running in a MetaMatrix Server VM is the ServerAdmin feature enabled.
        String isServerVM = VMNaming.getVMName();
        if (isServerVM != null && isServerVM.length() > 0) {
            adminFactory = SysAdminUtil.createFactory(environment, this.getClass().getClassLoader());
        }
    }
    
    

    /** 
     * @see com.metamatrix.connector.metadata.adapter.ObjectConnector#getObjectSource(com.metamatrix.data.api.ConnectorEnvironment, com.metamatrix.data.api.SecurityContext)
     */
    protected IObjectSource getMetadataObjectSource(final SecurityContext context) throws ConnectorException {
        ConnectorEnvironmentImpl internalEnvironment = (ConnectorEnvironmentImpl) this.getEnvironment();
        // lookup indesService
        IndexSelectorSource metadataService = (IndexSelectorSource) internalEnvironment.findResource(DQPServiceNames.METADATA_SERVICE);
        
        try {
			return metadataService.getMetadataObjectSource(context.getVirtualDatabaseName(), context.getVirtualDatabaseVersion());
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e, MetadataConnectorPlugin.Util.getString("IndexConnector.indexSelector_not_available")); //$NON-NLS-1$			
		}
    }
    
    protected ISysAdminSource getSysAdminObjectSource(final SecurityContext context) throws ConnectorException {  
        if (adminFactory == null) {
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("IndexConnector.SysAdmin_feature_not_available")); //$NON-NLS-1$            
        }
        return adminFactory.getObjectSource( context);
    }
}
