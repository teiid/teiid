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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.connector.metadata.IndexFile;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MultiObjectSource;
import com.metamatrix.connector.metadata.PropertyFileObjectSource;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceProperties;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 * Implementation of MetadaService using index files.
 */
/** 
 * @since 4.2
 */
public class IndexMetadataService implements MetadataService, IndexSelectorSource {

    private VDBService vdbService;
    private boolean started = false;
    private QueryMetadataCache metadataCache;    

    /**
     * Construct the IndexMetadataService
     */
    public IndexMetadataService() {}
    
    /**
     * Construct the IndexMetadataService
     */
    public IndexMetadataService(final QueryMetadataCache metadataCache) {
        this.metadataCache = metadataCache;
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        if(this.metadataCache == null) {
            this.metadataCache = (QueryMetadataCache) props.get(DQPServiceProperties.MetadataService.QUERY_METADATA_CACHE);
        }
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        if(!started) {
	        if(this.vdbService == null){
	            this.vdbService = (VDBService)environment.findService(DQPServiceNames.VDB_SERVICE);
	            if(this.vdbService == null){
                    LogManager.logError(LogConstants.CTX_DQP, DQPPlugin.Util.getString("IndexMetadataService.VDB_Service_is_not_available._1"));  //$NON-NLS-1$
	                throw new ApplicationLifecycleException(DQPPlugin.Util.getString("IndexMetadataService.VDB_Service_is_not_available._1"));  //$NON-NLS-1$
	            }
	        }
	        // mark started
	        started = true;
	        // indicate to the cache that this service is using it
	        this.metadataCache.shareIncrement();
        }
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bind() throws ApplicationLifecycleException {}

    /*
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbind() throws ApplicationLifecycleException {}

    /*
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        if(started) {
	        started = false;
	        // indicate to the cache that this service no longer needs it
	        this.metadataCache.shareDecrement();
        }
    }

	public IObjectSource getMetadataObjectSource(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
		IndexSelector indexSelector = this.metadataCache.getCompositeSelector(vdbName, vdbVersion);

		// build up sources to be used by the index connector
		IObjectSource indexFile = new IndexFile(indexSelector, vdbName, vdbVersion, this.vdbService);

		PropertyFileObjectSource propertyFileSource = new PropertyFileObjectSource();
		IObjectSource multiObjectSource = new MultiObjectSource(indexFile, MetadataConnectorConstants.PROPERTIES_FILE_EXTENSION,propertyFileSource);

		// return an adapter object that has access to all sources
		return multiObjectSource;		
	}
    
    /** 
     * @see com.metamatrix.dqp.service.MetadataService#lookupMetadata(java.lang.String, java.lang.String)
     * @since 4.2
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion) throws MetaMatrixComponentException{
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"IndexMetadataService lookup VDB", vdbName, vdbVersion});  //$NON-NLS-1$
        QueryMetadataInterface qmi = this.metadataCache.lookupMetadata(vdbName, vdbVersion);
        if(qmi == null) {
           LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"IndexMetadataService cache miss for VDB", vdbName, vdbVersion});  //$NON-NLS-1$
	       return this.metadataCache.lookupMetadata(vdbName, vdbVersion, this.vdbService.getVDBResource(vdbName, vdbVersion));
        }
        return qmi;
    }
    
    public QueryMetadataInterface testLoadMetadata(final String vdbName, final String vdbVersion, final String filePath) throws MetaMatrixComponentException{
        try {
			return this.metadataCache.lookupMetadata(vdbName, vdbVersion, new FileInputStream(filePath));
		} catch (FileNotFoundException e) {
			throw new MetaMatrixComponentException(e);		
		}        
    }
}
