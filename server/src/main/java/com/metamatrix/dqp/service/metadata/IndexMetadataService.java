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

package com.metamatrix.dqp.service.metadata;

import java.util.Map;
import java.util.Properties;

import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.metadata.QueryMetadataCache;

import com.google.inject.Inject;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 * Implementation of MetadaService using index files.
 */
/** 
 * @since 4.2
 */
public class IndexMetadataService implements MetadataService {

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
    @Inject
    public IndexMetadataService(final QueryMetadataCache metadataCache) {
        this.metadataCache = metadataCache;
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        if(!started) {
            this.vdbService = (VDBService)environment.findService(DQPServiceNames.VDB_SERVICE);
            if(this.vdbService == null){
                throw new ApplicationLifecycleException(DQPPlugin.Util.getString("IndexMetadataService.VDB_Service_is_not_available._1"));  //$NON-NLS-1$
            }
	        // mark started
	        started = true;
        }
    }

    /*
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        started = false;
        this.vdbService = null;
    }

	public IObjectSource getMetadataObjectSource(String vdbName, String vdbVersion) throws MetaMatrixComponentException {
		return this.metadataCache.getCompositeMetadataObjectSource(vdbName, vdbVersion, vdbService);
	}
    
    /** 
     * @see com.metamatrix.dqp.service.MetadataService#lookupMetadata(java.lang.String, java.lang.String)
     * @since 4.2
     */
    public QueryMetadataInterface lookupMetadata(final String vdbName, final String vdbVersion) throws MetaMatrixComponentException{
        LogManager.logTrace(LogConstants.CTX_DQP, new Object[] {"IndexMetadataService lookup VDB", vdbName, vdbVersion});  //$NON-NLS-1$
	    return this.metadataCache.lookupMetadata(vdbName, vdbVersion, this.vdbService.getVDB(vdbName, vdbVersion), null);
    }
    
    @Override
    public Map<String, DatatypeRecordImpl> getBuiltinDatatypes()
    		throws MetaMatrixComponentException {
    	return this.metadataCache.getBuiltinDatatypes();
    }
    
}
