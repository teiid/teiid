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

package com.metamatrix.server.dqp.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.Binder;
import com.google.inject.name.Names;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.service.CustomizableTrackingService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.metadata.IndexMetadataService;
import com.metamatrix.dqp.service.metadata.QueryMetadataCache;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseException;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.Configuration;
import com.metamatrix.server.ResourceFinder;
import com.metamatrix.server.dqp.service.PlatformAuthorizationService;
import com.metamatrix.server.dqp.service.PlatformBufferService;
import com.metamatrix.server.dqp.service.PlatformDataService;
import com.metamatrix.server.dqp.service.PlatformTransactionService;
import com.metamatrix.server.dqp.service.PlatformVDBService;
import com.metamatrix.server.dqp.service.tracker.DatabaseCommandLogger;
import com.metamatrix.server.query.service.QueryServicePropertyNames;

/**
 */
public class PlatformConfigSource implements DQPConfigSource {

    public static final String PROC_DEBUG_ALLOWED = "metamatrix.server.procDebug"; //$NON-NLS-1$

    private Properties dqpProps;
    
    private Host host;
    private String processName;
    
    public PlatformConfigSource(Properties queryServiceProps, Properties currentConfiguration, Object clientId, Host host, String processName) {
        dqpProps = PropertiesUtils.clone(queryServiceProps, currentConfiguration, true);
        dqpProps.setProperty(DQPConfigSource.PROCESS_POOL_MAX_THREADS, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESS_POOL_MAX_THREADS));
        dqpProps.setProperty(DQPConfigSource.PROCESS_POOL_THREAD_TTL, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESS_POOL_THREAD_TTL));
        dqpProps.setProperty(DQPConfigSource.MIN_FETCH_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MIN_FETCH_SIZE));
        dqpProps.setProperty(DQPConfigSource.MAX_FETCH_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_FETCH_SIZE));
        dqpProps.setProperty(DQPConfigSource.MAX_CODE_TABLE_RECORDS, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_CODE_TABLE_RECORDS));
        dqpProps.setProperty(DQPConfigSource.MAX_CODE_TABLES, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_CODE_TABLES));
        dqpProps.setProperty(DQPConfigSource.PROCESSOR_TIMESLICE, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESSOR_TIMESLICE));
        
        dqpProps.setProperty(DQPConfigSource.USE_RESULTSET_CACHE, queryServiceProps.getProperty(QueryServicePropertyNames.USE_RESULTSET_CACHE));
        dqpProps.setProperty(DQPConfigSource.MAX_RESULTSET_CACHE_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_RESULTSET_CACHE_SIZE));
        dqpProps.setProperty(DQPConfigSource.MAX_RESULTSET_CACHE_AGE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_RESULTSET_CACHE_AGE));
        dqpProps.setProperty(DQPConfigSource.RESULTSET_CACHE_SCOPE, queryServiceProps.getProperty(QueryServicePropertyNames.RESULTSET_CACHE_SCOPE));

        dqpProps.setProperty(DQPConfigSource.MAX_PLAN_CACHE_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_PLAN_CACHE_SIZE));
        
        String procDebugStr = currentConfiguration.getProperty(PROC_DEBUG_ALLOWED);
        if(procDebugStr != null) {
            dqpProps.setProperty(DQPConfigSource.PROCESSOR_DEBUG_ALLOWED, procDebugStr);
        }

        String streamingBatchSize = currentConfiguration.getProperty(DQPConfigSource.STREAMING_BATCH_SIZE);
        if(streamingBatchSize != null) {
            dqpProps.setProperty(DQPConfigSource.STREAMING_BATCH_SIZE, streamingBatchSize);
        }
        
        if (dqpProps.getProperty(DQPConfigSource.COMMAND_LOGGER_CLASSNAME) == null) {
        	dqpProps.setProperty(DQPConfigSource.COMMAND_LOGGER_CLASSNAME, DatabaseCommandLogger.class.getName());
        }
        
        this.host = host;
        this.processName = processName;
    }

    /*
     * @see com.metamatrix.dqp.config.DQPConfigSource#getProperties()
     */
    public Properties getProperties() {
        return dqpProps;
    }
    
	@Override
	public Map<String, Class<? extends ApplicationService>> getDefaultServiceClasses() {
		Map<String, Class<? extends ApplicationService>> result = new HashMap<String, Class<? extends ApplicationService>>();
		result.put(DQPServiceNames.TRACKING_SERVICE, CustomizableTrackingService.class);
		result.put(DQPServiceNames.BUFFER_SERVICE, PlatformBufferService.class);
		result.put(DQPServiceNames.VDB_SERVICE, PlatformVDBService.class);
		result.put(DQPServiceNames.METADATA_SERVICE, IndexMetadataService.class);
		result.put(DQPServiceNames.DATA_SERVICE, PlatformDataService.class);
		result.put(DQPServiceNames.TRANSACTION_SERVICE, PlatformTransactionService.class);
		result.put(DQPServiceNames.AUTHORIZATION_SERVICE, PlatformAuthorizationService.class);
		return result;
	}
	
	@Override
	public void updateBindings(Binder binder) {
		//TODO: this should really just be a child injector (guice 2)
		binder.bind(AuthorizationServiceInterface.class).toInstance(PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL));
		try {
			binder.bind(QueryMetadataCache.class).toInstance(RuntimeMetadataCatalog.getInstance().getQueryMetadataCache());
		} catch (VirtualDatabaseException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		binder.bindConstant().annotatedWith(Names.named(Configuration.PROCESSNAME)).to(processName);
		binder.bind(Host.class).annotatedWith(Names.named(Configuration.HOST)).toInstance(host);
		binder.bind(DQPContextCache	.class).toInstance(ResourceFinder.getContextCache());
	}
    
}