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

package com.metamatrix.server.dqp.config;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.extensionmodule.protocol.URLFactory;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.config.DQPConfigSource;
import com.metamatrix.dqp.config.DQPProperties;
import com.metamatrix.dqp.service.CustomizableTrackingService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceProperties;
import com.metamatrix.dqp.service.metadata.IndexMetadataService;
import com.metamatrix.dqp.service.metadata.QueryMetadataCache;
import com.metamatrix.dqp.service.metadata.SingletonMetadataCacheHolder;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.dqp.service.DatabaseTrackingService;
import com.metamatrix.server.dqp.service.PlatformAuthorizationService;
import com.metamatrix.server.dqp.service.PlatformBufferService;
import com.metamatrix.server.dqp.service.PlatformDataService;
import com.metamatrix.server.dqp.service.PlatformTransactionService;
import com.metamatrix.server.dqp.service.PlatformVDBService;
import com.metamatrix.server.query.service.QueryServicePropertyNames;
import com.metamatrix.server.util.ServerPropertyNames;

/**
 */
public class PlatformConfigSource implements DQPConfigSource {

    public static final String WEBSERVER_HOST = "metamatrix.webserver.host"; //$NON-NLS-1$
    public static final String WEBSERVER_PORT = "metamatrix.webserver.port"; //$NON-NLS-1$
    public static final String PROC_DEBUG_ALLOWED = "metamatrix.server.procDebug"; //$NON-NLS-1$
    public static final String COMMAND_LOGGER_CLASSNAME = "metamatrix.server.commandLoggerClassname"; //$NON-NLS-1$

    private Properties queryServiceProps;
    private BufferManager bufferMgr;

    private Map services = new HashMap();
    
    private static PlatformTransactionService transactionService;
    private static ApplicationService trackingService;    

    /**
     *
     *
     */
    public PlatformConfigSource(Properties queryServiceProps, BufferManager bufferMgr, Object clientId) {
        super();

        this.queryServiceProps = queryServiceProps;
        this.bufferMgr = bufferMgr;
    }

    /*
     * @see com.metamatrix.dqp.config.DQPConfigSource#getProperties()
     */
    public Properties getProperties() throws ApplicationInitializationException {
        Properties dqpProps = new Properties();
        dqpProps.setProperty(DQPProperties.PROCESS_POOL_MAX_THREADS, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESS_POOL_MAX_THREADS));
        dqpProps.setProperty(DQPProperties.PROCESS_POOL_THREAD_TTL, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESS_POOL_THREAD_TTL));
        dqpProps.setProperty(DQPProperties.MIN_FETCH_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MIN_FETCH_SIZE));
        dqpProps.setProperty(DQPProperties.MAX_FETCH_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_FETCH_SIZE));
        dqpProps.setProperty(DQPProperties.MAX_CODE_TABLE_RECORDS, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_CODE_TABLE_RECORDS));
        dqpProps.setProperty(DQPProperties.MAX_CODE_TABLES, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_CODE_TABLES));
        dqpProps.setProperty(DQPProperties.PROCESSOR_TIMESLICE, queryServiceProps.getProperty(QueryServicePropertyNames.PROCESSOR_TIMESLICE));
        
        dqpProps.setProperty(DQPProperties.USE_RESULTSET_CACHE, queryServiceProps.getProperty(QueryServicePropertyNames.USE_RESULTSET_CACHE));
        dqpProps.setProperty(DQPProperties.MAX_RESULTSET_CACHE_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_RESULTSET_CACHE_SIZE));
        dqpProps.setProperty(DQPProperties.MAX_RESULTSET_CACHE_AGE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_RESULTSET_CACHE_AGE));
        dqpProps.setProperty(DQPProperties.RESULTSET_CACHE_SCOPE, queryServiceProps.getProperty(QueryServicePropertyNames.RESULTSET_CACHE_SCOPE));

        dqpProps.setProperty(DQPProperties.MAX_PLAN_CACHE_SIZE, queryServiceProps.getProperty(QueryServicePropertyNames.MAX_PLAN_CACHE_SIZE));
        
        String procDebugStr = CurrentConfiguration.getProperty(PROC_DEBUG_ALLOWED);
        if(procDebugStr == null) {
            procDebugStr = "false"; //$NON-NLS-1$
        }
        dqpProps.setProperty(DQPProperties.PROCESSOR_DEBUG_ALLOWED, procDebugStr);

        String streamingBatchSize = CurrentConfiguration.getProperty(DQPProperties.STREAMING_BATCH_SIZE);
        if(streamingBatchSize != null) {
            dqpProps.setProperty(DQPProperties.STREAMING_BATCH_SIZE, streamingBatchSize);
        }
        addEnvProps(dqpProps);

        return dqpProps;
    }

    private void addEnvProps(Properties props) {
// NOTE: Currently, these properties are no longer used but I am leaving the code here as an example
// for when we need to add something like this back in later.
//        
//        try {
//            Properties soapProps = CurrentConfiguration.getResourceProperties(ResourceNames.WEB_SERVICES);            
//            PropertiesUtils.copyProperty(soapProps, WEBSERVER_HOST, props, ContextProperties.SOAP_HOST);
//            PropertiesUtils.copyProperty(soapProps, WEBSERVER_PORT, props, ContextProperties.SOAP_PORT);
//        } catch (ConfigurationException e) {
//            LogManager.logWarning(LogConstants.CTX_QUERY_SERVICE, ServerPlugin.Util.getString("PlatformConfigSource.Err_reading_resource_props", ResourceNames.WEB_SERVICES)); //$NON-NLS-1$
//        }
    }

    /*
     * @see com.metamatrix.dqp.config.DQPConfigSource#getService(java.lang.String)
     */
    public ApplicationService getService(String serviceName) throws ApplicationInitializationException {
        return findOrCreateService(serviceName);
    }

    /**
     * Find existing service if one exists, otherwise create one.
     */
    private synchronized ApplicationService findOrCreateService(String serviceName) throws ApplicationInitializationException {
        // Try cached services first
        ApplicationService svc = (ApplicationService) this.services.get(serviceName);
        if(svc != null) {
            return svc;
        }

        // Create services as necessary
        if(serviceName.equals(DQPServiceNames.AUTHORIZATION_SERVICE)) {
            svc =  createAuthorizationService();
        } else if(serviceName.equals(DQPServiceNames.BUFFER_SERVICE)) {
            svc =  createBufferService();
        } else if(serviceName.equals(DQPServiceNames.DATA_SERVICE)) {
            svc =  createDataService();
        } else if(serviceName.equals(DQPServiceNames.METADATA_SERVICE)) {
            svc =  createMetadataService();
        } else if(serviceName.equals(DQPServiceNames.TRACKING_SERVICE)) {
            svc =  createTrackingService();
        } else if(serviceName.equals(DQPServiceNames.VDB_SERVICE)) {
            svc =  createVDBService();
        } else if(serviceName.equals(DQPServiceNames.TRANSACTION_SERVICE)) {
            svc =  getTransactionService();
        } else {
            return null;
        }

        this.services.put(serviceName, svc);
        return svc;
    }

    public static synchronized ApplicationService getTransactionService() throws ApplicationInitializationException {
        if (transactionService == null) {
            transactionService = new PlatformTransactionService();
            Properties props = new Properties();
            transactionService.initialize(props);
        }
        return transactionService;
    }

    /**
     * @return
     */
    private ApplicationService createAuthorizationService() throws ApplicationInitializationException {
        // Initialize service proxies
        try {
            AuthorizationServiceInterface authService = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
            SessionServiceInterface sessionService = PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);

            PlatformAuthorizationService pas = new PlatformAuthorizationService(authService, sessionService);
            pas.initialize(new Properties());
            return pas;
        } catch(ServiceException e) {
            throw new ApplicationInitializationException(e);
        }
    }

    /**
     * @return
     */
    private ApplicationService createBufferService() throws ApplicationInitializationException {
        ApplicationService service = new PlatformBufferService();
        Properties props = new Properties();
        props.put(PlatformBufferService.BUFFER_MGR, this.bufferMgr);
        service.initialize(props);
        return service;
    }

    /**
     * @return
     */
    private ApplicationService createDataService() throws ApplicationInitializationException {
        ApplicationService service = new PlatformDataService();
        service.initialize(new Properties());
        return service;
    }

    /**
     * @return
     */
    private ApplicationService createMetadataService() throws ApplicationInitializationException {
        String systemVdbUrl = queryServiceProps.getProperty(ServerPropertyNames.SYSTEM_VDB_URL);
        if(systemVdbUrl == null) {
            throw new ApplicationInitializationException(ServerPlugin.Util.getString("PlatformConfigSource.1")); //$NON-NLS-1$
        }        
        QueryMetadataCache sharedCache = null; 
        try {
            URL systemUrl = URLFactory.parseURL(systemVdbUrl);
            sharedCache = SingletonMetadataCacheHolder.getMetadataCache(systemUrl);
        } catch(Exception e) {
            throw new ApplicationInitializationException(e, ServerPlugin.Util.getString("PlatformConfigSource.0")); //$NON-NLS-1$
        }
        ApplicationService service = new IndexMetadataService(sharedCache);
        service.initialize(new Properties());
        return service;        
    }

    /**
     * @return
     */
    private ApplicationService createTrackingService() throws ApplicationInitializationException {
        return getTrackingService();
    }
    
    /**
     * Public utility method to get tracking service - this method is also used
     * by ConnectorService.initService method. 
     * @return TrackingService instance
     * @throws ApplicationInitializationException
     */
    public static synchronized ApplicationService getTrackingService() throws ApplicationInitializationException {
    	if (trackingService != null) {
            return trackingService;
        }
        String commandLoggerClassnameProperty = CurrentConfiguration.getProperty(COMMAND_LOGGER_CLASSNAME);
        
        Properties props = new Properties();
        
        if(commandLoggerClassnameProperty != null) {
            trackingService = new CustomizableTrackingService();

            // Search for additional, implementation-specific properties stuff into this string.  
            // They should be delimited by semi-colon - TODO clean this up - sbale 5/3/05
            //
            // Possible examples of expected value of commandLoggerClassnameProperty String variable:
            //
            // com.metamatrix.dqp.spi.basic.FileCommandLogger;dqp.commandLogger.fileName=commandLogFile.txt
            // com.myCode.MyCommandLoggerClass;myFirstCustomProperty=someValue;mySecondCustomProperty=otherValue
            
            List tokens = StringUtil.getTokens(commandLoggerClassnameProperty, ";"); //$NON-NLS-1$
            
            // 1st token is the classname property
            String commandLoggerClassname = (String)tokens.remove(0);
            props.setProperty(DQPServiceProperties.TrackingService.COMMAND_LOGGER_CLASSNAME, commandLoggerClassname);
            
            // Additional tokens are name/value pairs, properties specific to service provider impl
            Iterator i = tokens.iterator();
            while (i.hasNext()) {
                String nameValueString = (String)i.next();
                List nameValuePair = StringUtil.getTokens(nameValueString, "="); //$NON-NLS-1$
                String name = (String)nameValuePair.get(0);
                String value = (String)nameValuePair.get(1);
                props.setProperty(name, value);
            }
        } else {
            trackingService = new DatabaseTrackingService();
        }
        
        trackingService.initialize(props);
        return trackingService;
    }

    /**
     * @return
     */
    private ApplicationService createVDBService() throws ApplicationInitializationException {
        PlatformVDBService pvs = new PlatformVDBService();
        pvs.initialize(new Properties());
        return pvs;
    }
}