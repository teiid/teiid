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

package com.metamatrix.server.query.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.common.comm.ClientServiceRegistrant;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.protocol.URLFactory;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.config.DQPConfigSource;
import com.metamatrix.dqp.config.DQPLauncher;
import com.metamatrix.dqp.internal.process.DQPCore;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.UDFSource;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.dqp.config.PlatformConfigSource;
import com.metamatrix.server.util.ErrorMessageKeys;
import com.metamatrix.server.util.LogConstants;
import com.metamatrix.server.util.ServerPropertyNames;

/**
 * Wraps up a QueryServiceEngine to tie it into the platform concept of services.  Is a remote object.
 * Provides the QueryServiceEngine with a session service object.
 * Configures the QueryServiceEngine from the configuration source.
 * Buffers the QueryServiceEngine from dependencies on CurrentConfiguration.
 * These measures allow the QueryServiceEngine to be instantiated in a light-weight fashion without a full, running "server".
 */
public class QueryService extends AbstractService implements ClientServiceRegistrant, QueryServiceInterface {
    
	private static final String UDF_CLASSPATH_PROPERTY = "metamatrix.server.UDFClasspath"; //$NON-NLS-1$
	private static final String CLASSPATH_DELIMITER = ";"; //$NON-NLS-1$
    private static final String CODE_TABLE_CACHE_NAME = "CodeTableCache"; //$NON-NLS-1$
    private static final String PLAN_CACHE_NAME = "PreparedPlanCache"; //$NON-NLS-1$
    private static final String RESULT_SET_CACHE_NAME = "QueryServiceResultSetCache"; //$NON-NLS-1$

    private DQPCore dqp;

    //=========================================================================
    // Methods from AbstractService
    //=========================================================================

    protected void initService(Properties props) throws Exception {
        // Initialize UDF source and change listener
        String udfSource = null;
        try {
            udfSource = props.getProperty(QueryServicePropertyNames.UDF_SOURCE, "FunctionDefinitions.xmi"); //$NON-NLS-1$
            if (udfSource != null) {
                registerUDFSource(udfSource);
            }
        } catch (Throwable t) {
            LogManager.logError(LogConstants.CTX_QUERY_SERVICE, t, ServerPlugin.Util.getString("QueryService.Unable_to_register_user-defined_function_source__{0}_1", udfSource)); //$NON-NLS-1$
        }

        // Add system properties to the config source
        props.setProperty(
            ServerPropertyNames.SYSTEM_VDB_URL,
            CurrentConfiguration.getProperty(ServerPropertyNames.SYSTEM_VDB_URL));
        DQPConfigSource configSource = new PlatformConfigSource(props, ResourceFinder.getBufferManager(), new Long(getID().getID()));
        dqp = new DQPLauncher(configSource).createDqp();
    }
    
    /**
     * Registers the source with the FunctionLibraryManager
     * @param udfSource the source file for function definitions
     */
    private void registerUDFSource(String udfSource) throws IOException {
    	URL[] urls = null;
    	
    	String extensionClasspath = CurrentConfiguration.getProperty(UDF_CLASSPATH_PROPERTY);
        if (extensionClasspath != null && extensionClasspath.trim().length() > 0){
            try {
                urls = URLFactory.parseURLs(extensionClasspath, CLASSPATH_DELIMITER);
            } catch (MalformedURLException e) {
                String message = ServerPlugin.Util.getString("ExtensionFunctionMetadataSource.Cannot_parse_classpath___{0}___1", extensionClasspath); //$NON-NLS-1$
                LogManager.logWarning(LogCommonConstants.CTX_CONFIG, e, message);            
            }
        }    	
        FunctionLibraryManager.registerSource(new UDFSource(retrieveUDFStream(udfSource), urls));
    }
    
    private InputStream retrieveUDFStream(String udfSource) {
        // Load the user defined function file from the extension source manager
        try {
            byte[] xmlData = ExtensionModuleManager.getInstance().getSource(udfSource);
            return new ByteArrayInputStream(xmlData);
        } catch (Throwable e) {
            throw new MetaMatrixRuntimeException(e,ErrorMessageKeys.function_0005,ServerPlugin.Util.getString(ErrorMessageKeys.function_0005, udfSource));
        }
    }    

    /*
     * @see com.metamatrix.platform.service.controller.AbstractService#closeService()
     */
    protected void closeService() throws Exception {
    	 this.dqp.stop();
    }

    /*
     * @see com.metamatrix.platform.service.controller.AbstractService#waitForServiceToClear()
     */
    protected void waitForServiceToClear() throws Exception {
    }

    /*
     * @see com.metamatrix.platform.service.controller.AbstractService#killService()
     */
    protected void killService() {
    	try {
            closeService();
        } catch (Exception e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#clearCache(com.metamatrix.platform.security.api.SessionToken)
     */
    public void clearCache(SessionToken sessionToken)
        throws ComponentNotFoundException, ServiceStateException, RemoteException {
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#getAllQueries()
     */
    public Collection getAllQueries() throws ServiceStateException, RemoteException {
    	return dqp.getRequests();
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQueries(com.metamatrix.platform.security.api.SessionToken, boolean, boolean)
     */
    public void cancelQueries(SessionToken sessionToken, boolean shouldRollback)
        throws ServiceStateException, InvalidRequestIDException, MetaMatrixComponentException, RemoteException {
        this.dqp.terminateConnection(sessionToken.getSessionID().toString());
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#getQueriesForSession(com.metamatrix.platform.security.api.SessionToken)
     */
    public Collection getQueriesForSession(SessionToken userToken) throws ServiceStateException, RemoteException {
        return this.dqp.getRequestsByClient(userToken.getSessionID().toString());
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, boolean, boolean)
     */
    public void cancelQuery(RequestID requestID, boolean shouldRollback)
        throws InvalidRequestIDException, MetaMatrixComponentException, ServiceStateException, RemoteException {
    	try {
			this.dqp.cancelRequest(requestID);
		} catch (MetaMatrixProcessingException e) {
			throw new InvalidRequestIDException(e, e.getMessage());
		}
    }
    
    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, int)
     */
    public void cancelQuery(RequestID requestID, int nodeID)
        throws InvalidRequestIDException, MetaMatrixComponentException, ServiceStateException, RemoteException {
		this.dqp.cancelAtomicRequest(requestID, nodeID);
    }    
    
    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, null is returned.
     */
    public Collection getQueueStatistics() {
        return this.dqp.getQueueStatistics();
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, null is returned.
     */
    public WorkerPoolStats getQueueStatistics(String name) {
    	WorkerPoolStats poolStats = new WorkerPoolStats();
        Collection results = this.dqp.getQueueStatistics(name);
        if ( results != null ) {
            Iterator resultsItr = results.iterator();
            // There is only one result (if any) in this results collection
            if ( resultsItr.hasNext() ) {
                Object aPoolStat = resultsItr.next();
                if ( aPoolStat != null && aPoolStat instanceof WorkerPoolStats ) {
                    poolStats = (WorkerPoolStats) aPoolStat;
                }
            }
        }
        return poolStats;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.platform.service.api.CacheAdmin#getCaches()
     */
    public Map getCaches() throws MetaMatrixComponentException, RemoteException {
    	Map names = new HashMap();
        names.put(CODE_TABLE_CACHE_NAME, CacheAdmin.CODE_TABLE_CACHE);
        names.put(PLAN_CACHE_NAME, CacheAdmin.PREPARED_PLAN_CACHE);
        names.put(RESULT_SET_CACHE_NAME, CacheAdmin.QUERY_SERVICE_RESULT_SET_CACHE);
        return names;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.platform.service.api.CacheAdmin#clearCache(java.lang.String, java.util.Properties)
     */
    public void clearCache(String name, Properties props) throws MetaMatrixComponentException, RemoteException {
    	if(name.equals(CODE_TABLE_CACHE_NAME)) {
        	this.dqp.clearCodeTableCache();
        } else if(name.equals(PLAN_CACHE_NAME)) {
        	this.dqp.clearPlanCache();
        } else if(name.equals(RESULT_SET_CACHE_NAME)) {
            this.dqp.clearResultSetCache();
        }
    }
    
    /** 
     * @see com.metamatrix.common.comm.ClientServiceRegistrant#setClientServiceRegistry(com.metamatrix.common.comm.ClientServiceRegistry)
     * @since 4.2
     */
    public void setClientServiceRegistry(ClientServiceRegistry registry) {
        registry.registerClientService(ClientSideDQP.class, this.dqp);
    }

}
