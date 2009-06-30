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

package com.metamatrix.server.query.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.Transaction;
import org.teiid.dqp.internal.process.DQPCore;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.UDFSource;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.dqp.config.PlatformConfigSource;
import com.metamatrix.server.util.LogConstants;

/**
 * Wraps up a QueryServiceEngine to tie it into the platform concept of services.  Is a remote object.
 * Provides the QueryServiceEngine with a session service object.
 * Configures the QueryServiceEngine from the configuration source.
 * Buffers the QueryServiceEngine from dependencies on CurrentConfiguration.getInstance().
 * These measures allow the QueryServiceEngine to be instantiated in a light-weight fashion without a full, running "server".
 */
public class QueryService extends AbstractService implements QueryServiceInterface {
    
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
        } catch (IOException t) {
            LogManager.logError(LogConstants.CTX_QUERY_SERVICE, t, ServerPlugin.Util.getString("QueryService.Unable_to_register_user-defined_function_source__{0}_1", udfSource)); //$NON-NLS-1$
        }

        DQPConfigSource configSource = new PlatformConfigSource(props, CurrentConfiguration.getInstance().getProperties(), new Long(getID().getID()), CurrentConfiguration.getInstance().getDefaultHost(), CurrentConfiguration.getInstance().getProcessName());
        dqp = new DQPCore();
        dqp.start(configSource);
    }
    
    /**
     * Registers the source with the FunctionLibraryManager
     * @param udfSource the source file for function definitions
     */
    private void registerUDFSource(String udfSource) throws IOException {
        try {
        	InputStream in = retrieveUDFStream(udfSource);
            if (in != null) {
            	FunctionLibraryManager.registerSource(new UDFSource(in, Thread.currentThread().getContextClassLoader()));
            }        	
        } catch(ExtensionModuleNotFoundException e) {
        	LogManager.logDetail(LogCommonConstants.CTX_CONFIG, e, ServerPlugin.Util.getString("QueryService.no_udf")); //$NON-NLS-1$
        } catch(MetaMatrixComponentException e) {
        	LogManager.logDetail(LogCommonConstants.CTX_CONFIG, e, ServerPlugin.Util.getString("QueryService.no_udf")); //$NON-NLS-1$
        }
    }
    
    private InputStream retrieveUDFStream(String udfSource) throws ExtensionModuleNotFoundException, MetaMatrixComponentException {
        // Load the user defined function file from the extension source manager
        byte[] xmlData = ExtensionModuleManager.getInstance().getSource(udfSource);
        return new ByteArrayInputStream(xmlData);
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
        throws ComponentNotFoundException{
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#getAllQueries()
     */
    public Collection getAllQueries() {
    	return dqp.getRequests();
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQueries(com.metamatrix.platform.security.api.SessionToken, boolean, boolean)
     */
    public void cancelQueries(SessionToken sessionToken, boolean shouldRollback)
        throws MetaMatrixComponentException{
        this.dqp.terminateConnection(sessionToken.getSessionID().toString());
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#getQueriesForSession(com.metamatrix.platform.security.api.SessionToken)
     */
    public Collection getQueriesForSession(SessionToken userToken) {
        return this.dqp.getRequestsByClient(userToken.getSessionID().toString());
    }

    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, boolean, boolean)
     */
    public void cancelQuery(RequestID requestID, boolean shouldRollback)
        throws InvalidRequestIDException, MetaMatrixComponentException {
    	if (!this.dqp.cancelRequest(requestID)) {
			throw new InvalidRequestIDException(DQPPlugin.Util.getString("DQPCore.failed_to_cancel")); //$NON-NLS-1$
		}
    }
    
    /*
     * @see com.metamatrix.server.query.service.QueryServiceInterface#cancelQuery(com.metamatrix.dqp.message.RequestID, int)
     */
    public void cancelQuery(AtomicRequestID ari)
        throws InvalidRequestIDException, MetaMatrixComponentException {
		this.dqp.cancelAtomicRequest(ari);
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
    public Map getCaches() throws MetaMatrixComponentException {
    	Map names = new HashMap();
        names.put(CODE_TABLE_CACHE_NAME, CacheAdmin.CODE_TABLE_CACHE);
        names.put(PLAN_CACHE_NAME, CacheAdmin.PREPARED_PLAN_CACHE);
        names.put(RESULT_SET_CACHE_NAME, CacheAdmin.QUERY_SERVICE_RESULT_SET_CACHE);
        return names;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.platform.service.api.CacheAdmin#clearCache(java.lang.String, java.util.Properties)
     */
    public void clearCache(String name, Properties props) throws MetaMatrixComponentException {
    	if(name.equals(CODE_TABLE_CACHE_NAME)) {
        	this.dqp.clearCodeTableCache();
        } else if(name.equals(PLAN_CACHE_NAME)) {
        	this.dqp.clearPlanCache();
        } else if(name.equals(RESULT_SET_CACHE_NAME)) {
            this.dqp.clearResultSetCache();
        }
    }
    
    @Override
    public void init(ServiceID id, DeployedComponentID deployedComponentID,
    		Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager) {
    	super.init(id, deployedComponentID, props, listenerRegistry, clManager);
    	listenerRegistry.registerClientService(ClientSideDQP.class, this.dqp, LogConstants.CTX_QUERY_SERVICE);
    }
    
    @Override
    public Collection<Transaction> getTransactions() {
    	TransactionService ts = getTransactionService();
    	if (ts == null) {
    		return Collections.emptyList();
    	}
    	return ts.getTransactions();
    }
    
    @Override
    public void terminateTransaction(String transactionId, String sessionId)
    		throws AdminException {
    	TransactionService ts = getTransactionService();
    	if (ts != null) {
    		ts.terminateTransaction(transactionId, sessionId);
    	}
    }
    
    @Override
    public void terminateTransaction(Xid transactionId) throws AdminException {
    	TransactionService ts = getTransactionService();
    	if (ts != null) {
    		ts.terminateTransaction(transactionId);
    	}
    }
    
    protected TransactionService getTransactionService() {
    	return this.dqp.getTransactionServiceDirect();
    }

}
