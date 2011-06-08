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
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.util.Locale;
import java.util.ResourceBundle;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.as.controller.registry.ModelNodeRegistration;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.util.threadpool.ThreadPool;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.jboss.ClusterableCacheFactory;
import org.teiid.deployers.VDBRepository;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;

public class TeiidExtension implements Extension {
	
	private static final String ACTIVE_SESSION_COUNT = "active-session-count";
	private static final String RUNTIME_VERSION = "runtime-version";
	private static final String REQUESTS_PER_SESSION = "requests-per-session";
	private static final String ACTIVE_SESSIONS = "active-sessions";
	private static final String REQUESTS_PER_VDB = "requests-per-vdb";
	private static final String LONG_RUNNING_QUERIES = "long-running-queries";
	private static final String TERMINATE_SESSION = "terminate-session";
	private static final String CANCEL_QUERY = "cancel-query";
	private static final String CACHE_TYPES = "cache-types";
	private static final String CLEAR_CACHE = "clear-cache";
	private static final String CACHE_STATISTICS = "cache-statistics";
	private static final String WORKERPOOL_STATISTICS = "workerpool-statistics";
	private static final String ACTIVE_TRANSACTIONS = "active-transactions";
	private static final String TERMINATE_TRANSACTION = "terminate-transaction";
	private static final String MERGE_VDBS = "merge-vdbs";
	private static final String EXECUTE_QUERY = "execute-query";
	
	public static final String SUBSYSTEM_NAME = "teiid"; //$NON-NLS-1$
	private static TeiidSubsystemParser parser = new TeiidSubsystemParser();
	private static TeiidSubsystemDescription teiidSubsystem = new TeiidSubsystemDescription();
	
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration registration = context.registerSubsystem(SUBSYSTEM_NAME);
		
		registration.registerXMLElementWriter(parser);
		
		final ModelNodeRegistration subsystem = registration.registerSubsystemModel(teiidSubsystem);
		subsystem.registerOperationHandler(ModelDescriptionConstants.ADD, subsystemAddOperation, subsystemAddDescription);
		//subsystem.registerOperationHandler(ModelDescriptionConstants.DESCRIBE, describe, describe, false);
		
		QueryEngineModelHandler op;
		subsystem.registerReadOnlyAttribute(RUNTIME_VERSION, new GetRuntimeVersion(RUNTIME_VERSION), Storage.RUNTIME); 
		subsystem.registerReadOnlyAttribute(ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(ACTIVE_SESSION_COUNT), Storage.RUNTIME); 
		
		op = new GetActiveSessions(ACTIVE_SESSIONS);
		subsystem.registerOperationHandler(ACTIVE_SESSIONS, op, op); 
		
		op = new GetRequestsPerSession(REQUESTS_PER_SESSION);
		subsystem.registerOperationHandler(REQUESTS_PER_SESSION, op, op);

		op = new GetRequestsPerVDB(REQUESTS_PER_VDB);
		subsystem.registerOperationHandler(REQUESTS_PER_VDB, op, op);
		
		op = new GetLongRunningQueries(LONG_RUNNING_QUERIES);
		subsystem.registerOperationHandler(LONG_RUNNING_QUERIES, op, op);		
		
		op = new TerminateSession(TERMINATE_SESSION);
		subsystem.registerOperationHandler(TERMINATE_SESSION, op, op);	
		
		op = new CancelQuery(CANCEL_QUERY);
		subsystem.registerOperationHandler(CANCEL_QUERY, op, op);		
		
		op = new CacheTypes(CACHE_TYPES);
		subsystem.registerOperationHandler(CACHE_TYPES, op, op);	
		
		op = new ClearCache(CLEAR_CACHE);
		subsystem.registerOperationHandler(CLEAR_CACHE, op, op);	
		
		op = new CacheStatistics(CACHE_STATISTICS);
		subsystem.registerOperationHandler(CACHE_STATISTICS, op, op);		
		
		op = new WorkerPoolStatistics(WORKERPOOL_STATISTICS);
		subsystem.registerOperationHandler(WORKERPOOL_STATISTICS, op, op);		
		
		op = new ActiveTransactions(ACTIVE_TRANSACTIONS);
		subsystem.registerOperationHandler(ACTIVE_TRANSACTIONS, op, op);	
		
		op = new TerminateTransaction(TERMINATE_TRANSACTION);
		subsystem.registerOperationHandler(TERMINATE_TRANSACTION, op, op);		
		
		op = new MergeVDBs(MERGE_VDBS);
		subsystem.registerOperationHandler(MERGE_VDBS, op, op);	
		
		op = new ExecuteQuery(EXECUTE_QUERY);
		subsystem.registerOperationHandler(EXECUTE_QUERY, op, op);			
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(Namespace.CURRENT.getUri(), parser);
	}
	
	static DescriptionProvider subsystemAddDescription = new DescriptionProvider() {
		@Override
		public ModelNode getModelDescription(Locale locale) {
			final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
			
	        final ModelNode node = new ModelNode();
	        node.get(OPERATION_NAME).set(ADD);
	        node.get(DESCRIPTION).set("susbsystem.add"); //$NON-NLS-1$
	        
	        TeiidSubsystemDescription.getQueryEngineDescription(node.get(CHILDREN, Configuration.QUERY_ENGINE), REQUEST_PROPERTIES, bundle);
	        return node;
		}
	};	
	
	static ModelAddOperationHandler subsystemAddOperation = new ModelAddOperationHandler() {
		@Override
		public OperationResult execute(OperationContext context, final ModelNode operation, ResultHandler resultHandler) throws OperationFailedException {
			final ModelNode modelNode = context.getSubModel();
			
			final ModelNode queryEngineNode = operation.require(Configuration.QUERY_ENGINE);
			modelNode.set(Configuration.QUERY_ENGINE).set(queryEngineNode.clone());
			
	        RuntimeOperationContext runtime = context.getRuntimeContext();
	        if (runtime != null) {
	            RuntimeTask task = new RuntimeTask() {
	                @Override
	                public void execute(RuntimeTaskContext context) throws OperationFailedException {
	                	
	                	VDBRepository vdbRepo = buildVDBRepository(queryEngineNode);
	                	
	                	SessionServiceImpl sessionService = buildSessionService(queryEngineNode);
	                	sessionService.setVDBRepository(vdbRepo);
	                	
	                	BufferServiceImpl bufferManager = buildBufferManager(queryEngineNode.get(Configuration.BUFFER_SERVICE));
	                	CacheFactory cacheFactory = getCacheFactory(queryEngineNode.get(Configuration.CACHE_FACORY));
	                	
	                	CacheConfiguration resultsetCache = buildCacheConfig(queryEngineNode.get(Configuration.RESULTSET_CACHE));
	                	CacheConfiguration preparePlanCache = buildCacheConfig(queryEngineNode.get(Configuration.PREPAREDPLAN_CACHE));
	                	
	                	SocketConfiguration jdbc = buildSocketConfiguration(queryEngineNode.get(Configuration.JDBC));
	                	SocketConfiguration odbc = buildSocketConfiguration(queryEngineNode.get(Configuration.ODBC));
	                	
	                	// now build the engine
	                	RuntimeEngineDeployer engine = buildRuntimeEngine(queryEngineNode);
	                	engine.setJdbcSocketConfiguration(jdbc);
	                	engine.setOdbcSocketConfiguration(odbc);
	                	engine.setSessionService(sessionService);
	                	engine.setBufferService(bufferManager);
	                	engine.setVDBRepository(vdbRepo);
	                	engine.setCacheFactory(cacheFactory);
	                	engine.setResultsetCacheConfig(resultsetCache);
	                	engine.setPreparedPlanCacheConfig(preparePlanCache);
	                	engine.setSecurityHelper(new JBossSecurityHelper());
	                	
	                    ServiceTarget target = context.getServiceTarget();
	                    ServiceBuilder<ClientServiceRegistry> serviceBuilder = target.addService(RuntimeEngineDeployer.SERVICE_NAME, engine);
	                    
	                    serviceBuilder.addDependency(ServiceName.JBOSS.append("connector", "workmanager"), WorkManager.class, engine.workManagerInjector); //$NON-NLS-1$ //$NON-NLS-2$
	                    serviceBuilder.addDependency(ServiceName.JBOSS.append("txn", "XATerminator"), XATerminator.class, engine.xaTerminatorInjector); //$NON-NLS-1$ //$NON-NLS-2$
	                    serviceBuilder.addDependency(ServiceName.JBOSS.append("txn", "TransactionManager"), TransactionManager.class, engine.txnManagerInjector); //$NON-NLS-1$ //$NON-NLS-2$
	                    //TODO: Threads??
	                    serviceBuilder.addDependency(ServiceName.JBOSS.append("???", "???"), ThreadPool.class, engine.threadPoolInjector); //$NON-NLS-1$ //$NON-NLS-2$
	                    
	                    serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
	                    serviceBuilder.install();	                    
	                }
	                

					private RuntimeEngineDeployer buildRuntimeEngine(ModelNode node) {
						RuntimeEngineDeployer engine = new RuntimeEngineDeployer();
						
	                	if (node.get(Configuration.JNDI_NAME) != null) {
	                		engine.setJndiName(node.get(Configuration.JNDI_NAME).asString());
	                	}						
	                	if (node.get(Configuration.MAX_THREADS) != null) {
	                		engine.setMaxThreads(node.get(Configuration.MAX_THREADS).asInt());
	                	}
	                	if (node.get(Configuration.MAX_ACTIVE_PLANS) != null) {
	                		engine.setMaxActivePlans(node.get(Configuration.MAX_ACTIVE_PLANS).asInt());
	                	}
	                	if (node.get(Configuration.USER_REQUEST_SOURCE_CONCURRENCY) != null) {
	                		engine.setUserRequestSourceConcurrency(node.get(Configuration.USER_REQUEST_SOURCE_CONCURRENCY).asInt());
	                	}	
	                	if (node.get(Configuration.TIME_SLICE_IN_MILLI) != null) {
	                		engine.setTimeSliceInMilli(node.get(Configuration.TIME_SLICE_IN_MILLI).asInt());
	                	}
	                	if (node.get(Configuration.MAX_ROWS_FETCH_SIZE) != null) {
	                		engine.setMaxRowsFetchSize(node.get(Configuration.MAX_ROWS_FETCH_SIZE).asInt());
	                	}
	                	if (node.get(Configuration.LOB_CHUNK_SIZE_IN_KB) != null) {
	                		engine.setLobChunkSizeInKB(node.get(Configuration.LOB_CHUNK_SIZE_IN_KB).asInt());
	                	}
	                	if (node.get(Configuration.USE_DATA_ROLES) != null) {
	                		engine.setUseDataRoles(node.get(Configuration.USE_DATA_ROLES).asBoolean());
	                	}
	                	if (node.get(Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT) != null) {
	                		engine.setAllowCreateTemporaryTablesByDefault(node.get(Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT).asBoolean());
	                	}
	                	if (node.get(Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT) != null) {
	                		engine.setAllowFunctionCallsByDefault(node.get(Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT).asBoolean());
	                	}
	                	if (node.get(Configuration.QUERY_THRESHOLD_IN_SECS) != null) {
	                		engine.setQueryThresholdInSecs(node.get(Configuration.QUERY_THRESHOLD_IN_SECS).asInt());
	                	}
	                	if (node.get(Configuration.MAX_SOURCE_ROWS) != null) {
	                		engine.setMaxSourceRows(node.get(Configuration.MAX_SOURCE_ROWS).asInt());
	                	}
	                	if (node.get(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS) != null) {
	                		engine.setExceptionOnMaxSourceRows(node.get(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS).asBoolean());
	                	}
	                	if (node.get(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED) != null) {
	                		engine.setMaxODBCLobSizeAllowed(node.get(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED).asInt());
	                	}
	                	if (node.get(Configuration.EVENT_DISTRIBUTOR_NAME) != null) {
	                		engine.setEventDistributorName(node.get(Configuration.EVENT_DISTRIBUTOR_NAME).asString());
	                	}
	                	if (node.get(Configuration.DETECTING_CHANGE_EVENTS) != null) {
	                		engine.setDetectingChangeEvents(node.get(Configuration.DETECTING_CHANGE_EVENTS).asBoolean());
	                	}	                	
						return engine;
					}


					private SessionServiceImpl buildSessionService(ModelNode node) {
	                	SessionServiceImpl sessionService = new SessionServiceImpl();
	                	if (node.get(Configuration.JDBC_SECURITY_DOMAIN) != null) {
	                		sessionService.setSecurityDomains(node.get(Configuration.JDBC_SECURITY_DOMAIN).asString());
	                	}
	                	if (node.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT) != null) {
	                		sessionService.setSessionExpirationTimeLimit(node.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT).asInt());
	                	}
	                	if (node.get(Configuration.MAX_SESSIONS_ALLOWED) != null) {
	                		sessionService.setSessionMaxLimit(node.get(Configuration.MAX_SESSIONS_ALLOWED).asInt());
	                	}		                	
	                	return sessionService;
	                }
	                
	                private VDBRepository buildVDBRepository(ModelNode node) {
	                	SystemFunctionManager systemFunctionManager = new SystemFunctionManager();
	                	if (node.get(Configuration.ALLOW_ENV_FUNCTION) != null) {
	                		systemFunctionManager.setAllowEnvFunction(node.get(Configuration.ALLOW_ENV_FUNCTION).asBoolean());
	                	}
	                	else {
	                		systemFunctionManager.setAllowEnvFunction(false);
	                	}
	                	
	                	VDBRepository vdbRepository = new VDBRepository();
	                	vdbRepository.setSystemFunctionManager(systemFunctionManager);	
	                	return vdbRepository;               	
	                }
	                
	                private BufferServiceImpl  buildBufferManager(ModelNode node) {
	                	BufferServiceImpl bufferManger = new BufferServiceImpl();
	                	
	                	if (node == null) {
	                		return bufferManger;
	                	}
	                	
	                	if (node.get(Configuration.USE_DISK) != null) {
	                		bufferManger.setUseDisk(node.get(Configuration.USE_DISK).asBoolean());
	                	}	                	
	                	if (node.get(Configuration.DISK_DIRECTORY) != null) {
	                		bufferManger.setDiskDirectory(node.get(Configuration.DISK_DIRECTORY).asString());
	                	}	                	
	                	if (node.get(Configuration.PROCESSOR_BATCH_SIZE) != null) {
	                		bufferManger.setProcessorBatchSize(node.get(Configuration.PROCESSOR_BATCH_SIZE).asInt());
	                	}	
	                	if (node.get(Configuration.CONNECTOR_BATCH_SIZE) != null) {
	                		bufferManger.setConnectorBatchSize(node.get(Configuration.CONNECTOR_BATCH_SIZE).asInt());
	                	}	
	                	if (node.get(Configuration.MAX_RESERVE_BATCH_COLUMNS) != null) {
	                		bufferManger.setMaxReserveBatchColumns(node.get(Configuration.MAX_RESERVE_BATCH_COLUMNS).asInt());
	                	}
	                	if (node.get(Configuration.MAX_PROCESSING_BATCH_COLUMNS) != null) {
	                		bufferManger.setMaxProcessingBatchesColumns(node.get(Configuration.MAX_PROCESSING_BATCH_COLUMNS).asInt());
	                	}	
	                	if (node.get(Configuration.MAX_FILE_SIZE) != null) {
	                		bufferManger.setMaxFileSize(node.get(Configuration.MAX_FILE_SIZE).asInt());
	                	}
	                	if (node.get(Configuration.MAX_BUFFER_SPACE) != null) {
	                		bufferManger.setMaxBufferSpace(node.get(Configuration.MAX_BUFFER_SPACE).asInt());
	                	}
	                	if (node.get(Configuration.MAX_OPEN_FILES) != null) {
	                		bufferManger.setMaxOpenFiles(node.get(Configuration.MAX_OPEN_FILES).asInt());
	                	}	                	
	                	return bufferManger;
	                }
	                
	                private CacheFactory getCacheFactory(ModelNode node) {
	                	ClusterableCacheFactory cacheFactory = new ClusterableCacheFactory();	
	                	
	                	if (node.get(Configuration.ENABLED) != null) {
	                		cacheFactory.setEnabled(node.get(Configuration.ENABLED).asBoolean());
	                	}
	                	else {
	                		cacheFactory.setEnabled(true);
	                	}
	                	if (node.get(Configuration.CACHE_SERVICE_JNDI_NAME) != null) {
	                		cacheFactory.setCacheManager(node.get(Configuration.CACHE_SERVICE_JNDI_NAME).asString());
	                	}	                	
	                	if (node.get(Configuration.RESULTSET_CACHE_NAME) != null) {
	                		cacheFactory.setResultsetCacheName(node.get(Configuration.RESULTSET_CACHE_NAME).asString());
	                	}		                	
	                	return cacheFactory;
	                }
	                
	                private CacheConfiguration buildCacheConfig(ModelNode node) {
	                	CacheConfiguration cacheConfig = new CacheConfiguration();
	                	
	                	if (node.get(Configuration.ENABLED) != null) {
	                		cacheConfig.setEnabled(node.get(Configuration.ENABLED).asBoolean());
	                	}
	                	if (node.get(Configuration.MAX_ENTRIES) != null) {
	                		cacheConfig.setMaxEntries(node.get(Configuration.MAX_ENTRIES).asInt());
	                	}
	                	if (node.get(Configuration.MAX_AGE_IN_SECS) != null) {
	                		cacheConfig.setMaxAgeInSeconds(node.get(Configuration.MAX_AGE_IN_SECS).asInt());
	                	}
	                	if (node.get(Configuration.MAX_STALENESS) != null) {
	                		cacheConfig.setMaxStaleness(node.get(Configuration.MAX_STALENESS).asInt());
	                	}
	                	if (node.get(Configuration.CACHE_TYPE) != null) {
	                		cacheConfig.setType(node.get(Configuration.CACHE_TYPE).asString());
	                	}
	                	if (node.get(Configuration.CACHE_LOCATION) != null) {
	                		cacheConfig.setLocation(node.get(Configuration.CACHE_LOCATION).asString());
	                	}	   	                	
	                	return cacheConfig;
					}	                
	                
					private SocketConfiguration buildSocketConfiguration(ModelNode node) {
						SocketConfiguration socket = new SocketConfiguration();
						
	                	if (node.get(Configuration.ENABLED) != null) {
	                		socket.setEnabled(node.get(Configuration.ENABLED).asBoolean());
	                	}		
	                	if (node.get(Configuration.SOCKET_BINDING) != null) {
	                		socket.setBindAddress(node.get(Configuration.SOCKET_BINDING).asString());
	                	}
	                	if (node.get(Configuration.MAX_SOCKET_THREAD_SIZE) != null) {
	                		socket.setMaxSocketThreads(node.get(Configuration.MAX_SOCKET_THREAD_SIZE).asInt());
	                	}
	                	if (node.get(Configuration.IN_BUFFER_SIZE) != null) {
	                		socket.setInputBufferSize(node.get(Configuration.IN_BUFFER_SIZE).asInt());
	                	}	
	                	if (node.get(Configuration.OUT_BUFFER_SIZE) != null) {
	                		socket.setOutputBufferSize(node.get(Configuration.OUT_BUFFER_SIZE).asInt());
	                	}		   
	                	
	                	SSLConfiguration ssl = new SSLConfiguration();
	                	ssl.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
	                	
	                	if (node.get(Configuration.SSL) != null) {
	                		ModelNode sslNode = node.get(Configuration.SSL);
	                		
		                	if (sslNode.get(Configuration.SSL_MODE) != null) {
		                		ssl.setMode(sslNode.get(Configuration.SSL_MODE).asString());
		                	}
		                	
		                	if (sslNode.get(Configuration.KEY_STORE_FILE) != null) {
		                		ssl.setKeystoreFilename(sslNode.get(Configuration.KEY_STORE_FILE).asString());
		                	}	
		                	
		                	if (sslNode.get(Configuration.KEY_STORE_PASSWD) != null) {
		                		ssl.setKeystorePassword(sslNode.get(Configuration.KEY_STORE_PASSWD).asString());
		                	}	
		                	
		                	if (sslNode.get(Configuration.KEY_STORE_TYPE) != null) {
		                		ssl.setKeystoreType(sslNode.get(Configuration.KEY_STORE_TYPE).asString());
		                	}		
		                	
		                	if (sslNode.get(Configuration.SSL_PROTOCOL) != null) {
		                		ssl.setSslProtocol(sslNode.get(Configuration.SSL_PROTOCOL).asString());
		                	}	
		                	if (sslNode.get(Configuration.KEY_MANAGEMENT_ALG) != null) {
		                		ssl.setKeymanagementAlgorithm(sslNode.get(Configuration.KEY_MANAGEMENT_ALG).asString());
		                	}
		                	if (sslNode.get(Configuration.TRUST_FILE) != null) {
		                		ssl.setTruststoreFilename(sslNode.get(Configuration.TRUST_FILE).asString());
		                	}
		                	if (sslNode.get(Configuration.TRUST_PASSWD) != null) {
		                		ssl.setTruststorePassword(sslNode.get(Configuration.TRUST_PASSWD).asString());
		                	}
		                	if (sslNode.get(Configuration.AUTH_MODE) != null) {
		                		ssl.setAuthenticationMode(sslNode.get(Configuration.AUTH_MODE).asString());
		                	}		                	
	                	}
	                	
	                	socket.setSSLConfiguration(ssl);
						return socket;
					}	                

	            };
	            runtime.setRuntimeTask(task);
	        }
	        
	        // compensating is remove operation
	        final ModelNode address = operation.require(OP_ADDR);
	        BasicOperationResult operationResult = new BasicOperationResult(Util.getResourceRemoveOperation(address));
	        resultHandler.handleResultComplete();
	        return operationResult;
		}
		
	};	

}
