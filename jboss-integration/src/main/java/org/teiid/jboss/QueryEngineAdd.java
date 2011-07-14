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

import java.util.List;
import java.util.concurrent.Executor;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.as.controller.AbstractBoottimeAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ManagedReferenceInjector;
import org.jboss.as.naming.NamingStore;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.deployment.Phase;
import org.jboss.as.server.services.path.RelativePathService;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.inject.ConcurrentMapInjector;
import org.jboss.msc.service.*;
import org.jboss.msc.value.InjectedValue;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.deployers.*;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.services.BufferServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;

class QueryEngineAdd extends AbstractBoottimeAddStepHandler {

	@Override
	protected void populateModel(ModelNode operation, ModelNode model) {
		final ModelNode queryEngineNode = operation.require(Configuration.QUERY_ENGINE);
		model.set(Configuration.QUERY_ENGINE).set(queryEngineNode.clone());

	}
	
	@Override
    protected void performBoottime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers) throws OperationFailedException {

    	final ModelNode queryEngineNode = operation.require(Configuration.QUERY_ENGINE);
    	ServiceTarget target = context.getServiceTarget();
    	
    	final VDBRepository vdbRepo = buildVDBRepository(queryEngineNode);
    	final JBossLifeCycleListener shutdownListener = new JBossLifeCycleListener();
    	
    	SystemVDBDeployer systemVDB = new SystemVDBDeployer();
    	systemVDB.setVDBRepository(vdbRepo);
    	SystemVDBService systemVDBService = new SystemVDBService(systemVDB);
    	newControllers.add(target.addService(TeiidServiceNames.SYSTEM_VDB, systemVDBService).install());
    	
    	//FIXME *******************
    	final ObjectSerializer serializer = new ObjectSerializer("/tmp");
    	//FIXME *******************
    	
    	final TranslatorRepository translatorRepo = new TranslatorRepository();
    	TranslatorRepositoryService translatorService = new TranslatorRepositoryService(translatorRepo);
    	newControllers.add(target.addService(TeiidServiceNames.TRANSLATOR_REPO, translatorService).install());

    	newControllers.add(RelativePathService.addService(TeiidServiceNames.BUFFER_DIR, "teiid-buffer", "jboss.server.temp.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	// TODO: remove verbose service by moving the buffer service from runtime project
    	final BufferServiceImpl bufferManager = buildBufferManager(queryEngineNode.get(Configuration.BUFFER_SERVICE));
    	BufferManagerService bufferService = new BufferManagerService(bufferManager);
    	ServiceBuilder<BufferServiceImpl> bufferServiceBuilder = target.addService(TeiidServiceNames.BUFFER_MGR, bufferService);
    	bufferServiceBuilder.addDependency(TeiidServiceNames.BUFFER_DIR, String.class, bufferService.pathInjector);
    	newControllers.add(bufferServiceBuilder.install());

    	CacheFactory cacheFactory = getCacheFactory(queryEngineNode.get(Configuration.CACHE_FACORY));
    	CacheConfiguration resultsetCache = buildCacheConfig(queryEngineNode.get(Configuration.RESULTSET_CACHE));
    	CacheConfiguration preparePlanCache = buildCacheConfig(queryEngineNode.get(Configuration.PREPAREDPLAN_CACHE));
    	
    	SocketConfiguration jdbc = null;
    	if (queryEngineNode.hasDefined(Configuration.JDBC)) {
    		jdbc = buildSocketConfiguration(queryEngineNode.get(Configuration.JDBC));
    	}
    	
    	SocketConfiguration odbc = null;
    	if (queryEngineNode.hasDefined(Configuration.ODBC)) {
    		odbc = buildSocketConfiguration(queryEngineNode.get(Configuration.ODBC));
    	}
    	
    	String asyncExecutor = "teiid-async"; //$NON-NLS-1$
    	if (queryEngineNode.hasDefined(Configuration.ASYNC_THREAD_GROUP)) {
    		asyncExecutor = queryEngineNode.get(Configuration.ASYNC_THREAD_GROUP).asString();
    	}	                	
    	
    	// now build the engine
    	final RuntimeEngineDeployer engine = buildQueryEngine(queryEngineNode);
    	engine.setJdbcSocketConfiguration(jdbc);
    	engine.setOdbcSocketConfiguration(odbc);
    	engine.setVDBRepository(vdbRepo);
    	engine.setCacheFactory(cacheFactory);
    	engine.setResultsetCacheConfig(resultsetCache);
    	engine.setPreparedPlanCacheConfig(preparePlanCache);
    	engine.setSecurityHelper(new JBossSecurityHelper());
    	engine.setTranslatorRepository(translatorRepo);
    	
        
        ServiceBuilder<ClientServiceRegistry> serviceBuilder = target.addService(TeiidServiceNames.ENGINE, engine);
        
        serviceBuilder.addDependency(ServiceName.JBOSS.append("connector", "workmanager"), WorkManager.class, engine.workManagerInjector); //$NON-NLS-1$ //$NON-NLS-2$
        serviceBuilder.addDependency(ServiceName.JBOSS.append("txn", "XATerminator"), XATerminator.class, engine.xaTerminatorInjector); //$NON-NLS-1$ //$NON-NLS-2$
        serviceBuilder.addDependency(ServiceName.JBOSS.append("txn", "TransactionManager"), TransactionManager.class, engine.txnManagerInjector); //$NON-NLS-1$ //$NON-NLS-2$
        serviceBuilder.addDependency(ServiceName.JBOSS.append("thread", "executor", asyncExecutor), Executor.class, engine.threadPoolInjector); //$NON-NLS-1$ //$NON-NLS-2$
        serviceBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferServiceImpl.class, engine.bufferServiceInjector);
        serviceBuilder.addDependency(TeiidServiceNames.SYSTEM_VDB, SystemVDBDeployer.class,  new InjectedValue<SystemVDBDeployer>());
        
        if (jdbc != null) {
        	serviceBuilder.addDependency(ServiceName.JBOSS.append("binding", jdbc.getSocketBinding()), SocketBinding.class, engine.jdbcSocketBindingInjector); //$NON-NLS-1$
        }
        
        if (odbc != null) {
        	serviceBuilder.addDependency(ServiceName.JBOSS.append("binding", odbc.getSocketBinding()), SocketBinding.class, engine.odbcSocketBindingInjector); //$NON-NLS-1$
        }
        
        // register JNDI Name
        ServiceName javaContext = ServiceName.JBOSS.append("naming", "context", "java"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        BinderService binder = new BinderService(LocalServerConnection.TEIID_RUNTIME);
        ServiceBuilder<ManagedReferenceFactory> namingBuilder = target.addService(javaContext.append(LocalServerConnection.TEIID_RUNTIME), binder);
        namingBuilder.addDependency(javaContext, NamingStore.class, binder.getNamingStoreInjector());
        namingBuilder.addDependency(TeiidServiceNames.ENGINE, RuntimeEngineDeployer.class, new ManagedReferenceInjector<RuntimeEngineDeployer>(binder.getManagedObjectInjector()));
        namingBuilder.setInitialMode(ServiceController.Mode.ON_DEMAND);
        newControllers.add(namingBuilder.install());
        
        
        // add security domains
        if ( queryEngineNode.hasDefined(Configuration.SECURITY_DOMAIN)) {
	        String domainNameOrder = queryEngineNode.get(Configuration.SECURITY_DOMAIN).asString();
	        if (domainNameOrder != null && domainNameOrder.trim().length()>0) {
	        	LogManager.logInfo(LogConstants.CTX_SECURITY, "Security Enabled: true"); //$NON-NLS-1$
		        String[] domainNames = domainNameOrder.split(","); //$NON-NLS-1$
		        for (String domainName : domainNames) {
		        	engine.addSecurityDomain(domainName);
		            serviceBuilder.addDependency(ServiceName.JBOSS.append("security", "security-domain", domainName), SecurityDomainContext.class, new ConcurrentMapInjector<String,SecurityDomainContext>(engine.securityDomains, domainName)); //$NON-NLS-1$ //$NON-NLS-2$
		        }
	        }
        }
        
        serviceBuilder.addListener(new AbstractServiceListener<Object>() {
            @Override
            public void transition(ServiceController<?> serviceController, ServiceController.Transition transition) {
            	
            	if (transition.equals(ServiceController.Transition.START_INITIATING_to_STARTING)) {
            		vdbRepo.start();
            		bufferManager.start();            		
            	}
            	
            	if (transition.equals(ServiceController.Transition.STOPPING_to_DOWN)) {
            		bufferManager.stop();
            	}
            }
            
            @Override
            public void serviceRemoveRequested(ServiceController<?> serviceController) {
                serviceController.removeListener(this);
            }                     
        });
                  
        serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        ServiceController<ClientServiceRegistry> controller = serviceBuilder.install(); 
        newControllers.add(controller);
        ServiceContainer container =  controller.getServiceContainer();
        container.addTerminateListener(shutdownListener);    	
    	
        context.addStep(new AbstractDeploymentChainStep() {
			@Override
			public void execute(DeploymentProcessorTarget processorTarget) {
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0x0001,new VDBStructure());
				processorTarget.addDeploymentProcessor(Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer(vdbRepo, serializer));
				processorTarget.addDeploymentProcessor(Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyProcessor());
				processorTarget.addDeploymentProcessor(Phase.INSTALL, Phase.INSTALL_WAR_DEPLOYMENT|0x0001, new VDBDeployer(vdbRepo, engine.threadPoolInjector, translatorRepo, serializer, shutdownListener));            			
			}
        	
        }, OperationContext.Stage.RUNTIME);
    }

	
	private RuntimeEngineDeployer buildQueryEngine(ModelNode node) {
		RuntimeEngineDeployer engine = new RuntimeEngineDeployer();
		
    	if (node.hasDefined(Configuration.MAX_THREADS)) {
    		engine.setMaxThreads(node.get(Configuration.MAX_THREADS).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_ACTIVE_PLANS)) {
    		engine.setMaxActivePlans(node.get(Configuration.MAX_ACTIVE_PLANS).asInt());
    	}
    	if (node.hasDefined(Configuration.USER_REQUEST_SOURCE_CONCURRENCY)) {
    		engine.setUserRequestSourceConcurrency(node.get(Configuration.USER_REQUEST_SOURCE_CONCURRENCY).asInt());
    	}	
    	if (node.hasDefined(Configuration.TIME_SLICE_IN_MILLI)) {
    		engine.setTimeSliceInMilli(node.get(Configuration.TIME_SLICE_IN_MILLI).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_ROWS_FETCH_SIZE)) {
    		engine.setMaxRowsFetchSize(node.get(Configuration.MAX_ROWS_FETCH_SIZE).asInt());
    	}
    	if (node.hasDefined(Configuration.LOB_CHUNK_SIZE_IN_KB)) {
    		engine.setLobChunkSizeInKB(node.get(Configuration.LOB_CHUNK_SIZE_IN_KB).asInt());
    	}
    	if (node.hasDefined(Configuration.USE_DATA_ROLES)) {
    		engine.setUseDataRoles(node.get(Configuration.USE_DATA_ROLES).asBoolean());
    	}
    	if (node.hasDefined(Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT)) {
    		engine.setAllowCreateTemporaryTablesByDefault(node.get(Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT).asBoolean());
    	}
    	if (node.hasDefined(Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT)) {
    		engine.setAllowFunctionCallsByDefault(node.get(Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT).asBoolean());
    	}
    	if (node.hasDefined(Configuration.QUERY_THRESHOLD_IN_SECS)) {
    		engine.setQueryThresholdInSecs(node.get(Configuration.QUERY_THRESHOLD_IN_SECS).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_SOURCE_ROWS)) {
    		engine.setMaxSourceRows(node.get(Configuration.MAX_SOURCE_ROWS).asInt());
    	}
    	if (node.hasDefined(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS)) {
    		engine.setExceptionOnMaxSourceRows(node.get(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS).asBoolean());
    	}
    	if (node.hasDefined(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED)) {
    		engine.setMaxODBCLobSizeAllowed(node.get(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED).asInt());
    	}
    	if (node.hasDefined(Configuration.EVENT_DISTRIBUTOR_NAME)) {
    		engine.setEventDistributorName(node.get(Configuration.EVENT_DISTRIBUTOR_NAME).asString());
    	}
    	if (node.hasDefined(Configuration.DETECTING_CHANGE_EVENTS)) {
    		engine.setDetectingChangeEvents(node.get(Configuration.DETECTING_CHANGE_EVENTS).asBoolean());
    	}	             
    	if (node.hasDefined(Configuration.SESSION_EXPIRATION_TIME_LIMIT)) {
    		engine.setSessionExpirationTimeLimit(node.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_SESSIONS_ALLOWED)) {
    		engine.setSessionMaxLimit(node.get(Configuration.MAX_SESSIONS_ALLOWED).asInt());
    	}		                	
    	
		return engine;
	}


    private VDBRepository buildVDBRepository(ModelNode node) {
    	SystemFunctionManager systemFunctionManager = new SystemFunctionManager();
    	if (node.hasDefined(Configuration.ALLOW_ENV_FUNCTION)) {
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
    	
    	if (node.hasDefined(Configuration.USE_DISK)) {
    		bufferManger.setUseDisk(node.get(Configuration.USE_DISK).asBoolean());
    	}	                	
    	if (node.hasDefined(Configuration.PROCESSOR_BATCH_SIZE)) {
    		bufferManger.setProcessorBatchSize(node.get(Configuration.PROCESSOR_BATCH_SIZE).asInt());
    	}	
    	if (node.hasDefined(Configuration.CONNECTOR_BATCH_SIZE)) {
    		bufferManger.setConnectorBatchSize(node.get(Configuration.CONNECTOR_BATCH_SIZE).asInt());
    	}	
    	if (node.hasDefined(Configuration.MAX_RESERVE_BATCH_COLUMNS)) {
    		bufferManger.setMaxReserveBatchColumns(node.get(Configuration.MAX_RESERVE_BATCH_COLUMNS).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_PROCESSING_BATCH_COLUMNS)) {
    		bufferManger.setMaxProcessingBatchesColumns(node.get(Configuration.MAX_PROCESSING_BATCH_COLUMNS).asInt());
    	}	
    	if (node.hasDefined(Configuration.MAX_FILE_SIZE)) {
    		bufferManger.setMaxFileSize(node.get(Configuration.MAX_FILE_SIZE).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_BUFFER_SPACE)) {
    		bufferManger.setMaxBufferSpace(node.get(Configuration.MAX_BUFFER_SPACE).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_OPEN_FILES)) {
    		bufferManger.setMaxOpenFiles(node.get(Configuration.MAX_OPEN_FILES).asInt());
    	}	                	
    	return bufferManger;
    }
    
    private CacheFactory getCacheFactory(ModelNode node) {
    	CacheFactory cacheFactory = new DefaultCacheFactory();	
    	/*
    	if (node.hasDefined(Configuration.CLASS)) {
    		String className = node.get(Configuration.CLASS).asString();
    	}
    	
    	if (node.hasDefined(Configuration.ENABLED)) {
    		cacheFactory.setEnabled(node.get(Configuration.ENABLED).asBoolean());
    	}
    	else {
    		cacheFactory.setEnabled(true);
    	}
    	if (node.hasDefined(Configuration.CACHE_SERVICE_JNDI_NAME)) {
    		cacheFactory.setCacheManager(node.get(Configuration.CACHE_SERVICE_JNDI_NAME).asString());
    	}	                	
    	if (node.hasDefined(Configuration.RESULTSET_CACHE_NAME)) {
    		cacheFactory.setResultsetCacheName(node.get(Configuration.RESULTSET_CACHE_NAME).asString());
    	}
    	*/		                	
    	return cacheFactory;
    }
    
    private CacheConfiguration buildCacheConfig(ModelNode node) {
    	CacheConfiguration cacheConfig = new CacheConfiguration();
    	
    	if (node.hasDefined(Configuration.MAX_ENTRIES)) {
    		cacheConfig.setMaxEntries(node.get(Configuration.MAX_ENTRIES).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_AGE_IN_SECS)) {
    		cacheConfig.setMaxAgeInSeconds(node.get(Configuration.MAX_AGE_IN_SECS).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_STALENESS)) {
    		cacheConfig.setMaxStaleness(node.get(Configuration.MAX_STALENESS).asInt());
    	}
    	if (node.hasDefined(Configuration.CACHE_TYPE)) {
    		cacheConfig.setType(node.get(Configuration.CACHE_TYPE).asString());
    	}
    	if (node.hasDefined(Configuration.CACHE_LOCATION)) {
    		cacheConfig.setLocation(node.get(Configuration.CACHE_LOCATION).asString());
    	}	   	                	
    	return cacheConfig;
	}	                
    
	private SocketConfiguration buildSocketConfiguration(ModelNode node) {
		SocketConfiguration socket = new SocketConfiguration();
		
		if (node.hasDefined(Configuration.SOCKET_BINDING)) {
			socket.setSocketBinding(node.require(Configuration.SOCKET_BINDING).asString());
		}
		else {
			throw new IllegalArgumentException(IntegrationPlugin.Util.getString(Configuration.SOCKET_BINDING+".not_defined")); //$NON-NLS-1$
		}

   		if (node.hasDefined(Configuration.MAX_SOCKET_THREAD_SIZE)) {
    		socket.setMaxSocketThreads(node.get(Configuration.MAX_SOCKET_THREAD_SIZE).asInt());
    	}
    	if (node.hasDefined(Configuration.IN_BUFFER_SIZE)) {
    		socket.setInputBufferSize(node.get(Configuration.IN_BUFFER_SIZE).asInt());
    	}	
    	if (node.hasDefined(Configuration.OUT_BUFFER_SIZE)) {
    		socket.setOutputBufferSize(node.get(Configuration.OUT_BUFFER_SIZE).asInt());
    	}		   
    	
    	SSLConfiguration ssl = new SSLConfiguration();
    	ssl.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
    	
    	if (node.hasDefined(Configuration.SSL)) {
    		ModelNode sslNode = node.get(Configuration.SSL);
    		
        	if (sslNode.hasDefined(Configuration.SSL_MODE)) {
        		ssl.setMode(sslNode.get(Configuration.SSL_MODE).asString());
        	}
        	
        	if (sslNode.hasDefined(Configuration.KEY_STORE_FILE)) {
        		ssl.setKeystoreFilename(sslNode.get(Configuration.KEY_STORE_FILE).asString());
        	}	
        	
        	if (sslNode.hasDefined(Configuration.KEY_STORE_PASSWD)) {
        		ssl.setKeystorePassword(sslNode.get(Configuration.KEY_STORE_PASSWD).asString());
        	}	
        	
        	if (sslNode.hasDefined(Configuration.KEY_STORE_TYPE)) {
        		ssl.setKeystoreType(sslNode.get(Configuration.KEY_STORE_TYPE).asString());
        	}		
        	
        	if (sslNode.hasDefined(Configuration.SSL_PROTOCOL)) {
        		ssl.setSslProtocol(sslNode.get(Configuration.SSL_PROTOCOL).asString());
        	}	
        	if (sslNode.hasDefined(Configuration.KEY_MANAGEMENT_ALG)) {
        		ssl.setKeymanagementAlgorithm(sslNode.get(Configuration.KEY_MANAGEMENT_ALG).asString());
        	}
        	if (sslNode.hasDefined(Configuration.TRUST_FILE)) {
        		ssl.setTruststoreFilename(sslNode.get(Configuration.TRUST_FILE).asString());
        	}
        	if (sslNode.hasDefined(Configuration.TRUST_PASSWD)) {
        		ssl.setTruststorePassword(sslNode.get(Configuration.TRUST_PASSWD).asString());
        	}
        	if (sslNode.hasDefined(Configuration.AUTH_MODE)) {
        		ssl.setAuthenticationMode(sslNode.get(Configuration.AUTH_MODE).asString());
        	}
    	}
    	socket.setSSLConfiguration(ssl);
    	
		return socket;
	}	
}
