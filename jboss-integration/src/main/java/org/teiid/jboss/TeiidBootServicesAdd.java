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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUEST_PROPERTIES;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.ServiceLoader;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.as.clustering.jgroups.ChannelFactory;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.deployment.Phase;
import org.jboss.as.server.services.path.RelativePathService;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.service.ServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.InjectedValue;
import org.teiid.PolicyDecider;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.jboss.ClusterableCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.SystemVDBDeployer;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DataRolePolicyDecider;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.replication.jboss.JGroupsObjectReplicator;
import org.teiid.services.BufferServiceImpl;

class TeiidBootServicesAdd extends AbstractAddStepHandler implements DescriptionProvider {

	private static Element[] attributes = {
		Element.ALLOW_ENV_FUNCTION_ELEMENT,
		Element.ASYNC_THREAD_POOL_ELEMENT,
		Element.MAX_THREADS_ELEMENT,
		Element.MAX_ACTIVE_PLANS_ELEMENT,
		Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, 
		Element.TIME_SLICE_IN_MILLI_ELEMENT, 
		Element.MAX_ROWS_FETCH_SIZE_ELEMENT,
		Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT,
		Element.QUERY_THRESHOLD_IN_SECS_ELEMENT,
		Element.MAX_SOURCE_ROWS_ELEMENT,
		Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, 
		Element.DETECTING_CHANGE_EVENTS_ELEMENT,
		Element.AUTHORIZATION_VALIDATOR_MODULE_ATTRIBUTE,
		Element.POLICY_DECIDER_MODULE_ATTRIBUTE,
		
		// object replicator
		Element.OR_STACK_ATTRIBUTE,
		Element.OR_CLUSTER_NAME_ATTRIBUTE,

		// Buffer Service
		Element.USE_DISK_ATTRIBUTE,
		Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE,
		Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE,
		Element.MAX_PROCESSING_KB_ATTRIBUTE,
		Element.MAX_RESERVED_KB_ATTRIBUTE,
		Element.MAX_FILE_SIZE_ATTRIBUTE,
		Element.MAX_BUFFER_SPACE_ATTRIBUTE,
		Element.MAX_OPEN_FILES_ATTRIBUTE,
		
		// prepared plan cache
		Element.PPC_MAX_ENTRIES_ATTRIBUTE,
		Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE,
		Element.PPC_MAX_STALENESS_ATTRIBUTE,
		
		// resultset cache
		Element.RSC_NAME_ELEMENT,
		Element.RSC_CONTAINER_NAME_ELEMENT,
		Element.RSC_MAX_STALENESS_ELEMENT,
		Element.RSC_ENABLE_ATTRIBUTE
	};
	
	@Override
	public ModelNode getModelDescription(Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode node = new ModelNode();
        node.get(OPERATION_NAME).set(ADD);
        node.get(DESCRIPTION).set(bundle.getString("teiid.add")); //$NON-NLS-1$
        
        describeTeiid(node, REQUEST_PROPERTIES,  bundle);
		
        return node;
	}

	static void describeTeiid(final ModelNode node, String type, final ResourceBundle bundle) {
		for (int i = 0; i < attributes.length; i++) {
			attributes[i].describe(node, type, bundle);
		}	
	}
	
	@Override
	protected void populateModel(ModelNode operation, ModelNode model)	throws OperationFailedException {
		populate(operation, model);
	}

	static void populate(ModelNode operation, ModelNode model) {
		for (int i = 0; i < attributes.length; i++) {
			attributes[i].populate(operation, model);
		}
	}
	

	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {
		
		ServiceTarget target = context.getServiceTarget();
		
		final JBossLifeCycleListener shutdownListener = new JBossLifeCycleListener();
		
		final String asyncThreadPoolName = Element.ASYNC_THREAD_POOL_ELEMENT.asString(operation); 
		
		// translator repository
    	final TranslatorRepository translatorRepo = new TranslatorRepository();
    	ValueService<TranslatorRepository> translatorService = new ValueService<TranslatorRepository>(new org.jboss.msc.value.Value<TranslatorRepository>() {
			@Override
			public TranslatorRepository getValue() throws IllegalStateException, IllegalArgumentException {
				return translatorRepo;
			}
    	});
    	ServiceController<TranslatorRepository> service = target.addService(TeiidServiceNames.TRANSLATOR_REPO, translatorService).install();
    	newControllers.add(service);
    	
    	// system function tree
		SystemFunctionManager systemFunctionManager = new SystemFunctionManager();
		if (Element.ALLOW_ENV_FUNCTION_ELEMENT.isDefined(operation)) {
			systemFunctionManager.setAllowEnvFunction(Element.ALLOW_ENV_FUNCTION_ELEMENT.asBoolean(operation));
		}
		else {
			systemFunctionManager.setAllowEnvFunction(false);
		}
		systemFunctionManager.setClassloader(Module.getCallerModule().getClassLoader()); 
    	
    	// VDB repository
    	final VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemFunctionManager(systemFunctionManager);
    	VDBRepositoryService vdbRepositoryService = new VDBRepositoryService(vdbRepository);
    	newControllers.add(target.addService(TeiidServiceNames.VDB_REPO, vdbRepositoryService).install());
		
    	// System VDB Service
    	SystemVDBDeployer systemVDB = new SystemVDBDeployer();
    	systemVDB.setVDBRepository(vdbRepository);
    	SystemVDBService systemVDBService = new SystemVDBService(systemVDB);
    	newControllers.add(target.addService(TeiidServiceNames.SYSTEM_VDB, systemVDBService).install());
    	
    	newControllers.add(RelativePathService.addService(TeiidServiceNames.DATA_DIR, "teiid-data", "jboss.server.data.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	final ObjectsSerializerService serializer = new ObjectsSerializerService();
    	ServiceBuilder<ObjectSerializer> objectSerializerService = target.addService(TeiidServiceNames.OBJECT_SERIALIZER, serializer);
    	objectSerializerService.addDependency(TeiidServiceNames.DATA_DIR, String.class, serializer.getPathInjector());
    	newControllers.add(objectSerializerService.install());

    	// TODO: remove verbose service by moving the buffer service from runtime project
    	newControllers.add(RelativePathService.addService(TeiidServiceNames.BUFFER_DIR, "teiid-buffer", "jboss.server.temp.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	final BufferServiceImpl bufferManager = buildBufferManager(operation);
    	BufferManagerService bufferService = new BufferManagerService(bufferManager);
    	ServiceBuilder<BufferServiceImpl> bufferServiceBuilder = target.addService(TeiidServiceNames.BUFFER_MGR, bufferService);
    	bufferServiceBuilder.addDependency(TeiidServiceNames.BUFFER_DIR, String.class, bufferService.pathInjector);
    	newControllers.add(bufferServiceBuilder.install());
    	
    	PolicyDecider policyDecider;
    	if (Element.POLICY_DECIDER_MODULE_ATTRIBUTE.isDefined(operation)) {
    		policyDecider = buildService(PolicyDecider.class, Element.POLICY_DECIDER_MODULE_ATTRIBUTE.asString(operation));    		
    	}
    	else {
    		DataRolePolicyDecider drpd = new DataRolePolicyDecider();
    		drpd.setAllowCreateTemporaryTablesByDefault(true);
    		drpd.setAllowFunctionCallsByDefault(true);
    		policyDecider = drpd;
    	}
    	
    	final AuthorizationValidator authValidator;
    	if (Element.AUTHORIZATION_VALIDATOR_MODULE_ATTRIBUTE.isDefined(operation)) {
    		authValidator = buildService(AuthorizationValidator.class, Element.AUTHORIZATION_VALIDATOR_MODULE_ATTRIBUTE.asString(operation));
    		authValidator.setEnabled(true);
    	}
    	else {
    		DefaultAuthorizationValidator dap = new DefaultAuthorizationValidator();
    		dap.setPolicyDecider(policyDecider);
    		dap.setEnabled(true);
    		authValidator = dap;
    	}
    	
    	ValueService<AuthorizationValidator> authValidatorService = new ValueService<AuthorizationValidator>(new org.jboss.msc.value.Value<AuthorizationValidator>() {
			@Override
			public AuthorizationValidator getValue() throws IllegalStateException, IllegalArgumentException {
				return authValidator;
			}
    	});    	
    	newControllers.add(target.addService(TeiidServiceNames.AUTHORIZATION_VALIDATOR, authValidatorService).install());
    	
    	// resultset cache
    	final SessionAwareCache<CachedResults> resultsetCache = buildResultsetCache(operation, bufferManager.getBufferManager());
    	ValueService<SessionAwareCache<CachedResults>> resultSetService = new ValueService<SessionAwareCache<CachedResults>>(new org.jboss.msc.value.Value<SessionAwareCache<CachedResults>>() {
			@Override
			public SessionAwareCache<CachedResults> getValue() throws IllegalStateException, IllegalArgumentException {
				return resultsetCache;
			}
    	});
    	newControllers.add(target.addService(TeiidServiceNames.CACHE_RESULTSET, resultSetService).install());
    	
    	// prepared-plan cache
    	final SessionAwareCache<PreparedPlan> preparedPlanCache = buildPreparedPlanCache(operation, bufferManager.getBufferManager());
    	ValueService<SessionAwareCache<PreparedPlan>> preparedPlanService = new ValueService<SessionAwareCache<PreparedPlan>>(new org.jboss.msc.value.Value<SessionAwareCache<PreparedPlan>>() {
			@Override
			public SessionAwareCache<PreparedPlan> getValue() throws IllegalStateException, IllegalArgumentException {
				return preparedPlanCache;
			}
    	});
    	newControllers.add(target.addService(TeiidServiceNames.CACHE_PREPAREDPLAN, preparedPlanService).install());
    	
    	// Object Replicator
    	if (Element.OR_STACK_ATTRIBUTE.isDefined(operation)) {
    		String stack = Element.OR_STACK_ATTRIBUTE.asString(operation);
    		
    		String clusterName = "teiid-rep"; //$NON-NLS-1$ 
    		if (Element.OR_CLUSTER_NAME_ATTRIBUTE.isDefined(operation)) {
    			clusterName = Element.OR_CLUSTER_NAME_ATTRIBUTE.asString(operation);
    		}
    		
    		JGroupsObjectReplicatorService replicatorService = new JGroupsObjectReplicatorService(clusterName);
    		replicatorService.setBufferManager(bufferManager.getBufferManager());
			ServiceBuilder<JGroupsObjectReplicator> serviceBuilder = target.addService(TeiidServiceNames.OBJECT_REPLICATOR, replicatorService);
			serviceBuilder.addDependency(ServiceName.JBOSS.append("jgroups", stack), ChannelFactory.class, replicatorService.channelFactoryInjector); //$NON-NLS-1$
			newControllers.add(serviceBuilder.install());
    	}
    	
    	// Query Engine
    	final RuntimeEngineDeployer engine = buildQueryEngine(operation);
    	
        ServiceBuilder<DQPCore> engineBuilder = target.addService(TeiidServiceNames.ENGINE, engine);
        engineBuilder.addDependency(ServiceName.JBOSS.append("connector", "workmanager"), WorkManager.class, engine.getWorkManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "XATerminator"), XATerminator.class, engine.getXaTerminatorInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "TransactionManager"), TransactionManager.class, engine.getTxnManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferServiceImpl.class, engine.getBufferServiceInjector());
        engineBuilder.addDependency(TeiidServiceNames.SYSTEM_VDB, SystemVDBDeployer.class,  new InjectedValue<SystemVDBDeployer>());
        engineBuilder.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class, engine.getTranslatorRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, engine.getVdbRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.AUTHORIZATION_VALIDATOR, AuthorizationValidator.class, engine.getAuthorizationValidatorInjector());
        engineBuilder.addDependency(TeiidServiceNames.CACHE_RESULTSET, SessionAwareCache.class, engine.getResultSetCacheInjector());
        engineBuilder.addDependency(TeiidServiceNames.CACHE_PREPAREDPLAN, SessionAwareCache.class, engine.getPreparedPlanCacheInjector());
        engineBuilder.addDependency(DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, engine.getObjectReplicatorInjector());
        
        engineBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        ServiceController<DQPCore> controller = engineBuilder.install(); 
        newControllers.add(controller);
        ServiceContainer container =  controller.getServiceContainer();
        container.addTerminateListener(shutdownListener);
            	
    	// Register VDB deployer
        context.addStep(new AbstractDeploymentChainStep() {
			@Override
			public void execute(DeploymentProcessorTarget processorTarget) {
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT,new DynamicVDBRootMountDeployer());
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0x0001,new VDBStructureDeployer());
				processorTarget.addDeploymentProcessor(Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer());
				processorTarget.addDeploymentProcessor(Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyDeployer());
				processorTarget.addDeploymentProcessor(Phase.INSTALL, Phase.INSTALL_WAR_DEPLOYMENT|0x0001, new VDBDeployer(translatorRepo, asyncThreadPoolName));            			
			}
        	
        }, OperationContext.Stage.RUNTIME);    	
	}
	
    private <T> T buildService(Class<T> type, String moduleName) throws OperationFailedException {
        final ModuleIdentifier moduleId;
        final Module module;
        try {
            moduleId = ModuleIdentifier.create(moduleName);
            module = Module.getCallerModuleLoader().loadModule(moduleId);
        } catch (ModuleLoadException e) {
            throw new OperationFailedException(e, new ModelNode().set(IntegrationPlugin.Util.getString("failed_load_module", moduleName))); //$NON-NLS-1$
        }
        ServiceLoader<T> services = module.loadService(type);
        return services.iterator().next();
    }
	
		
    private BufferServiceImpl  buildBufferManager(ModelNode node) {
    	BufferServiceImpl bufferManger = new BufferServiceImpl();
    	
    	if (node == null) {
    		return bufferManger;
    	}
    	
    	if (Element.USE_DISK_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setUseDisk(Element.USE_DISK_ATTRIBUTE.asBoolean(node));
    	}	                	
    	if (Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setProcessorBatchSize(Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE.asInt(node));
    	}	
    	if (Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setConnectorBatchSize(Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE.asInt(node));
    	}	
    	if (Element.MAX_PROCESSING_KB_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxProcessingKb(Element.MAX_PROCESSING_KB_ATTRIBUTE.asInt(node));
    	}
    	if (Element.MAX_RESERVED_KB_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxReserveKb(Element.MAX_RESERVED_KB_ATTRIBUTE.asInt(node));
    	}
    	if (Element.MAX_FILE_SIZE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxFileSize(Element.MAX_FILE_SIZE_ATTRIBUTE.asLong(node));
    	}
    	if (Element.MAX_BUFFER_SPACE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxBufferSpace(Element.MAX_BUFFER_SPACE_ATTRIBUTE.asLong(node));
    	}
    	if (Element.MAX_OPEN_FILES_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxOpenFiles(Element.MAX_OPEN_FILES_ATTRIBUTE.asInt(node));
    	}	                	
    	return bufferManger;
    }	

    private SessionAwareCache<CachedResults> buildResultsetCache(ModelNode node, BufferManager bufferManager) {

    	CacheConfiguration cacheConfig = new CacheConfiguration();
    	// these settings are not really used; they are defined by infinispan
    	cacheConfig.setMaxEntries(1024);
   		cacheConfig.setMaxAgeInSeconds(7200);
   		cacheConfig.setType(Policy.EXPIRATION.name());
    	cacheConfig.setLocation("resultset"); //$NON-NLS-1$
    	cacheConfig.setMaxStaleness(60);
    	
    	if (Element.RSC_ENABLE_ATTRIBUTE.isDefined(node)) {
    		if (!Element.RSC_ENABLE_ATTRIBUTE.asBoolean(node)) {
    			return null;
    		}
    	}    	
    	
    	ClusterableCacheFactory cacheFactory = null;

    	if (Element.RSC_CONTAINER_NAME_ELEMENT.isDefined(node)) {
    		cacheFactory = new ClusterableCacheFactory();
    		cacheFactory.setCacheManager(Element.RSC_CONTAINER_NAME_ELEMENT.asString(node));
    	}
    	else {
    		SessionAwareCache<CachedResults> resultsetCache = new SessionAwareCache<CachedResults>(new DefaultCacheFactory(), SessionAwareCache.Type.RESULTSET, cacheConfig);
        	resultsetCache.setBufferManager(bufferManager);
        	return resultsetCache;    		
    	}
    	
    	if (Element.RSC_NAME_ELEMENT.isDefined(node)) {
    		cacheFactory.setResultsetCacheName(Element.RSC_NAME_ELEMENT.asString(node));
    	}	 
    	else {
    		cacheFactory.setResultsetCacheName("resultset"); //$NON-NLS-1$
    	}

   		if (Element.RSC_MAX_STALENESS_ELEMENT.isDefined(node)) {
    		cacheConfig.setMaxStaleness(Element.RSC_MAX_STALENESS_ELEMENT.asInt(node));
    	}

   		SessionAwareCache<CachedResults> resultsetCache = new SessionAwareCache<CachedResults>(cacheFactory, SessionAwareCache.Type.RESULTSET, cacheConfig);
    	resultsetCache.setBufferManager(bufferManager);
    	return resultsetCache;
	}	      
    
    
    private SessionAwareCache<PreparedPlan> buildPreparedPlanCache(ModelNode node, BufferManager bufferManager) {
    	CacheConfiguration cacheConfig = new CacheConfiguration();
    	if (Element.PPC_MAX_ENTRIES_ATTRIBUTE.isDefined(node)) {
    		cacheConfig.setMaxEntries(Element.PPC_MAX_ENTRIES_ATTRIBUTE.asInt(node));
    	}
    	else {
    		cacheConfig.setMaxEntries(512);
    	}
    	
    	if (Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE.isDefined(node)) {
    		cacheConfig.setMaxAgeInSeconds(Element.PPC_MAX_AGE_IN_SECS_ATTRIBUTE.asInt(node));
    	}
    	else {
    		cacheConfig.setMaxAgeInSeconds(28800);
    	}
    	
    	if (Element.PPC_MAX_STALENESS_ATTRIBUTE.isDefined(node)) {
    		cacheConfig.setMaxStaleness(Element.PPC_MAX_STALENESS_ATTRIBUTE.asInt(node));
    	}
    	else {
    		cacheConfig.setMaxStaleness(0);
    	}
		cacheConfig.setType(Policy.LRU.name());
    	
    	cacheConfig.setLocation("prepared"); //$NON-NLS-1$
    	SessionAwareCache<PreparedPlan> cache = new SessionAwareCache<PreparedPlan>(new DefaultCacheFactory(), SessionAwareCache.Type.PREPAREDPLAN, cacheConfig);
    	cache.setBufferManager(bufferManager);
    	
    	return cache;
	}	    
    
    
	private RuntimeEngineDeployer buildQueryEngine(ModelNode node) {
		RuntimeEngineDeployer engine = new RuntimeEngineDeployer();
    	
    	if (Element.MAX_THREADS_ELEMENT.isDefined(node)) {
    		engine.setMaxThreads(Element.MAX_THREADS_ELEMENT.asInt(node));
    	}
    	if (Element.MAX_ACTIVE_PLANS_ELEMENT.isDefined(node)) {
    		engine.setMaxActivePlans(Element.MAX_ACTIVE_PLANS_ELEMENT.asInt(node));
    	}
    	if (Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.isDefined(node)) {
    		engine.setUserRequestSourceConcurrency(Element.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.asInt(node));
    	}	
    	if (Element.TIME_SLICE_IN_MILLI_ELEMENT.isDefined(node)) {
    		engine.setTimeSliceInMilli(Element.TIME_SLICE_IN_MILLI_ELEMENT.asInt(node));
    	}
    	if (Element.MAX_ROWS_FETCH_SIZE_ELEMENT.isDefined(node)) {
    		engine.setMaxRowsFetchSize(Element.MAX_ROWS_FETCH_SIZE_ELEMENT.asInt(node));
    	}
    	if (Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT.isDefined(node)) {
    		engine.setLobChunkSizeInKB(Element.LOB_CHUNK_SIZE_IN_KB_ELEMENT.asInt(node));
    	}
    	if (Element.QUERY_THRESHOLD_IN_SECS_ELEMENT.isDefined(node)) {
    		engine.setQueryThresholdInSecs(Element.QUERY_THRESHOLD_IN_SECS_ELEMENT.asInt(node));
    	}
    	if (Element.MAX_SOURCE_ROWS_ELEMENT.isDefined(node)) {
    		engine.setMaxSourceRows(Element.MAX_SOURCE_ROWS_ELEMENT.asInt(node));
    	}
    	if (Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.isDefined(node)) {
    		engine.setExceptionOnMaxSourceRows(Element.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.asBoolean(node));
    	}
    	if (Element.DETECTING_CHANGE_EVENTS_ELEMENT.isDefined(node)) {
    		engine.setDetectingChangeEvents(Element.DETECTING_CHANGE_EVENTS_ELEMENT.asBoolean(node));
    	}	             
		return engine;
	}    
}
