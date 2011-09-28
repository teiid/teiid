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
import static org.teiid.jboss.Configuration.DESC;
import static org.teiid.jboss.Configuration.addAttribute;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.ServiceLoader;

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
import org.jboss.dmr.ModelType;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.*;
import org.teiid.PolicyDecider;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.jboss.ClusterableCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.SystemVDBDeployer;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.*;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.replication.jboss.JGroupsObjectReplicator;
import org.teiid.services.BufferServiceImpl;

class TeiidBootServicesAdd extends AbstractAddStepHandler implements DescriptionProvider {

	static final String DASH = "-"; //$NON-NLS-1$

	@Override
	public ModelNode getModelDescription(Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode node = new ModelNode();
        node.get(OPERATION_NAME).set(ADD);
        node.get(DESCRIPTION).set(bundle.getString("teiid-boot.add")); //$NON-NLS-1$
        
        describeTeiidRoot(bundle, REQUEST_PROPERTIES,  node);
		
        return node;
	}

	static void describeTeiidRoot(final ResourceBundle bundle, String type, final ModelNode node) {
		addAttribute(node, Configuration.ALLOW_ENV_FUNCTION, type, bundle.getString(Configuration.ALLOW_ENV_FUNCTION+DESC), ModelType.BOOLEAN, false, "false"); //$NON-NLS-1$
        addAttribute(node, Configuration.ASYNC_THREAD_GROUP, type, bundle.getString(Configuration.ASYNC_THREAD_GROUP+DESC), ModelType.STRING, true, null); 

        addAttribute(node, Configuration.AUTHORIZATION_VALIDATOR_MODULE, type, bundle.getString(Configuration.AUTHORIZATION_VALIDATOR_MODULE+DESC), ModelType.BOOLEAN, false, null);
        addAttribute(node, Configuration.POLICY_DECIDER_MODULE, type, bundle.getString(Configuration.POLICY_DECIDER_MODULE+DESC), ModelType.STRING, false, null);
        
        describeObjectReplicator(node, type, bundle);
		describeBufferManager(node, type, bundle);		
		describePreparedPlanCache(node, type, bundle);
		describeResultsetCache(node, type, bundle);
	}

	@Override
	protected void populateModel(ModelNode operation, ModelNode model)	throws OperationFailedException {
		populate(operation, model);
	}

	static void populate(ModelNode operation, ModelNode model) {
		if (operation.hasDefined(Configuration.ALLOW_ENV_FUNCTION)) {
			model.get(Configuration.ALLOW_ENV_FUNCTION).set(operation.get(Configuration.ALLOW_ENV_FUNCTION).asString());
		}
		if (operation.hasDefined(Configuration.ASYNC_THREAD_GROUP)) {
			model.get(Configuration.ASYNC_THREAD_GROUP).set(operation.get(Configuration.ASYNC_THREAD_GROUP).asString());
		}
		
		populateBufferManager(operation, model);
		
		if (operation.hasDefined(Configuration.POLICY_DECIDER_MODULE)) {
			model.get(Configuration.POLICY_DECIDER_MODULE).set(operation.get(Configuration.POLICY_DECIDER_MODULE).asString());
		}
		
		if (operation.hasDefined(Configuration.AUTHORIZATION_VALIDATOR_MODULE)) {
			model.get(Configuration.AUTHORIZATION_VALIDATOR_MODULE).set(operation.get(Configuration.AUTHORIZATION_VALIDATOR_MODULE).asString());
		}		
		
		populateResultsetCache(operation, model);
		populatePreparedPlanCache(operation, model);
		populateObjectReplicator(operation, model);
	}
	


	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {
		ServiceTarget target = context.getServiceTarget();
		
		final String asyncThreadPoolName = operation.require(Configuration.ASYNC_THREAD_GROUP).asString(); 
		
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
		if (operation.hasDefined(Configuration.ALLOW_ENV_FUNCTION)) {
			systemFunctionManager.setAllowEnvFunction(operation.get(Configuration.ALLOW_ENV_FUNCTION).asBoolean());
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
    	if (operation.hasDefined(Configuration.POLICY_DECIDER_MODULE)) {
    		policyDecider = buildService(PolicyDecider.class, operation.get(Configuration.POLICY_DECIDER_MODULE).asString());    		
    	}
    	else {
    		DataRolePolicyDecider drpd = new DataRolePolicyDecider();
    		drpd.setAllowCreateTemporaryTablesByDefault(true);
    		drpd.setAllowFunctionCallsByDefault(true);
    		policyDecider = drpd;
    	}
    	
    	final AuthorizationValidator authValidator;
    	if (operation.hasDefined(Configuration.AUTHORIZATION_VALIDATOR_MODULE)) {
    		authValidator = buildService(AuthorizationValidator.class, operation.get(Configuration.AUTHORIZATION_VALIDATOR_MODULE).asString());
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
    	if (operation.hasDefined(ORC(Configuration.STACK))) {
    		String stack = operation.get(ORC(Configuration.STACK)).asString();
    		String clusterName = "teiid-rep"; //$NON-NLS-1$ 
    		if (operation.hasDefined(ORC(Configuration.CLUSTER_NAME))) {
    			clusterName = operation.get(ORC(Configuration.CLUSTER_NAME)).asString();
    		}
    		JGroupsObjectReplicatorService replicatorService = new JGroupsObjectReplicatorService(clusterName);
    		replicatorService.setBufferManager(bufferManager.getBufferManager());
			ServiceBuilder<JGroupsObjectReplicator> serviceBuilder = target.addService(TeiidServiceNames.OBJECT_REPLICATOR, replicatorService);
			serviceBuilder.addDependency(ServiceName.JBOSS.append("jgroups", stack), ChannelFactory.class, replicatorService.channelFactoryInjector); //$NON-NLS-1$
			newControllers.add(serviceBuilder.install());
    	}
    	
    	
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
	
    private static String BS(String name) {
    	return Configuration.BUFFER_SERVICE+DASH+name;
    }
	static void describeBufferManager(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, BS(Configuration.USE_DISK), type, bundle.getString(Configuration.USE_DISK+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.PROCESSOR_BATCH_SIZE), type, bundle.getString(Configuration.PROCESSOR_BATCH_SIZE+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.CONNECTOR_BATCH_SIZE), type, bundle.getString(Configuration.CONNECTOR_BATCH_SIZE+DESC), ModelType.INT, false, "1024"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.MAX_PROCESSING_KB), type, bundle.getString(Configuration.MAX_PROCESSING_KB+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.MAX_RESERVED_KB), type, bundle.getString(Configuration.MAX_RESERVED_KB+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.MAX_FILE_SIZE), type, bundle.getString(Configuration.MAX_FILE_SIZE+DESC), ModelType.LONG, false, "2048"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.MAX_BUFFER_SPACE), type, bundle.getString(Configuration.MAX_BUFFER_SPACE+DESC), ModelType.LONG, false, "51200"); //$NON-NLS-1$
		addAttribute(node, BS(Configuration.MAX_OPEN_FILES), type, bundle.getString(Configuration.MAX_OPEN_FILES+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
	}	
		
    private static String ORC(String name) {
    	return Configuration.OBJECT_REPLICATOR+DASH+name;
    }	
	static void describeObjectReplicator(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, ORC(Configuration.STACK), type, bundle.getString(Configuration.STACK+DESC), ModelType.STRING, false, null); 
		addAttribute(node, ORC(Configuration.CLUSTER_NAME), type, bundle.getString(Configuration.CLUSTER_NAME+DESC), ModelType.STRING, false, null);
	}	
	
    private BufferServiceImpl  buildBufferManager(ModelNode node) {
    	BufferServiceImpl bufferManger = new BufferServiceImpl();
    	
    	if (node == null) {
    		return bufferManger;
    	}
    	
    	if (node.hasDefined(BS(Configuration.USE_DISK))) {
    		bufferManger.setUseDisk(node.get(BS(Configuration.USE_DISK)).asBoolean());
    	}	                	
    	if (node.hasDefined(BS(Configuration.PROCESSOR_BATCH_SIZE))) {
    		bufferManger.setProcessorBatchSize(node.get(BS(Configuration.PROCESSOR_BATCH_SIZE)).asInt());
    	}	
    	if (node.hasDefined(BS(Configuration.CONNECTOR_BATCH_SIZE))) {
    		bufferManger.setConnectorBatchSize(node.get(BS(Configuration.CONNECTOR_BATCH_SIZE)).asInt());
    	}	
    	if (node.hasDefined(BS(Configuration.MAX_PROCESSING_KB))) {
    		bufferManger.setMaxProcessingKb(node.get(BS(Configuration.MAX_PROCESSING_KB)).asInt());
    	}
    	if (node.hasDefined(BS(Configuration.MAX_RESERVED_KB))) {
    		bufferManger.setMaxReserveKb(node.get(BS(Configuration.MAX_RESERVED_KB)).asInt());
    	}
    	if (node.hasDefined(BS(Configuration.MAX_FILE_SIZE))) {
    		bufferManger.setMaxFileSize(node.get(BS(Configuration.MAX_FILE_SIZE)).asLong());
    	}
    	if (node.hasDefined(BS(Configuration.MAX_BUFFER_SPACE))) {
    		bufferManger.setMaxBufferSpace(node.get(BS(Configuration.MAX_BUFFER_SPACE)).asLong());
    	}
    	if (node.hasDefined(BS(Configuration.MAX_OPEN_FILES))) {
    		bufferManger.setMaxOpenFiles(node.get(BS(Configuration.MAX_OPEN_FILES)).asInt());
    	}	                	
    	return bufferManger;
    }	
    
    private static void populateBufferManager(ModelNode operation, ModelNode model) {
    	
		if (operation.hasDefined(BS(Configuration.USE_DISK))) {
			model.get(BS(Configuration.USE_DISK)).set(operation.get(BS(Configuration.USE_DISK)).asString());
		}

		if (operation.hasDefined(BS(Configuration.PROCESSOR_BATCH_SIZE))) {
			model.get(BS(Configuration.PROCESSOR_BATCH_SIZE)).set(operation.get(BS(Configuration.PROCESSOR_BATCH_SIZE)).asString());
		}
		if (operation.hasDefined(BS(Configuration.CONNECTOR_BATCH_SIZE))) {
			model.get(BS(Configuration.CONNECTOR_BATCH_SIZE)).set(operation.get(BS(Configuration.CONNECTOR_BATCH_SIZE)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_PROCESSING_KB))) {
			model.get(BS(Configuration.MAX_PROCESSING_KB)).set(operation.get(BS(Configuration.MAX_PROCESSING_KB)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_RESERVED_KB))) {
			model.get(BS(Configuration.MAX_RESERVED_KB)).set(operation.get(BS(Configuration.MAX_RESERVED_KB)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_FILE_SIZE))) {
			model.get(BS(Configuration.MAX_FILE_SIZE)).set(operation.get(BS(Configuration.MAX_FILE_SIZE)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_BUFFER_SPACE))) {
			model.get(BS(Configuration.MAX_BUFFER_SPACE)).set(operation.get(BS(Configuration.MAX_BUFFER_SPACE)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_BUFFER_SPACE))) {
			model.get(BS(Configuration.MAX_BUFFER_SPACE)).set(operation.get(BS(Configuration.MAX_BUFFER_SPACE)).asString());
		}
		if (operation.hasDefined(BS(Configuration.MAX_OPEN_FILES))) {
			model.get(BS(Configuration.MAX_OPEN_FILES)).set(operation.get(BS(Configuration.MAX_OPEN_FILES)).asString());
		}
    }
    
	private static void populateResultsetCache(ModelNode operation, ModelNode model) {
		if (operation.hasDefined(RSC(Configuration.NAME))) {
			model.get(RSC(Configuration.NAME)).set(operation.get(RSC(Configuration.NAME)).asString());
		}
		
		if (operation.hasDefined(RSC(Configuration.CONTAINER_NAME))) {
			model.get(RSC(Configuration.CONTAINER_NAME)).set(operation.get(RSC(Configuration.CONTAINER_NAME)).asString());
		}

		if (operation.hasDefined(RSC(Configuration.ENABLE))) {
			model.get(RSC(Configuration.ENABLE)).set(operation.get(RSC(Configuration.ENABLE)).asBoolean());
		}

		if (operation.hasDefined(RSC(Configuration.MAX_STALENESS))) {
			model.get(RSC(Configuration.MAX_STALENESS)).set(operation.get(RSC(Configuration.MAX_STALENESS)).asInt());
		}
	}    
	
	private static void populateObjectReplicator(ModelNode operation, ModelNode model) {
		if (operation.hasDefined(ORC(Configuration.STACK))) {
			model.get(ORC(Configuration.STACK)).set(operation.get(ORC(Configuration.STACK)).asString());
		}
		
		if (operation.hasDefined(ORC(Configuration.CLUSTER_NAME))) {
			model.get(ORC(Configuration.CLUSTER_NAME)).set(operation.get(ORC(Configuration.CLUSTER_NAME)).asString());
		}
	}	
    
	private static void populatePreparedPlanCache(ModelNode operation, ModelNode model) {
		if (operation.hasDefined(PPC(Configuration.MAX_ENTRIES))) {
    		model.get(PPC(Configuration.MAX_ENTRIES)).set(operation.get(PPC(Configuration.MAX_ENTRIES)).asInt());
    	}
    	if (operation.hasDefined(PPC(Configuration.MAX_AGE_IN_SECS))) {
    		model.get(PPC(Configuration.MAX_AGE_IN_SECS)).set(operation.get(PPC(Configuration.MAX_AGE_IN_SECS)).asInt());
    	}
    	if (operation.hasDefined(PPC(Configuration.MAX_STALENESS))) {
    		model.get(PPC(Configuration.MAX_STALENESS)).set(operation.get(PPC(Configuration.MAX_STALENESS)).asInt());
    	}
	}

    private static String RSC(String name) {
    	return Configuration.RESULTSET_CACHE+DASH+name;
    }	
	private static void describeResultsetCache(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, RSC(Configuration.NAME), type, bundle.getString(RSC(Configuration.NAME)+DESC), ModelType.STRING, false, "resultset"); //$NON-NLS-1$
		addAttribute(node, RSC(Configuration.MAX_STALENESS), type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "60");//$NON-NLS-1$
		addAttribute(node, RSC(Configuration.ENABLE), type, bundle.getString(Configuration.ENABLE+DESC), ModelType.BOOLEAN, false, null);
		addAttribute(node, RSC(Configuration.CONTAINER_NAME), type, bundle.getString(Configuration.CONTAINER_NAME+DESC), ModelType.STRING, false, null);		
	}
	
    private static String PPC(String name) {
    	return Configuration.PREPAREDPLAN_CACHE+DASH+name;
    }		
	private static void describePreparedPlanCache(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, PPC(Configuration.MAX_ENTRIES), type, bundle.getString(Configuration.MAX_ENTRIES+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, PPC(Configuration.MAX_AGE_IN_SECS), type, bundle.getString(Configuration.MAX_AGE_IN_SECS+DESC), ModelType.INT, false, "28800");//$NON-NLS-1$
		addAttribute(node, PPC(Configuration.MAX_STALENESS), type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "0");//$NON-NLS-1$
	}
    
    private SessionAwareCache<CachedResults> buildResultsetCache(ModelNode operation, BufferManager bufferManager) {

    	CacheConfiguration cacheConfig = new CacheConfiguration();
    	// these settings are not really used; they are defined by infinispan
    	cacheConfig.setMaxEntries(1024);
   		cacheConfig.setMaxAgeInSeconds(7200);
   		cacheConfig.setType(Policy.EXPIRATION.name());
    	cacheConfig.setLocation("resultset"); //$NON-NLS-1$
    	cacheConfig.setMaxStaleness(60);
    	
    	if (operation.hasDefined(RSC(Configuration.ENABLE))) {
    		if (!operation.get(RSC(Configuration.ENABLE)).asBoolean()) {
    			return null;
    		}
    	}    	
    	
    	ClusterableCacheFactory cacheFactory = null;

    	if (operation.hasDefined(RSC(Configuration.CONTAINER_NAME))) {
    		cacheFactory = new ClusterableCacheFactory();
    		cacheFactory.setCacheManager(operation.get(RSC(Configuration.CONTAINER_NAME)).asString());
    	}
    	else {
    		SessionAwareCache<CachedResults> resultsetCache = new SessionAwareCache<CachedResults>(new DefaultCacheFactory(), SessionAwareCache.Type.RESULTSET, cacheConfig);
        	resultsetCache.setBufferManager(bufferManager);
        	return resultsetCache;    		
    	}
    	
    	if (operation.hasDefined(RSC(Configuration.NAME))) {
    		cacheFactory.setResultsetCacheName(operation.get(RSC(Configuration.NAME)).asString());
    	}	 
    	else {
    		cacheFactory.setResultsetCacheName("resultset"); //$NON-NLS-1$
    	}

   		if (operation.hasDefined(RSC(Configuration.MAX_STALENESS))) {
    		cacheConfig.setMaxStaleness(operation.get(RSC(Configuration.MAX_STALENESS)).asInt());
    	}

   		SessionAwareCache<CachedResults> resultsetCache = new SessionAwareCache<CachedResults>(cacheFactory, SessionAwareCache.Type.RESULTSET, cacheConfig);
    	resultsetCache.setBufferManager(bufferManager);
    	return resultsetCache;
	}	      
    
    
    private SessionAwareCache<PreparedPlan> buildPreparedPlanCache(ModelNode node, BufferManager bufferManager) {
    	CacheConfiguration cacheConfig = new CacheConfiguration();
    	if (node.hasDefined(PPC(Configuration.MAX_ENTRIES))) {
    		cacheConfig.setMaxEntries(node.get(PPC(Configuration.MAX_ENTRIES)).asInt());
    	}
    	else {
    		cacheConfig.setMaxEntries(512);
    	}
    	
    	if (node.hasDefined(PPC(Configuration.MAX_AGE_IN_SECS))) {
    		cacheConfig.setMaxAgeInSeconds(node.get(PPC(Configuration.MAX_AGE_IN_SECS)).asInt());
    	}
    	else {
    		cacheConfig.setMaxAgeInSeconds(28800);
    	}
    	
    	if (node.hasDefined(PPC(Configuration.MAX_STALENESS))) {
    		cacheConfig.setMaxStaleness(node.get(PPC(Configuration.MAX_STALENESS)).asInt());
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
    
}
