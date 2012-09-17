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

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.as.clustering.jgroups.ChannelFactory;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.as.controller.services.path.RelativePathService;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.Services;
import org.jboss.as.server.deployment.Phase;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.value.InjectedValue;
import org.teiid.PolicyDecider;
import org.teiid.cache.CacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.events.EventDistributorFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.replication.jboss.JGroupsObjectReplicator;
import org.teiid.services.InternalEventDistributorFactory;

class TeiidAdd extends AbstractAddStepHandler implements DescriptionProvider {

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
		Element.QUERY_TIMEOUT,
		Element.WORKMANAGER,
		Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT,
		Element.POLICY_DECIDER_MODULE_ELEMENT,
		
		// object replicator
		Element.DC_STACK_ATTRIBUTE,

		// Buffer Service
		Element.USE_DISK_ATTRIBUTE,
		Element.INLINE_LOBS,
		Element.PROCESSOR_BATCH_SIZE_ATTRIBUTE,
		Element.CONNECTOR_BATCH_SIZE_ATTRIBUTE,
		Element.MAX_PROCESSING_KB_ATTRIBUTE,
		Element.MAX_RESERVED_KB_ATTRIBUTE,
		Element.MAX_FILE_SIZE_ATTRIBUTE,
		Element.MAX_BUFFER_SPACE_ATTRIBUTE,
		Element.MAX_OPEN_FILES_ATTRIBUTE,
		Element.MEMORY_BUFFER_SPACE_ATTRIBUTE,
		Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE,
		Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE,
		
		// prepared plan cache
		Element.PPC_NAME_ELEMENT,
		Element.PPC_CONTAINER_NAME_ELEMENT,
		Element.PPC_ENABLE_ATTRIBUTE,
		
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
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		try {
			try {
				classloader = Module.getCallerModule().getClassLoader();
			} catch(Throwable t) {
				//ignore..
			}
			Thread.currentThread().setContextClassLoader(classloader);
			initilaizeTeiidEngine(context, operation, newControllers);
		} finally {
			Thread.currentThread().setContextClassLoader(classloader);
		}
	}

	private void initilaizeTeiidEngine(final OperationContext context, final ModelNode operation, final List<ServiceController<?>> newControllers)
			throws OperationFailedException {
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
		systemFunctionManager.setClassloader(Thread.currentThread().getContextClassLoader()); 
    	
    	// VDB repository
    	final VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemFunctionManager(systemFunctionManager);
    	
    	VDBRepositoryService vdbRepositoryService = new VDBRepositoryService(vdbRepository);
    	newControllers.add(target.addService(TeiidServiceNames.VDB_REPO, vdbRepositoryService).install());
		
    	// VDB Status manager
    	final VDBStatusCheckerExecutorService statusChecker = new VDBStatusCheckerExecutorService();
    	statusChecker.setTranslatorRepository(translatorRepo);
    	ValueService<VDBStatusChecker> statusService = new ValueService<VDBStatusChecker>(new org.jboss.msc.value.Value<VDBStatusChecker>() {
			@Override
			public VDBStatusChecker getValue() throws IllegalStateException, IllegalArgumentException {
				return statusChecker;
			}
    	});
    	ServiceBuilder<VDBStatusChecker> statusBuilder = target.addService(TeiidServiceNames.VDB_STATUS_CHECKER, statusService);
    	statusBuilder.addDependency(TeiidServiceNames.executorServiceName(asyncThreadPoolName), Executor.class,  statusChecker.executorInjector);
    	statusBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class,  statusChecker.vdbRepoInjector);
    	newControllers.add(statusBuilder.install());    	
    	
    	newControllers.add(RelativePathService.addService(TeiidServiceNames.DATA_DIR, "teiid-data", "jboss.server.data.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	final ObjectsSerializerService serializer = new ObjectsSerializerService();
    	ServiceBuilder<ObjectSerializer> objectSerializerService = target.addService(TeiidServiceNames.OBJECT_SERIALIZER, serializer);
    	objectSerializerService.addDependency(TeiidServiceNames.DATA_DIR, String.class, serializer.getPathInjector());
    	newControllers.add(objectSerializerService.install());
    	
    	// Object Replicator
    	boolean replicatorAvailable = false;
    	if (Element.DC_STACK_ATTRIBUTE.isDefined(operation)) {
    		String stack = Element.DC_STACK_ATTRIBUTE.asString(operation);
    		
    		replicatorAvailable = true;
    		JGroupsObjectReplicatorService replicatorService = new JGroupsObjectReplicatorService();
			ServiceBuilder<JGroupsObjectReplicator> serviceBuilder = target.addService(TeiidServiceNames.OBJECT_REPLICATOR, replicatorService);
			serviceBuilder.addDependency(ServiceName.JBOSS.append("jgroups", "stack", stack), ChannelFactory.class, replicatorService.channelFactoryInjector); //$NON-NLS-1$ //$NON-NLS-2$
			newControllers.add(serviceBuilder.install());
			LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50003)); 
    	} else {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_not_enabled")); //$NON-NLS-1$
    	}

    	// TODO: remove verbose service by moving the buffer service from runtime project
    	newControllers.add(RelativePathService.addService(TeiidServiceNames.BUFFER_DIR, "teiid-buffer", "jboss.server.temp.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	BufferManagerService bufferService = buildBufferManager(operation);
    	ServiceBuilder<BufferManager> bufferServiceBuilder = target.addService(TeiidServiceNames.BUFFER_MGR, bufferService);
    	bufferServiceBuilder.addDependency(TeiidServiceNames.BUFFER_DIR, String.class, bufferService.pathInjector);
    	newControllers.add(bufferServiceBuilder.install());
    	
    	TupleBufferCacheService tupleBufferService = new TupleBufferCacheService();
    	ServiceBuilder<TupleBufferCache> tupleBufferBuilder = target.addService(TeiidServiceNames.TUPLE_BUFFER, tupleBufferService);
    	tupleBufferBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, tupleBufferService.bufferMgrInjector);
    	tupleBufferBuilder.addDependency(replicatorAvailable?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, tupleBufferService.replicatorInjector);
    	newControllers.add(tupleBufferBuilder.install());
    	
    	
    	EventDistributorFactoryService edfs = new EventDistributorFactoryService();
    	ServiceBuilder<InternalEventDistributorFactory> edfsServiceBuilder = target.addService(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY, edfs);
    	edfsServiceBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, edfs.vdbRepositoryInjector);
    	edfsServiceBuilder.addDependency(replicatorAvailable?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, edfs.objectReplicatorInjector);
    	newControllers.add(edfsServiceBuilder.install());
    	
    	PolicyDecider policyDecider = null;
    	if (Element.POLICY_DECIDER_MODULE_ELEMENT.isDefined(operation)) {
    		policyDecider = buildService(PolicyDecider.class, Element.POLICY_DECIDER_MODULE_ELEMENT.asString(operation));    		
    	}
    	
    	final AuthorizationValidator authValidator;
    	if (Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.isDefined(operation)) {
    		authValidator = buildService(AuthorizationValidator.class, Element.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.asString(operation));
    	}
    	else {
    		DefaultAuthorizationValidator dap = new DefaultAuthorizationValidator();
    		dap.setPolicyDecider(policyDecider);
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
    	boolean rsCache = true;
    	if (Element.RSC_ENABLE_ATTRIBUTE.isDefined(operation) && !Element.RSC_ENABLE_ATTRIBUTE.asBoolean(operation)) {
    		rsCache = false;
    	}
    		
    	if (!Element.RSC_CONTAINER_NAME_ELEMENT.isDefined(operation)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50094));
    	}

    	String cacheName = "resultset"; //$NON-NLS-1$
    	if (Element.RSC_NAME_ELEMENT.isDefined(operation)) {
    		// if null; default cache will be used
    		cacheName = Element.RSC_NAME_ELEMENT.asString(operation);
    	}	 
    	
    	if (rsCache) {
	    	ServiceName cfName = ServiceName.JBOSS.append("teiid", "infinispan-rs-cache-factory"); //$NON-NLS-1$ //$NON-NLS-2$
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(cfName, cfs);
	    	
	    	String ispnName = Element.RSC_CONTAINER_NAME_ELEMENT.asString(operation);
	    	cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
	    	newControllers.add(cacheFactoryBuilder.install());
	    	
	    	int maxStaleness = 60;
	    	if (Element.RSC_MAX_STALENESS_ELEMENT.isDefined(operation)) {
	    		maxStaleness = Element.RSC_MAX_STALENESS_ELEMENT.asInt(operation);
	    	}
	    	
	    	CacheService<CachedResults> resultSetService = new CacheService<CachedResults>(cacheName, SessionAwareCache.Type.RESULTSET, maxStaleness);
	    	ServiceBuilder<SessionAwareCache<CachedResults>> resultsCacheBuilder = target.addService(TeiidServiceNames.CACHE_RESULTSET, resultSetService);
	    	resultsCacheBuilder.addDependency(TeiidServiceNames.TUPLE_BUFFER, TupleBufferCache.class, resultSetService.tupleBufferCacheInjector);
	    	resultsCacheBuilder.addDependency(cfName, CacheFactory.class, resultSetService.cacheFactoryInjector);
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName + SessionAwareCache.REPL)); //$NON-NLS-1$
	    	newControllers.add(resultsCacheBuilder.install());
    	}
    	
    	// prepared-plan cache
    	boolean ppCache = true;
    	if (Element.PPC_ENABLE_ATTRIBUTE.isDefined(operation)) {
    		ppCache = Element.PPC_ENABLE_ATTRIBUTE.asBoolean(operation);
    	}
    		
    	if (!Element.PPC_CONTAINER_NAME_ELEMENT.isDefined(operation)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50095));
    	}

    	cacheName = "preparedplan"; //$NON-NLS-1$
    	if (Element.PPC_NAME_ELEMENT.isDefined(operation)) {
    		cacheName = Element.PPC_NAME_ELEMENT.asString(operation);
    	}	 
    	
    	if (ppCache) {
	    	ServiceName cfName = ServiceName.JBOSS.append("teiid", "infinispan-pp-cache-factory"); //$NON-NLS-1$ //$NON-NLS-2$
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(cfName, cfs);
	    	
	    	String ispnName = Element.PPC_CONTAINER_NAME_ELEMENT.asString(operation);
    		cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
    		newControllers.add(cacheFactoryBuilder.install());
	    	
	    	CacheService<PreparedPlan> preparedPlanService = new CacheService<PreparedPlan>(cacheName, SessionAwareCache.Type.PREPAREDPLAN, 0);
	    	ServiceBuilder<SessionAwareCache<PreparedPlan>> preparedPlanCacheBuilder = target.addService(TeiidServiceNames.CACHE_PREPAREDPLAN, preparedPlanService);
	    	preparedPlanCacheBuilder.addDependency(cfName, CacheFactory.class, preparedPlanService.cacheFactoryInjector);
	    	preparedPlanCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	newControllers.add(preparedPlanCacheBuilder.install());
    	}    	
    	
    	// Query Engine
    	final DQPCoreService engine = buildQueryEngine(operation);
    	String workManager = "default"; //$NON-NLS-1$
    	if (Element.WORKMANAGER.isDefined(operation)) {
    		workManager = Element.WORKMANAGER.asString(operation);
    	}
    	
        ServiceBuilder<DQPCore> engineBuilder = target.addService(TeiidServiceNames.ENGINE, engine);
        engineBuilder.addDependency(ServiceName.JBOSS.append("connector", "workmanager", workManager), WorkManager.class, engine.getWorkManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "XATerminator"), XATerminator.class, engine.getXaTerminatorInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "TransactionManager"), TransactionManager.class, engine.getTxnManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, engine.getBufferManagerInjector());
        engineBuilder.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class, engine.getTranslatorRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, engine.getVdbRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.AUTHORIZATION_VALIDATOR, AuthorizationValidator.class, engine.getAuthorizationValidatorInjector());
        engineBuilder.addDependency(rsCache?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.CACHE_RESULTSET, SessionAwareCache.class, engine.getResultSetCacheInjector());
        engineBuilder.addDependency(TeiidServiceNames.CACHE_PREPAREDPLAN, SessionAwareCache.class, engine.getPreparedPlanCacheInjector());
        engineBuilder.addDependency(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY, InternalEventDistributorFactory.class, engine.getEventDistributorFactoryInjector());
        
        engineBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        ServiceController<DQPCore> controller = engineBuilder.install(); 
        newControllers.add(controller);
        ServiceContainer container =  controller.getServiceContainer();
        container.addTerminateListener(shutdownListener);
        container.getService(Services.JBOSS_SERVER_CONTROLLER).addListener(shutdownListener);
            	
        // add JNDI for event distributor
		final ReferenceFactoryService<EventDistributorFactory> referenceFactoryService = new ReferenceFactoryService<EventDistributorFactory>();
		final ServiceName referenceFactoryServiceName = TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY.append("reference-factory"); //$NON-NLS-1$
		final ServiceBuilder<?> referenceBuilder = target.addService(referenceFactoryServiceName, referenceFactoryService);
		referenceBuilder.addDependency(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY, EventDistributorFactory.class, referenceFactoryService.getInjector());
		referenceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
		  
		String jndiName = "teiid/event-distributor-factory";//$NON-NLS-1$
		final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(jndiName); 
		final BinderService binderService = new BinderService(bindInfo.getBindName());
		final ServiceBuilder<?> binderBuilder = target.addService(bindInfo.getBinderServiceName(), binderService);
		binderBuilder.addDependency(referenceFactoryServiceName, ManagedReferenceFactory.class, binderService.getManagedObjectInjector());
		binderBuilder.addDependency(bindInfo.getParentContextServiceName(), ServiceBasedNamingStore.class, binderService.getNamingStoreInjector());        
		binderBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
					
		newControllers.add(referenceBuilder.install());
		newControllers.add(binderBuilder.install());
		
		LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("event_distributor_bound", jndiName)); //$NON-NLS-1$
        
        // Register VDB deployer
        context.addStep(new AbstractDeploymentChainStep() {
			@Override
			public void execute(DeploymentProcessorTarget processorTarget) {
				// vdb deployers
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT,new DynamicVDBRootMountDeployer());
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0x0001,new VDBStructureDeployer());
				processorTarget.addDeploymentProcessor(Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer());
				processorTarget.addDeploymentProcessor(Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyDeployer());
				processorTarget.addDeploymentProcessor(Phase.INSTALL, Phase.INSTALL_WAR_DEPLOYMENT|0x1000, new VDBDeployer(translatorRepo, asyncThreadPoolName, statusChecker, shutdownListener));
				
				// translator deployers
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_JDBC_DRIVER|0x0001,new TranslatorStructureDeployer());
				processorTarget.addDeploymentProcessor(Phase.DEPENDENCIES, Phase.DEPENDENCIES_MODULE|0x0001, new TranslatorDependencyDeployer());
				processorTarget.addDeploymentProcessor(Phase.INSTALL, Phase.INSTALL_JDBC_DRIVER|0x0001, new TranslatorDeployer());
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
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50069, moduleName), e);
        }
        ServiceLoader<T> services = module.loadService(type);
        Iterator<T> iter = services.iterator();
        if (!iter.hasNext()) {
        	throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50089, type.getName(), moduleName));
        }
        return iter.next();
    }
		
    private BufferManagerService buildBufferManager(ModelNode node) {
    	BufferManagerService bufferManger = new BufferManagerService();
    	
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
    	if (Element.MEMORY_BUFFER_SPACE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMemoryBufferSpace(Element.MEMORY_BUFFER_SPACE_ATTRIBUTE.asInt(node));
    	}  
    	if (Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMemoryBufferOffHeap(Element.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.asBoolean(node));
    	} 
    	if (Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.isDefined(node)) {
    		bufferManger.setMaxStorageObjectSize(Element.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.asInt(node));
    	}
    	if (Element.INLINE_LOBS.isDefined(node)) {
    		bufferManger.setInlineLobs(Element.INLINE_LOBS.asBoolean(node));
    	}     	
    	
    	return bufferManger;
    }	
    
	private DQPCoreService buildQueryEngine(ModelNode node) {
		DQPCoreService engine = new DQPCoreService();
    	
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
    	if (Element.QUERY_TIMEOUT.isDefined(node)) {
    		engine.setQueryTimeout(Element.QUERY_TIMEOUT.asLong(node));
    	}
		return engine;
	}    
	
	static class VDBStatusCheckerExecutorService extends VDBStatusChecker{
		final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
		final InjectedValue<VDBRepository> vdbRepoInjector = new InjectedValue<VDBRepository>();
		
		@Override
		public Executor getExecutor() {
			return this.executorInjector.getValue();
		}    	
		
		@Override
		public VDBRepository getVDBRepository() {
			return this.vdbRepoInjector.getValue();
		}
	}

	public static void registerReadWriteAttributes(ManagementResourceRegistration subsystem) {
		for (int i = 0; i < attributes.length; i++) {
			subsystem.registerReadWriteAttribute(attributes[i].getModelName(), null, AttributeWrite.INSTANCE, Storage.CONFIGURATION);
		}		
	}
}
