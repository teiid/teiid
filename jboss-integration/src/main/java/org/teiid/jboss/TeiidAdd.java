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
import static org.teiid.jboss.TeiidConstants.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
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
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.ImmutableManagementResourceRegistration;
import org.jboss.as.controller.registry.Resource;
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
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.service.ServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
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
import org.teiid.jboss.TeiidConstants.TeiidAttribute;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;
import org.teiid.services.InternalEventDistributorFactory;

class TeiidAdd extends AbstractAddStepHandler {
	
	public static TeiidAdd INSTANCE = new TeiidAdd(); 

	static TeiidAttribute[] ATTRIBUTES = {
		TeiidConstants.ALLOW_ENV_FUNCTION_ELEMENT,
		TeiidConstants.ASYNC_THREAD_POOL_ELEMENT,
		TeiidConstants.MAX_THREADS_ELEMENT,
		TeiidConstants.MAX_ACTIVE_PLANS_ELEMENT,
		TeiidConstants.USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, 
		TeiidConstants.TIME_SLICE_IN_MILLI_ELEMENT, 
		TeiidConstants.MAX_ROWS_FETCH_SIZE_ELEMENT,
		TeiidConstants.LOB_CHUNK_SIZE_IN_KB_ELEMENT,
		TeiidConstants.QUERY_THRESHOLD_IN_SECS_ELEMENT,
		TeiidConstants.MAX_SOURCE_ROWS_ELEMENT,
		TeiidConstants.EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, 
		TeiidConstants.DETECTING_CHANGE_EVENTS_ELEMENT,
		TeiidConstants.QUERY_TIMEOUT,
		TeiidConstants.WORKMANAGER,
		TeiidConstants.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT,
		TeiidConstants.POLICY_DECIDER_MODULE_ELEMENT,
		
		// object replicator
		TeiidConstants.DC_STACK_ATTRIBUTE,

		// Buffer Service
		TeiidConstants.USE_DISK_ATTRIBUTE,
		TeiidConstants.INLINE_LOBS,
		TeiidConstants.PROCESSOR_BATCH_SIZE_ATTRIBUTE,
		TeiidConstants.MAX_PROCESSING_KB_ATTRIBUTE,
		TeiidConstants.MAX_RESERVED_KB_ATTRIBUTE,
		TeiidConstants.MAX_FILE_SIZE_ATTRIBUTE,
		TeiidConstants.MAX_BUFFER_SPACE_ATTRIBUTE,
		TeiidConstants.MAX_OPEN_FILES_ATTRIBUTE,
		TeiidConstants.MEMORY_BUFFER_SPACE_ATTRIBUTE,
		TeiidConstants.MEMORY_BUFFER_OFFHEAP_ATTRIBUTE,
		TeiidConstants.MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE,
		TeiidConstants.ENCRYPT_FILES_ATTRIBUTE,
		
		// prepared plan cache
		TeiidConstants.PPC_NAME_ATTRIBUTE,
		TeiidConstants.PPC_CONTAINER_NAME_ATTRIBUTE,
		TeiidConstants.PPC_ENABLE_ATTRIBUTE,
		
		// resultset cache
		TeiidConstants.RSC_NAME_ATTRIBUTE,
		TeiidConstants.RSC_CONTAINER_NAME_ATTRIBUTE,
		TeiidConstants.RSC_MAX_STALENESS_ATTRIBUTE,
		TeiidConstants.RSC_ENABLE_ATTRIBUTE
	};
	
	@Override
	protected void populateModel(final OperationContext context, final ModelNode operation, final Resource resource) throws  OperationFailedException {	
		resource.getModel().setEmptyObject();
		populate(operation, resource.getModel());
		if (context.isNormalServer()) {
			deployResources(context);
		}
	}
	
	@Override
	protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
		throw new UnsupportedOperationException();
	}

	static void populate(ModelNode operation, ModelNode model) throws OperationFailedException {
		for (int i = 0; i < ATTRIBUTES.length; i++) {
			ATTRIBUTES[i].validateAndSet(operation, model);
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
		
		final String asyncThreadPoolName = ASYNC_THREAD_POOL_ELEMENT.asString(operation, context); 
		
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
		if (ALLOW_ENV_FUNCTION_ELEMENT.isDefined(operation, context)) {
			systemFunctionManager.setAllowEnvFunction(ALLOW_ENV_FUNCTION_ELEMENT.asBoolean(operation, context));
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
    	if (DC_STACK_ATTRIBUTE.isDefined(operation, context)) {
    		String stack = DC_STACK_ATTRIBUTE.asString(operation, context);
    		
    		replicatorAvailable = true;
    		JGroupsObjectReplicatorService replicatorService = new JGroupsObjectReplicatorService();
			ServiceBuilder<JGroupsObjectReplicator> serviceBuilder = target.addService(TeiidServiceNames.OBJECT_REPLICATOR, replicatorService);
			serviceBuilder.addDependency(ServiceName.JBOSS.append("jgroups", "stack", stack), ChannelFactory.class, replicatorService.channelFactoryInjector); //$NON-NLS-1$ //$NON-NLS-2$
			serviceBuilder.addDependency(TeiidServiceNames.executorServiceName(asyncThreadPoolName), Executor.class,  replicatorService.executorInjector);
			newControllers.add(serviceBuilder.install());
			LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50003)); 
    	} else {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_not_enabled")); //$NON-NLS-1$
    	}

    	// TODO: remove verbose service by moving the buffer service from runtime project
    	newControllers.add(RelativePathService.addService(TeiidServiceNames.BUFFER_DIR, "teiid-buffer", "jboss.server.temp.dir", target)); //$NON-NLS-1$ //$NON-NLS-2$
    	BufferManagerService bufferService = buildBufferManager(context, operation);
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
    	if (POLICY_DECIDER_MODULE_ELEMENT.isDefined(operation, context)) {
    		policyDecider = buildService(PolicyDecider.class, POLICY_DECIDER_MODULE_ELEMENT.asString(operation, context));    		
    	}
    	
    	final AuthorizationValidator authValidator;
    	if (AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.isDefined(operation, context)) {
    		authValidator = buildService(AuthorizationValidator.class, AUTHORIZATION_VALIDATOR_MODULE_ELEMENT.asString(operation, context));
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
    	if (RSC_ENABLE_ATTRIBUTE.isDefined(operation, context) && !RSC_ENABLE_ATTRIBUTE.asBoolean(operation, context)) {
    		rsCache = false;
    	}
    		
    	if (!RSC_CONTAINER_NAME_ATTRIBUTE.isDefined(operation, context)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50094));
    	}

    	String cacheName = "resultset"; //$NON-NLS-1$
    	if (RSC_NAME_ATTRIBUTE.isDefined(operation, context)) {
    		// if null; default cache will be used
    		cacheName = RSC_NAME_ATTRIBUTE.asString(operation, context);
    	}	 
    	
    	if (rsCache) {
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(TeiidServiceNames.RESULTSET_CACHE_FACTORY, cfs);
	    	
	    	String ispnName = RSC_CONTAINER_NAME_ATTRIBUTE.asString(operation, context);
	    	cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
	    	newControllers.add(cacheFactoryBuilder.install());
	    	
	    	int maxStaleness = 60;
	    	if (RSC_MAX_STALENESS_ATTRIBUTE.isDefined(operation, context)) {
	    		maxStaleness = RSC_MAX_STALENESS_ATTRIBUTE.asInt(operation, context);
	    	}
	    	
	    	CacheService<CachedResults> resultSetService = new CacheService<CachedResults>(cacheName, SessionAwareCache.Type.RESULTSET, maxStaleness);
	    	ServiceBuilder<SessionAwareCache<CachedResults>> resultsCacheBuilder = target.addService(TeiidServiceNames.CACHE_RESULTSET, resultSetService);
	    	resultsCacheBuilder.addDependency(TeiidServiceNames.TUPLE_BUFFER, TupleBufferCache.class, resultSetService.tupleBufferCacheInjector);
	    	resultsCacheBuilder.addDependency(TeiidServiceNames.RESULTSET_CACHE_FACTORY, CacheFactory.class, resultSetService.cacheFactoryInjector);
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName + SessionAwareCache.REPL)); //$NON-NLS-1$
	    	newControllers.add(resultsCacheBuilder.install());
    	}
    	
    	// prepared-plan cache
    	boolean ppCache = true;
    	if (PPC_ENABLE_ATTRIBUTE.isDefined(operation, context)) {
    		ppCache = PPC_ENABLE_ATTRIBUTE.asBoolean(operation, context);
    	}
    		
    	if (!PPC_CONTAINER_NAME_ATTRIBUTE.isDefined(operation, context)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50095));
    	}

    	cacheName = "preparedplan"; //$NON-NLS-1$
    	if (PPC_NAME_ATTRIBUTE.isDefined(operation, context)) {
    		cacheName = PPC_NAME_ATTRIBUTE.asString(operation, context);
    	}	 
    	
    	if (ppCache) {
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY, cfs);
	    	
	    	String ispnName = PPC_CONTAINER_NAME_ATTRIBUTE.asString(operation, context);
    		cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
    		newControllers.add(cacheFactoryBuilder.install());
	    	
	    	CacheService<PreparedPlan> preparedPlanService = new CacheService<PreparedPlan>(cacheName, SessionAwareCache.Type.PREPAREDPLAN, 0);
	    	ServiceBuilder<SessionAwareCache<PreparedPlan>> preparedPlanCacheBuilder = target.addService(TeiidServiceNames.CACHE_PREPAREDPLAN, preparedPlanService);
	    	preparedPlanCacheBuilder.addDependency(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY, CacheFactory.class, preparedPlanService.cacheFactoryInjector);
	    	preparedPlanCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	newControllers.add(preparedPlanCacheBuilder.install());
    	}    	
    	
    	// Query Engine
    	final DQPCoreService engine = buildQueryEngine(context, operation);
    	String workManager = "default"; //$NON-NLS-1$
    	if (WORKMANAGER.isDefined(operation, context)) {
    		workManager = WORKMANAGER.asString(operation, context);
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
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT,new DynamicVDBRootMountDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0x0001,new VDBStructureDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.INSTALL, Phase.INSTALL_WAR_DEPLOYMENT|0x1000, new VDBDeployer(translatorRepo, asyncThreadPoolName, vdbRepository, shutdownListener));
				
				// translator deployers
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_JDBC_DRIVER|0x0001,new TranslatorStructureDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.DEPENDENCIES, Phase.DEPENDENCIES_MODULE|0x0001, new TranslatorDependencyDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.INSTALL, Phase.INSTALL_JDBC_DRIVER|0x0001, new TranslatorDeployer());
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
		
    private BufferManagerService buildBufferManager(final OperationContext context, ModelNode node) throws OperationFailedException {
    	BufferManagerService bufferManger = new BufferManagerService();
    	
    	if (node == null) {
    		return bufferManger;
    	}
    	
    	if (USE_DISK_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setUseDisk(USE_DISK_ATTRIBUTE.asBoolean(node, context));
    	}	                	
    	if (PROCESSOR_BATCH_SIZE_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setProcessorBatchSize(PROCESSOR_BATCH_SIZE_ATTRIBUTE.asInt(node, context));
    	}	
    	if (MAX_PROCESSING_KB_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxProcessingKb(MAX_PROCESSING_KB_ATTRIBUTE.asInt(node, context));
    	}
    	if (MAX_RESERVED_KB_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxReserveKb(MAX_RESERVED_KB_ATTRIBUTE.asInt(node, context));
    	}
    	if (MAX_FILE_SIZE_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxFileSize(MAX_FILE_SIZE_ATTRIBUTE.asLong(node, context));
    	}
    	if (MAX_BUFFER_SPACE_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxBufferSpace(MAX_BUFFER_SPACE_ATTRIBUTE.asLong(node, context));
    	}
    	if (MAX_OPEN_FILES_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxOpenFiles(MAX_OPEN_FILES_ATTRIBUTE.asInt(node, context));
    	}	    
    	if (MEMORY_BUFFER_SPACE_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMemoryBufferSpace(MEMORY_BUFFER_SPACE_ATTRIBUTE.asInt(node, context));
    	}  
    	if (MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMemoryBufferOffHeap(MEMORY_BUFFER_OFFHEAP_ATTRIBUTE.asBoolean(node, context));
    	} 
    	if (MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setMaxStorageObjectSize(MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.asInt(node, context));
    	}
    	if (INLINE_LOBS.isDefined(node, context)) {
    		bufferManger.setInlineLobs(INLINE_LOBS.asBoolean(node, context));
    	}     	
    	if (ENCRYPT_FILES_ATTRIBUTE.isDefined(node, context)) {
    		bufferManger.setEncryptFiles(ENCRYPT_FILES_ATTRIBUTE.asBoolean(node, context));
    	}
    	return bufferManger;
    }	
    
	private DQPCoreService buildQueryEngine(final OperationContext context, ModelNode node) throws OperationFailedException {
		DQPCoreService engine = new DQPCoreService();
    	
    	if (MAX_THREADS_ELEMENT.isDefined(node, context)) {
    		engine.setMaxThreads(MAX_THREADS_ELEMENT.asInt(node, context));
    	}
    	if (MAX_ACTIVE_PLANS_ELEMENT.isDefined(node, context)) {
    		engine.setMaxActivePlans(MAX_ACTIVE_PLANS_ELEMENT.asInt(node, context));
    	}
    	if (USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.isDefined(node, context)) {
    		engine.setUserRequestSourceConcurrency(USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT.asInt(node, context));
    	}	
    	if (TIME_SLICE_IN_MILLI_ELEMENT.isDefined(node, context)) {
    		engine.setTimeSliceInMilli(TIME_SLICE_IN_MILLI_ELEMENT.asInt(node, context));
    	}
    	if (MAX_ROWS_FETCH_SIZE_ELEMENT.isDefined(node, context)) {
    		engine.setMaxRowsFetchSize(MAX_ROWS_FETCH_SIZE_ELEMENT.asInt(node, context));
    	}
    	if (LOB_CHUNK_SIZE_IN_KB_ELEMENT.isDefined(node, context)) {
    		engine.setLobChunkSizeInKB(LOB_CHUNK_SIZE_IN_KB_ELEMENT.asInt(node, context));
    	}
    	if (QUERY_THRESHOLD_IN_SECS_ELEMENT.isDefined(node, context)) {
    		engine.setQueryThresholdInSecs(QUERY_THRESHOLD_IN_SECS_ELEMENT.asInt(node, context));
    	}
    	if (MAX_SOURCE_ROWS_ELEMENT.isDefined(node, context)) {
    		engine.setMaxSourceRows(MAX_SOURCE_ROWS_ELEMENT.asInt(node, context));
    	}
    	if (EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.isDefined(node, context)) {
    		engine.setExceptionOnMaxSourceRows(EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT.asBoolean(node, context));
    	}
    	if (DETECTING_CHANGE_EVENTS_ELEMENT.isDefined(node, context)) {
    		engine.setDetectingChangeEvents(DETECTING_CHANGE_EVENTS_ELEMENT.asBoolean(node, context));
    	}	 
    	if (QUERY_TIMEOUT.isDefined(node, context)) {
    		engine.setQueryTimeout(QUERY_TIMEOUT.asLong(node, context));
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

	private void deployResources(OperationContext context) throws OperationFailedException{
        if (requiresRuntime(context)) {
        	try {
	        	Module module = Module.forClass(getClass());
	        	if (module == null) {
	        		return; // during testing
	        	}
	        	
	        	URL deployments = module.getExportedResource("deployments.properties"); //$NON-NLS-1$
	        	if (deployments == null) {
	        		return; // no deployments 
	        	}
	            BufferedReader in = new BufferedReader(new InputStreamReader(deployments.openStream()));
	
	            String deployment;
	            while ((deployment = in.readLine()) != null) {
	                PathAddress deploymentAddress = PathAddress.pathAddress(PathElement.pathElement(DEPLOYMENT, deployment));
	                ModelNode op = new ModelNode();
	                op.get(OP).set(ADD);
	                op.get(OP_ADDR).set(deploymentAddress.toModelNode());
	                op.get(ENABLED).set(true);
	                op.get(PERSISTENT).set(false); // prevents writing this deployment out to standalone.xml
	
	                URL url = module.getExportedResource(deployment);
	                String urlString = url.toExternalForm();
	
	                ModelNode contentItem = new ModelNode();
	                contentItem.get(URL).set(urlString);
	                op.get(CONTENT).add(contentItem);
	
	                ImmutableManagementResourceRegistration rootResourceRegistration = context.getRootResourceRegistration();
	                OperationStepHandler handler = rootResourceRegistration.getOperationHandler(deploymentAddress, ADD);
	
	                context.addStep(op, handler, OperationContext.Stage.MODEL);
	            }
	            in.close();
        	}catch(IOException e) {
        		throw new OperationFailedException(e.getMessage(), e);
        	}
        }		
	}	
}
