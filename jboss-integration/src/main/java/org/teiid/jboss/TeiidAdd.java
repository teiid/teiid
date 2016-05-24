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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ProcessType;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.access.Environment;
import org.jboss.as.controller.registry.ImmutableManagementResourceRegistration;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.controller.services.path.RelativePathService;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.deployment.Phase;
import org.jboss.as.threads.ThreadFactoryResolver;
import org.jboss.as.threads.ThreadsServices;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.*;
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.value.InjectedValue;
import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.PreParser;
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
import org.teiid.dqp.service.SessionService;
import org.teiid.events.EventDistributorFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;
import org.teiid.runtime.MaterializationManager;
import org.teiid.services.InternalEventDistributorFactory;
import org.teiid.services.SessionServiceImpl;
import org.wildfly.clustering.jgroups.ChannelFactory;
import org.wildfly.clustering.jgroups.spi.service.ProtocolStackServiceName;

class TeiidAdd extends AbstractAddStepHandler {
	
	public static TeiidAdd INSTANCE = new TeiidAdd(); 

	static SimpleAttributeDefinition[] ATTRIBUTES = {
		TeiidConstants.ALLOW_ENV_FUNCTION_ELEMENT,
		TeiidConstants.ASYNC_THREAD_POOL_ELEMENT,
		TeiidConstants.THREAD_COUNT_ATTRIBUTE,
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
		TeiidConstants.PREPARSER_MODULE_ELEMENT,
		TeiidConstants.AUTHORIZATION_VALIDATOR_MODULE_ELEMENT,
		TeiidConstants.POLICY_DECIDER_MODULE_ELEMENT,
		TeiidConstants.DATA_ROLES_REQUIRED_ELEMENT,
		
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
		TeiidConstants.RSC_ENABLE_ATTRIBUTE,
		
		// session
		TeiidConstants.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_TYPE_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE
	};
	
	@Override
    protected void populateModel(final OperationContext context,
            final ModelNode operation, final Resource resource)
            throws OperationFailedException {	
		resource.getModel().setEmptyObject();
		populate(operation, resource.getModel());
		
		if (context.getProcessType().equals(ProcessType.STANDALONE_SERVER) && context.isNormalServer()) {
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
    protected void performRuntime(final OperationContext context,
            final ModelNode operation, final ModelNode model)
            throws OperationFailedException {
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		try {
			try {
				classloader = Module.getCallerModule().getClassLoader();
			} catch(Throwable t) {
				//ignore..
			}
			Thread.currentThread().setContextClassLoader(classloader);
			initilaizeTeiidEngine(context, operation);
		} finally {
			Thread.currentThread().setContextClassLoader(classloader);
		}
	}

    public String getNodeName() {
        String nodeName = System.getProperty("jboss.node.name");
        try {
            return (nodeName != null)?nodeName:InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        } 
    }	

    private void initilaizeTeiidEngine(final OperationContext context,
            final ModelNode operation) throws OperationFailedException {
		ServiceTarget target = context.getServiceTarget();
		
		final String nodeName = getNodeName();
		
		// async thread-pool
		int maxThreads = 10;
        if (isDefined(ASYNC_THREAD_POOL_ELEMENT, operation, context)) {
            if(asInt(THREAD_COUNT_ATTRIBUTE, operation, context) != null) {
                maxThreads = asInt(THREAD_COUNT_ATTRIBUTE, operation, context);
            }
        }
        buildThreadService(maxThreads, target);
		
		// translator repository
    	final TranslatorRepository translatorRepo = new TranslatorRepository();
        ValueService<TranslatorRepository> translatorService = new ValueService<TranslatorRepository>(
                new org.jboss.msc.value.Value<TranslatorRepository>() {
			@Override
			public TranslatorRepository getValue() throws IllegalStateException, IllegalArgumentException {
				return translatorRepo;
			}
    	});
        ServiceController<TranslatorRepository> service = target.addService(
                TeiidServiceNames.TRANSLATOR_REPO, translatorService).install();
    	
    	// system function tree
		SystemFunctionManager systemFunctionManager = new SystemFunctionManager();
		if (isDefined(ALLOW_ENV_FUNCTION_ELEMENT, operation, context)) {
			systemFunctionManager.setAllowEnvFunction(asBoolean(ALLOW_ENV_FUNCTION_ELEMENT, operation, context));
		}
		else {
			systemFunctionManager.setAllowEnvFunction(false);
		}
		systemFunctionManager.setClassloader(Thread.currentThread().getContextClassLoader()); 
    	
    	// VDB repository
    	final VDBRepository vdbRepository = new VDBRepository();
    	vdbRepository.setSystemFunctionManager(systemFunctionManager);
    	if (isDefined(DATA_ROLES_REQUIRED_ELEMENT, operation, context) && asBoolean(DATA_ROLES_REQUIRED_ELEMENT, operation, context)) {
    		vdbRepository.setDataRolesRequired(true);
    	}

    	VDBRepositoryService vdbRepositoryService = new VDBRepositoryService(vdbRepository);
    	target.addService(TeiidServiceNames.VDB_REPO, vdbRepositoryService).install();
		
    	// VDB Status manager
    	final VDBStatusCheckerExecutorService statusChecker = new VDBStatusCheckerExecutorService();
    	ValueService<VDBStatusChecker> statusService = new ValueService<VDBStatusChecker>(new org.jboss.msc.value.Value<VDBStatusChecker>() {
			@Override
			public VDBStatusChecker getValue() throws IllegalStateException, IllegalArgumentException {
				return statusChecker;
			}
    	});
    	ServiceBuilder<VDBStatusChecker> statusBuilder = target.addService(TeiidServiceNames.VDB_STATUS_CHECKER, statusService);
    	statusBuilder.addDependency(TeiidServiceNames.THREAD_POOL_SERVICE, Executor.class,  statusChecker.executorInjector);
    	statusBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class,  statusChecker.vdbRepoInjector);
    	statusBuilder.install();
    	
    	RelativePathService.addService(TeiidServiceNames.DATA_DIR, "teiid-data", "jboss.server.data.dir", target); //$NON-NLS-1$ //$NON-NLS-2$
    	final ObjectsSerializerService serializer = new ObjectsSerializerService();
    	ServiceBuilder<ObjectSerializer> objectSerializerService = target.addService(TeiidServiceNames.OBJECT_SERIALIZER, serializer);
    	objectSerializerService.addDependency(TeiidServiceNames.DATA_DIR, String.class, serializer.getPathInjector());
    	objectSerializerService.install();
    	
    	// Object Replicator
    	boolean replicatorAvailable = false;
    	if (isDefined(DC_STACK_ATTRIBUTE, operation, context)) {
    		String stack = asString(DC_STACK_ATTRIBUTE, operation, context);
    		
    		replicatorAvailable = true;
    		JGroupsObjectReplicatorService replicatorService = new JGroupsObjectReplicatorService();
			ServiceBuilder<JGroupsObjectReplicator> serviceBuilder = target.addService(TeiidServiceNames.OBJECT_REPLICATOR, replicatorService);
			serviceBuilder.addDependency(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(stack), ChannelFactory.class, replicatorService.channelFactoryInjector); //$NON-NLS-1$ //$NON-NLS-2$
			serviceBuilder.addDependency(TeiidServiceNames.THREAD_POOL_SERVICE, Executor.class,  replicatorService.executorInjector);
			serviceBuilder.install();
			LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50003)); 
    	} else {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_not_enabled")); //$NON-NLS-1$
    	}

    	// TODO: remove verbose service by moving the buffer service from runtime project
    	RelativePathService.addService(TeiidServiceNames.BUFFER_DIR, "teiid-buffer", "jboss.server.temp.dir", target); //$NON-NLS-1$ //$NON-NLS-2$
    	BufferManagerService bufferService = buildBufferManager(context, operation);
    	ServiceBuilder<BufferManager> bufferServiceBuilder = target.addService(TeiidServiceNames.BUFFER_MGR, bufferService);
    	bufferServiceBuilder.addDependency(TeiidServiceNames.BUFFER_DIR, String.class, bufferService.pathInjector);
    	bufferServiceBuilder.install();
    	
    	TupleBufferCacheService tupleBufferService = new TupleBufferCacheService();
    	ServiceBuilder<TupleBufferCache> tupleBufferBuilder = target.addService(TeiidServiceNames.TUPLE_BUFFER, tupleBufferService);
    	tupleBufferBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, tupleBufferService.bufferMgrInjector);
    	tupleBufferBuilder.addDependency(replicatorAvailable?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, tupleBufferService.replicatorInjector);
    	tupleBufferBuilder.install();
    	
    	
    	EventDistributorFactoryService edfs = new EventDistributorFactoryService();
    	ServiceBuilder<InternalEventDistributorFactory> edfsServiceBuilder = target.addService(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY, edfs);
    	edfsServiceBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, edfs.vdbRepositoryInjector);
    	edfsServiceBuilder.addDependency(replicatorAvailable?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, edfs.objectReplicatorInjector);
    	edfsServiceBuilder.install();
    	
    	PolicyDecider policyDecider = null;
    	if (isDefined(POLICY_DECIDER_MODULE_ELEMENT, operation, context)) {
    		policyDecider = buildService(PolicyDecider.class, asString(POLICY_DECIDER_MODULE_ELEMENT, operation, context));    		
    	}
    	
    	final AuthorizationValidator authValidator;
    	if (isDefined(AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, operation, context)) {
    		authValidator = buildService(AuthorizationValidator.class, asString(AUTHORIZATION_VALIDATOR_MODULE_ELEMENT, operation, context));
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
    	target.addService(TeiidServiceNames.AUTHORIZATION_VALIDATOR, authValidatorService).install();
    	
    	final PreParser preParser;
    	if (isDefined(PREPARSER_MODULE_ELEMENT, operation, context)) {
    		preParser = buildService(PreParser.class, asString(PREPARSER_MODULE_ELEMENT, operation, context));
    	} else {
    		preParser = new PreParser() {
				
				@Override
				public String preParse(String command, CommandContext context) {
					return command;
				}
			};
    	}
    	
    	ValueService<PreParser> preParserService = new ValueService<PreParser>(new org.jboss.msc.value.Value<PreParser>() {
			@Override
			public PreParser getValue() throws IllegalStateException, IllegalArgumentException {
				return preParser;
			}
    	});    	
    	target.addService(TeiidServiceNames.PREPARSER, preParserService).install();
    	
    	// resultset cache
    	boolean rsCache = true;
    	if (isDefined(RSC_ENABLE_ATTRIBUTE, operation, context) && !asBoolean(RSC_ENABLE_ATTRIBUTE, operation, context)) {
    		rsCache = false;
    	}
    		
    	if (!isDefined(RSC_CONTAINER_NAME_ATTRIBUTE, operation, context)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50094));
    	}

    	String cacheName = "resultset"; //$NON-NLS-1$
    	if (isDefined(RSC_NAME_ATTRIBUTE, operation, context)) {
    		// if null; default cache will be used
    		cacheName = asString(RSC_NAME_ATTRIBUTE, operation, context);
    	}	 
    	
    	if (rsCache) {
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(TeiidServiceNames.RESULTSET_CACHE_FACTORY, cfs);
	    	
	    	String ispnName = asString(RSC_CONTAINER_NAME_ATTRIBUTE, operation, context);
	    	cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
	    	cacheFactoryBuilder.install();
	    	
	    	int maxStaleness = 60;
	    	if (isDefined(RSC_MAX_STALENESS_ATTRIBUTE, operation, context)) {
	    		maxStaleness = asInt(RSC_MAX_STALENESS_ATTRIBUTE, operation, context);
	    	}
	    	
	    	CacheService<CachedResults> resultSetService = new CacheService<CachedResults>(cacheName, SessionAwareCache.Type.RESULTSET, maxStaleness);
	    	ServiceBuilder<SessionAwareCache<CachedResults>> resultsCacheBuilder = target.addService(TeiidServiceNames.CACHE_RESULTSET, resultSetService);
	    	resultsCacheBuilder.addDependency(TeiidServiceNames.TUPLE_BUFFER, TupleBufferCache.class, resultSetService.tupleBufferCacheInjector);
	    	resultsCacheBuilder.addDependency(TeiidServiceNames.RESULTSET_CACHE_FACTORY, CacheFactory.class, resultSetService.cacheFactoryInjector);
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	resultsCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName + SessionAwareCache.REPL)); //$NON-NLS-1$
	    	resultsCacheBuilder.install();
    	}
    	
    	// prepared-plan cache
    	boolean ppCache = true;
    	if (isDefined(PPC_ENABLE_ATTRIBUTE, operation, context)) {
    		ppCache = asBoolean(PPC_ENABLE_ATTRIBUTE, operation, context);
    	}
    		
    	if (!isDefined(PPC_CONTAINER_NAME_ATTRIBUTE, operation, context)) {
    		throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50095));
    	}

    	cacheName = "preparedplan"; //$NON-NLS-1$
    	if (isDefined(PPC_NAME_ATTRIBUTE, operation, context)) {
    		cacheName = asString(PPC_NAME_ATTRIBUTE, operation, context);
    	}	 
    	
    	if (ppCache) {
	    	CacheFactoryService cfs = new CacheFactoryService();
	    	ServiceBuilder<CacheFactory> cacheFactoryBuilder = target.addService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY, cfs);
	    	
	    	String ispnName = asString(PPC_CONTAINER_NAME_ATTRIBUTE, operation, context);
    		cacheFactoryBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName), EmbeddedCacheManager.class, cfs.cacheContainerInjector); //$NON-NLS-1$
    		cacheFactoryBuilder.install();
	    	
	    	CacheService<PreparedPlan> preparedPlanService = new CacheService<PreparedPlan>(cacheName, SessionAwareCache.Type.PREPAREDPLAN, 0);
	    	ServiceBuilder<SessionAwareCache<PreparedPlan>> preparedPlanCacheBuilder = target.addService(TeiidServiceNames.CACHE_PREPAREDPLAN, preparedPlanService);
	    	preparedPlanCacheBuilder.addDependency(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY, CacheFactory.class, preparedPlanService.cacheFactoryInjector);
	    	preparedPlanCacheBuilder.addDependency(ServiceName.JBOSS.append("infinispan", ispnName, cacheName)); //$NON-NLS-1$
	    	preparedPlanCacheBuilder.install();
    	}    	
    	
    	// Query Engine
    	final DQPCoreService engine = buildQueryEngine(context, operation);
    	String workManager = "default"; //$NON-NLS-1$
    	if (isDefined(WORKMANAGER, operation, context)) {
    		workManager = asString(WORKMANAGER, operation, context);
    	}
    	
        ServiceBuilder<DQPCore> engineBuilder = target.addService(TeiidServiceNames.ENGINE, engine);
        engineBuilder.addDependency(ServiceName.JBOSS.append("connector", "workmanager", workManager), WorkManager.class, engine.getWorkManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "XATerminator"), XATerminator.class, engine.getXaTerminatorInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(ServiceName.JBOSS.append("txn", "TransactionManager"), TransactionManager.class, engine.getTxnManagerInjector()); //$NON-NLS-1$ //$NON-NLS-2$
        engineBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, engine.getBufferManagerInjector());
        engineBuilder.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class, engine.getTranslatorRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, engine.getVdbRepositoryInjector());
        engineBuilder.addDependency(TeiidServiceNames.AUTHORIZATION_VALIDATOR, AuthorizationValidator.class, engine.getAuthorizationValidatorInjector());
        engineBuilder.addDependency(TeiidServiceNames.PREPARSER, PreParser.class, engine.getPreParserInjector());
        engineBuilder.addDependency(rsCache?DependencyType.REQUIRED:DependencyType.OPTIONAL, TeiidServiceNames.CACHE_RESULTSET, SessionAwareCache.class, engine.getResultSetCacheInjector());
        engineBuilder.addDependency(TeiidServiceNames.CACHE_PREPAREDPLAN, SessionAwareCache.class, engine.getPreparedPlanCacheInjector());
        engineBuilder.addDependency(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY, InternalEventDistributorFactory.class, engine.getEventDistributorFactoryInjector());
        
        engineBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        engineBuilder.install(); 
        Environment environment = context.getCallEnvironment();
        
		final JBossLifeCycleListener shutdownListener = new JBossLifeCycleListener(environment);
            	
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
					
		referenceBuilder.install();
		binderBuilder.install();
		
		LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("event_distributor_bound", jndiName)); //$NON-NLS-1$

		// Materialization management service
		MaterializationManagementService matviewService = new MaterializationManagementService(shutdownListener);
		ServiceBuilder<MaterializationManager> matviewBuilder = target.addService(TeiidServiceNames.MATVIEW_SERVICE, matviewService);
		matviewBuilder.addDependency(TeiidServiceNames.ENGINE, DQPCore.class,  matviewService.dqpInjector);
		matviewBuilder.addDependency(TeiidServiceNames.THREAD_POOL_SERVICE, Executor.class,  matviewService.executorInjector);
		matviewBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, matviewService.vdbRepositoryInjector);
		matviewBuilder.install();
		
        // Register VDB deployer
        context.addStep(new AbstractDeploymentChainStep() {
			@Override
			public void execute(DeploymentProcessorTarget processorTarget) {
				// vdb deployers
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0xFF75,new DynamicVDBRootMountDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0xFF76,new VDBStructureDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.INSTALL, Phase.INSTALL_WAR_DEPLOYMENT|0x1000, new VDBDeployer(translatorRepo, vdbRepository, shutdownListener));
				
				// translator deployers
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.STRUCTURE, Phase.STRUCTURE_JDBC_DRIVER|0x0001,new TranslatorStructureDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.DEPENDENCIES, Phase.DEPENDENCIES_MODULE|0x0001, new TranslatorDependencyDeployer());
				processorTarget.addDeploymentProcessor(TeiidExtension.TEIID_SUBSYSTEM, Phase.INSTALL, Phase.INSTALL_JDBC_DRIVER|0x0001, new TranslatorDeployer());
			}
        	
        }, OperationContext.Stage.RUNTIME);

        //install a session service
        //TODO: may eventually couple this with DQPCore
        final SessionServiceImpl sessionServiceImpl = new SessionServiceImpl();
        
        if (isDefined(AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, operation, context)) {
   			String securityDomain = asString(AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, operation, context);
   			sessionServiceImpl.setSecurityDomain(securityDomain);
		}  
   		
   		if (isDefined(AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, operation, context)) {
   			sessionServiceImpl.setSessionMaxLimit(asLong(AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, operation, context));
   		}
    	
   		if (isDefined(AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, operation, context)) {
   			sessionServiceImpl.setSessionExpirationTimeLimit(asLong(AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, operation, context));
   		}
   		
   		if (isDefined(AUTHENTICATION_TYPE_ATTRIBUTE, operation, context)) {
   			sessionServiceImpl.setAuthenticationType(AuthenticationType.valueOf(asString(AUTHENTICATION_TYPE_ATTRIBUTE, operation, context)));
   		}
   		else {
   			sessionServiceImpl.setAuthenticationType(AuthenticationType.USERPASSWORD);
   		}
   		
   		if (isDefined(AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE, operation, context)) {
   			boolean allowUnauthenticated = asBoolean(AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE, operation, context);
   			sessionServiceImpl.setTrustAllLocal(allowUnauthenticated);
		}
   		
   		sessionServiceImpl.setDqp(engine.getValue());
   		sessionServiceImpl.setVDBRepository(vdbRepository);
   		sessionServiceImpl.setSecurityHelper(new JBossSecurityHelper());
   		sessionServiceImpl.start();
   		
   		ServiceBuilder<SessionService> sessionServiceBuilder = target.addService(TeiidServiceNames.SESSION, new Service<SessionService>() {
   			@Override
   			public SessionService getValue() throws IllegalStateException,
   					IllegalArgumentException {
   				return sessionServiceImpl;
   			}
   			
   			@Override
   			public void stop(StopContext context) {
   				sessionServiceImpl.stop();
   			}

			@Override
			public void start(StartContext context) throws StartException {
						
			}
   		});
   		
   		sessionServiceBuilder.install();
	}
	
    private void buildThreadService(int maxThreads, ServiceTarget target) {
        ThreadExecutorService service = new ThreadExecutorService(maxThreads);
        final ServiceBuilder<?> serviceBuilder = target.addService(TeiidServiceNames.THREAD_POOL_SERVICE, service);
        serviceBuilder.install();
    }

    static class TeiidThreadFactoryResolver extends ThreadFactoryResolver.SimpleResolver{
        private TeiidThreadFactoryResolver() {
            super(ThreadsServices.FACTORY);
        }
        @Override
        protected String getThreadGroupName(String threadPoolName) {
            return "Teiid Async Thread";
        }
    }
    
    static <T> T buildService(Class<T> type, String moduleName) throws OperationFailedException {
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
        final T instance = iter.next();
		@SuppressWarnings("unchecked")
        T proxy = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), new Class[] { type }, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(instance.getClass().getClassLoader());
                    return method.invoke(instance, args);
                } catch (InvocationTargetException e) {
                	throw e.getTargetException();
                } finally {
                    Thread.currentThread().setContextClassLoader(originalCL);
                }
            }
        });
        return proxy;
    }

    private BufferManagerService buildBufferManager(final OperationContext context, ModelNode node) throws OperationFailedException {
    	BufferManagerService bufferManger = new BufferManagerService();
    	
    	if (node == null) {
    		return bufferManger;
    	}
    	
    	if (isDefined(USE_DISK_ATTRIBUTE, node, context)) {
    		bufferManger.setUseDisk(asBoolean(USE_DISK_ATTRIBUTE, node, context));
    	}	                	
    	if (isDefined(PROCESSOR_BATCH_SIZE_ATTRIBUTE, node, context)) {
    		bufferManger.setProcessorBatchSize(asInt(PROCESSOR_BATCH_SIZE_ATTRIBUTE, node, context));
    	}	
    	if (isDefined(MAX_PROCESSING_KB_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxProcessingKb(asInt(MAX_PROCESSING_KB_ATTRIBUTE, node, context));
    	}
    	if (isDefined(MAX_RESERVED_KB_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxReserveKb(asInt(MAX_RESERVED_KB_ATTRIBUTE, node, context));
    	}
    	if (isDefined(MAX_FILE_SIZE_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxFileSize(asLong(MAX_FILE_SIZE_ATTRIBUTE, node, context));
    	}
    	if (isDefined(MAX_BUFFER_SPACE_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxBufferSpace(asLong(MAX_BUFFER_SPACE_ATTRIBUTE, node, context));
    	}
    	if (isDefined(MAX_OPEN_FILES_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxOpenFiles(asInt(MAX_OPEN_FILES_ATTRIBUTE, node, context));
    	}	    
    	if (isDefined(MEMORY_BUFFER_SPACE_ATTRIBUTE, node, context)) {
    		bufferManger.setMemoryBufferSpace(asInt(MEMORY_BUFFER_SPACE_ATTRIBUTE, node, context));
    	}  
    	if (isDefined(MEMORY_BUFFER_OFFHEAP_ATTRIBUTE, node, context)) {
    		bufferManger.setMemoryBufferOffHeap(asBoolean(MEMORY_BUFFER_OFFHEAP_ATTRIBUTE, node, context));
    	} 
    	if (isDefined(MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE, node, context)) {
    		bufferManger.setMaxStorageObjectSize(asInt(MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE, node, context));
    	}
    	if (isDefined(INLINE_LOBS, node, context)) {
    		bufferManger.setInlineLobs(asBoolean(INLINE_LOBS, node, context));
    	}     	
    	if (isDefined(ENCRYPT_FILES_ATTRIBUTE, node, context)) {
    		bufferManger.setEncryptFiles(asBoolean(ENCRYPT_FILES_ATTRIBUTE, node, context));
    	}
    	return bufferManger;
    }	
    
	private DQPCoreService buildQueryEngine(final OperationContext context, ModelNode node) throws OperationFailedException {
		DQPCoreService engine = new DQPCoreService();
    	
    	if (isDefined(MAX_THREADS_ELEMENT, node, context)) {
    		engine.setMaxThreads(asInt(MAX_THREADS_ELEMENT, node, context));
    	}
    	if (isDefined(MAX_ACTIVE_PLANS_ELEMENT, node, context)) {
    		engine.setMaxActivePlans(asInt(MAX_ACTIVE_PLANS_ELEMENT, node, context));
    	}
    	if (isDefined(USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, node, context)) {
    		engine.setUserRequestSourceConcurrency(asInt(USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT, node, context));
    	}	
    	if (isDefined(TIME_SLICE_IN_MILLI_ELEMENT, node, context)) {
    		engine.setTimeSliceInMilli(asInt(TIME_SLICE_IN_MILLI_ELEMENT, node, context));
    	}
    	if (isDefined(MAX_ROWS_FETCH_SIZE_ELEMENT, node, context)) {
    		engine.setMaxRowsFetchSize(asInt(MAX_ROWS_FETCH_SIZE_ELEMENT, node, context));
    	}
    	if (isDefined(LOB_CHUNK_SIZE_IN_KB_ELEMENT, node, context)) {
    		engine.setLobChunkSizeInKB(asInt(LOB_CHUNK_SIZE_IN_KB_ELEMENT, node, context));
    	}
    	if (isDefined(QUERY_THRESHOLD_IN_SECS_ELEMENT, node, context)) {
    		engine.setQueryThresholdInSecs(asInt(QUERY_THRESHOLD_IN_SECS_ELEMENT, node, context));
    	}
    	if (isDefined(MAX_SOURCE_ROWS_ELEMENT, node, context)) {
    		engine.setMaxSourceRows(asInt(MAX_SOURCE_ROWS_ELEMENT, node, context));
    	}
    	if (isDefined(EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, node, context)) {
    		engine.setExceptionOnMaxSourceRows(asBoolean(EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT, node, context));
    	}
    	if (isDefined(DETECTING_CHANGE_EVENTS_ELEMENT, node, context)) {
    		engine.setDetectingChangeEvents(asBoolean(DETECTING_CHANGE_EVENTS_ELEMENT, node, context));
    	}	 
    	if (isDefined(QUERY_TIMEOUT, node, context)) {
    		engine.setQueryTimeout(asLong(QUERY_TIMEOUT, node, context));
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
