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
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.deployers.SystemVDBDeployer;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.DataRolePolicyDecider;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.services.BufferServiceImpl;

public class TeiidBootServicesAdd extends AbstractAddStepHandler implements DescriptionProvider {
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
        addAttribute(node, Configuration.ASYNC_THREAD_GROUP, type, bundle.getString(Configuration.ASYNC_THREAD_GROUP+DESC), ModelType.STRING, true, "teiid-async-threads"); //$NON-NLS-1$

        addAttribute(node, Configuration.AUTHORIZATION_VALIDATOR_MODULE, type, bundle.getString(Configuration.AUTHORIZATION_VALIDATOR_MODULE+DESC), ModelType.BOOLEAN, false, "false"); //$NON-NLS-1$
        addAttribute(node, Configuration.POLICY_DECIDER_MODULE, type, bundle.getString(Configuration.POLICY_DECIDER_MODULE+DESC), ModelType.STRING, false, "teiid-async-threads"); //$NON-NLS-1$
        
		ModelNode bufferNode = node.get(CHILDREN, Configuration.BUFFER_SERVICE);
		bufferNode.get(TYPE).set(ModelType.OBJECT);
		bufferNode.get(DESCRIPTION).set(bundle.getString(Configuration.BUFFER_SERVICE+DESC));
		bufferNode.get(REQUIRED).set(false);
		describeBufferManager(bufferNode, ATTRIBUTES, bundle);		
        
		// result-set-cache
		ModelNode rsCacheNode = node.get(CHILDREN, Configuration.RESULTSET_CACHE);
		rsCacheNode.get(TYPE).set(ModelType.OBJECT);
		rsCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.RESULTSET_CACHE+DESC));
		rsCacheNode.get(REQUIRED).set(false);
		describeResultsetcache(rsCacheNode, ATTRIBUTES, bundle);
		
		// preparedplan-set-cache
		ModelNode preparedPlanCacheNode = node.get(CHILDREN, Configuration.PREPAREDPLAN_CACHE);
		preparedPlanCacheNode.get(TYPE).set(ModelType.OBJECT);
		preparedPlanCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.PREPAREDPLAN_CACHE+DESC));
		preparedPlanCacheNode.get(REQUIRED).set(false);
		describePreparedPlanCache(preparedPlanCacheNode, ATTRIBUTES, bundle);
		
		//distributed-cache
		ModelNode distributedCacheNode = node.get(CHILDREN, Configuration.CACHE_FACORY);
		distributedCacheNode.get(TYPE).set(ModelType.OBJECT);
		distributedCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.CACHE_FACORY+DESC));
		distributedCacheNode.get(REQUIRED).set(false);
		describeDistributedCache(preparedPlanCacheNode, ATTRIBUTES, bundle);
	}

	@Override
	protected void populateModel(ModelNode operation, ModelNode model)	throws OperationFailedException {
		if (operation.hasDefined(Configuration.ALLOW_ENV_FUNCTION)) {
			model.get(Configuration.ALLOW_ENV_FUNCTION).set(operation.get(Configuration.ALLOW_ENV_FUNCTION).asString());
		}
		if (operation.hasDefined(Configuration.ASYNC_THREAD_GROUP)) {
			model.get(Configuration.ASYNC_THREAD_GROUP).set(operation.get(Configuration.ASYNC_THREAD_GROUP).asString());
		}
		populateBufferManager(operation, model);
		//TODO: add cache model descriptions
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
		SystemFunctionManager systemFunctionManager = null;
		try {
			systemFunctionManager = new SystemFunctionManager();
			if (operation.hasDefined(Configuration.ALLOW_ENV_FUNCTION)) {
				systemFunctionManager.setAllowEnvFunction(operation.get(Configuration.ALLOW_ENV_FUNCTION).asBoolean());
			}
			else {
				systemFunctionManager.setAllowEnvFunction(false);
			}
			systemFunctionManager.setClassloader(Module.getCallerModule().getModule(ModuleIdentifier.create("org.jboss.teiid")).getClassLoader()); //$NON-NLS-1$
		} catch (ModuleLoadException e) {
			throw new OperationFailedException(e, operation);
		}
    	
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
    	final BufferServiceImpl bufferManager = buildBufferManager(operation.get(Configuration.BUFFER_SERVICE));
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
    	
    	//cache factory
    	final CacheFactory cacheFactory = getCacheFactory(operation.get(Configuration.CACHE_FACORY));
    	ValueService<CacheFactory> cacheFactoryService = new ValueService<CacheFactory>(new org.jboss.msc.value.Value<CacheFactory>() {
			@Override
			public CacheFactory getValue() throws IllegalStateException, IllegalArgumentException {
				return cacheFactory;
			}
    	});
    	newControllers.add(target.addService(TeiidServiceNames.CACHE_FACTORY, cacheFactoryService).install());
    	
    	CacheConfiguration resultsetCache = buildCacheConfig(operation.get(Configuration.RESULTSET_CACHE));
    	CacheConfiguration preparePlanCache = buildCacheConfig(operation.get(Configuration.PREPAREDPLAN_CACHE));
    	
    	
    	// add translators
    	
    	
    	// Register VDB deployer
        context.addStep(new AbstractDeploymentChainStep() {
			@Override
			public void execute(DeploymentProcessorTarget processorTarget) {
				processorTarget.addDeploymentProcessor(Phase.STRUCTURE, Phase.STRUCTURE_WAR_DEPLOYMENT_INIT|0x0001,new VDBStructure());
				processorTarget.addDeploymentProcessor(Phase.PARSE, Phase.PARSE_WEB_DEPLOYMENT|0x0001, new VDBParserDeployer());
				processorTarget.addDeploymentProcessor(Phase.DEPENDENCIES, Phase.DEPENDENCIES_WAR_MODULE|0x0001, new VDBDependencyProcessor());
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
	
	
	static void describeBufferManager(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.USE_DISK, type, bundle.getString(Configuration.USE_DISK+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.PROCESSOR_BATCH_SIZE, type, bundle.getString(Configuration.PROCESSOR_BATCH_SIZE+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, Configuration.CONNECTOR_BATCH_SIZE, type, bundle.getString(Configuration.CONNECTOR_BATCH_SIZE+DESC), ModelType.INT, false, "1024"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_PROCESSING_KB, type, bundle.getString(Configuration.MAX_PROCESSING_KB+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_RESERVED_KB, type, bundle.getString(Configuration.MAX_RESERVED_KB+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_FILE_SIZE, type, bundle.getString(Configuration.MAX_FILE_SIZE+DESC), ModelType.LONG, false, "2048"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_BUFFER_SPACE, type, bundle.getString(Configuration.MAX_BUFFER_SPACE+DESC), ModelType.LONG, false, "51200"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_OPEN_FILES, type, bundle.getString(Configuration.MAX_OPEN_FILES+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
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
    	if (node.hasDefined(Configuration.MAX_PROCESSING_KB)) {
    		bufferManger.setMaxProcessingKb(node.get(Configuration.MAX_PROCESSING_KB).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_RESERVED_KB)) {
    		bufferManger.setMaxReserveKb(node.get(Configuration.MAX_RESERVED_KB).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_FILE_SIZE)) {
    		bufferManger.setMaxFileSize(node.get(Configuration.MAX_FILE_SIZE).asLong());
    	}
    	if (node.hasDefined(Configuration.MAX_BUFFER_SPACE)) {
    		bufferManger.setMaxBufferSpace(node.get(Configuration.MAX_BUFFER_SPACE).asLong());
    	}
    	if (node.hasDefined(Configuration.MAX_OPEN_FILES)) {
    		bufferManger.setMaxOpenFiles(node.get(Configuration.MAX_OPEN_FILES).asInt());
    	}	                	
    	return bufferManger;
    }	
    
    private void populateBufferManager(ModelNode operation, ModelNode model) {
    	
    	ModelNode childNode = operation.get(CHILDREN, Configuration.BUFFER_SERVICE);
    	if (!childNode.isDefined()) {
    		return;
    	}
		if (operation.hasDefined(Configuration.USE_DISK)) {
			model.get(Configuration.USE_DISK).set(operation.get(Configuration.USE_DISK).asString());
		}

		if (operation.hasDefined(Configuration.PROCESSOR_BATCH_SIZE)) {
			model.get(Configuration.PROCESSOR_BATCH_SIZE).set(operation.get(Configuration.PROCESSOR_BATCH_SIZE).asString());
		}
		if (operation.hasDefined(Configuration.CONNECTOR_BATCH_SIZE)) {
			model.get(Configuration.CONNECTOR_BATCH_SIZE).set(operation.get(Configuration.CONNECTOR_BATCH_SIZE).asString());
		}
		if (operation.hasDefined(Configuration.MAX_PROCESSING_KB)) {
			model.get(Configuration.MAX_PROCESSING_KB).set(operation.get(Configuration.MAX_PROCESSING_KB).asString());
		}
		if (operation.hasDefined(Configuration.MAX_RESERVED_KB)) {
			model.get(Configuration.MAX_RESERVED_KB).set(operation.get(Configuration.MAX_RESERVED_KB).asString());
		}
		if (operation.hasDefined(Configuration.MAX_FILE_SIZE)) {
			model.get(Configuration.MAX_FILE_SIZE).set(operation.get(Configuration.MAX_FILE_SIZE).asString());
		}
		if (operation.hasDefined(Configuration.MAX_BUFFER_SPACE)) {
			model.get(Configuration.MAX_BUFFER_SPACE).set(operation.get(Configuration.MAX_BUFFER_SPACE).asString());
		}
		if (operation.hasDefined(Configuration.MAX_BUFFER_SPACE)) {
			model.get(Configuration.MAX_BUFFER_SPACE).set(operation.get(Configuration.MAX_BUFFER_SPACE).asString());
		}
		if (operation.hasDefined(Configuration.MAX_OPEN_FILES)) {
			model.get(Configuration.MAX_OPEN_FILES).set(operation.get(Configuration.MAX_OPEN_FILES).asString());
		}
    }
    
	private static void describeDistributedCache(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.CACHE_SERVICE_JNDI_NAME, type, bundle.getString(Configuration.CACHE_SERVICE_JNDI_NAME+DESC), ModelType.STRING, false, "java:TeiidCacheManager"); //$NON-NLS-1$
		addAttribute(node, Configuration.RESULTSET_CACHE_NAME, type, bundle.getString(Configuration.RESULTSET_CACHE_NAME+DESC), ModelType.STRING, false, "teiid-resultset-cache"); //$NON-NLS-1$
	}
	
	private static void describeResultsetcache(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.MAX_ENTRIES, type, bundle.getString(Configuration.MAX_ENTRIES+DESC), ModelType.INT, false, "1024"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_AGE_IN_SECS, type, bundle.getString(Configuration.MAX_AGE_IN_SECS+DESC), ModelType.INT, false, "7200");//$NON-NLS-1$
		addAttribute(node, Configuration.MAX_STALENESS, type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "60");//$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_TYPE, type, bundle.getString(Configuration.CACHE_TYPE+DESC), ModelType.STRING, false, "EXPIRATION"); //$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_LOCATION, type, bundle.getString(Configuration.CACHE_LOCATION+DESC), ModelType.STRING, false, "resultset");	//$NON-NLS-1$	
	}
	
	private static void describePreparedPlanCache(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.MAX_ENTRIES, type, bundle.getString(Configuration.MAX_ENTRIES+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_AGE_IN_SECS, type, bundle.getString(Configuration.MAX_AGE_IN_SECS+DESC), ModelType.INT, false, "28800");//$NON-NLS-1$
		addAttribute(node, Configuration.MAX_STALENESS, type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "0");//$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_TYPE, type, bundle.getString(Configuration.CACHE_TYPE+DESC), ModelType.STRING, false, "LRU"); //$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_LOCATION, type, bundle.getString(Configuration.CACHE_LOCATION+DESC), ModelType.STRING, false, "preparedplan");	//$NON-NLS-1$	
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
    
}
