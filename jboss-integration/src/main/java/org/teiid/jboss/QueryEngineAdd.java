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

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.NamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.inject.ConcurrentMapInjector;
import org.jboss.msc.service.*;
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.SystemVDBDeployer;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.services.BufferServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;

class QueryEngineAdd extends AbstractAddStepHandler implements DescriptionProvider {

	@Override
	public ModelNode getModelDescription(Locale locale) {
		final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
		
        final ModelNode node = new ModelNode();
        node.get(OPERATION_NAME).set(ADD);
        node.get(DESCRIPTION).set("engine.add");  //$NON-NLS-1$
        
        describeQueryEngine(node, REQUEST_PROPERTIES, bundle);
        return node;
	}
	
	@Override
	protected void populateModel(ModelNode operation, ModelNode model) {
		populate(operation, model);
	}
	
	@Override
	protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {

    	ServiceTarget target = context.getServiceTarget();
    	
    	final JBossLifeCycleListener shutdownListener = new JBossLifeCycleListener();
       	
        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);
    	final String engineName = pathAddress.getLastElement().getValue();
    	
    	// now build the engine
    	final RuntimeEngineDeployer engine = buildQueryEngine(engineName, operation);
    	engine.setSecurityHelper(new JBossSecurityHelper());
    	
    	SocketConfiguration jdbc = buildSocketConfiguration(Configuration.JDBC, operation);
    	if (jdbc != null) {
    		engine.setJdbcSocketConfiguration(jdbc);
    	}
    	
    	SocketConfiguration odbc = buildSocketConfiguration(Configuration.ODBC, operation);
    	if (odbc != null) {
    		engine.setOdbcSocketConfiguration(odbc);
    	}    	
        
        ServiceBuilder<ClientServiceRegistry> engineBuilder = target.addService(TeiidServiceNames.engineServiceName(engine.getName()), engine);
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
        
        if (jdbc != null) {
        	engineBuilder.addDependency(ServiceName.JBOSS.append("binding", jdbc.getSocketBinding()), SocketBinding.class, engine.getJdbcSocketBindingInjector()); //$NON-NLS-1$
        }
        
        if (odbc != null) {
        	engineBuilder.addDependency(ServiceName.JBOSS.append("binding", odbc.getSocketBinding()), SocketBinding.class, engine.getOdbcSocketBindingInjector()); //$NON-NLS-1$
        }
              
        // register a JNDI name, this looks hard.
        final QueryEngineReferenceFactoryService referenceFactoryService = new QueryEngineReferenceFactoryService();
        final ServiceName referenceFactoryServiceName = TeiidServiceNames.engineServiceName(engine.getName()).append("reference-factory"); //$NON-NLS-1$
        final ServiceBuilder<?> referenceBuilder = target.addService(referenceFactoryServiceName,referenceFactoryService);
        referenceBuilder.addDependency(TeiidServiceNames.engineServiceName(engine.getName()), RuntimeEngineDeployer.class, referenceFactoryService.getQueryEngineInjector());
        referenceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        
        final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.TEIID_RUNTIME_CONTEXT+engine.getName());
        final BinderService engineBinderService = new BinderService(bindInfo.getBindName());
        final ServiceBuilder<?> engineBinderBuilder = target.addService(bindInfo.getBinderServiceName(), engineBinderService);
        engineBinderBuilder.addDependency(referenceFactoryServiceName, ManagedReferenceFactory.class, engineBinderService.getManagedObjectInjector());
        engineBinderBuilder.addDependency(bindInfo.getParentContextServiceName(), NamingStore.class, engineBinderService.getNamingStoreInjector());        
        engineBinderBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        
        // add security domains
        if ( operation.hasDefined(Configuration.SECURITY_DOMAIN)) {
	        List<ModelNode> domains = operation.get(Configuration.SECURITY_DOMAIN).asList();
	        for (ModelNode domain:domains) {
	        	LogManager.logInfo(LogConstants.CTX_SECURITY, "Security Enabled: true"); //$NON-NLS-1$
	            engineBuilder.addDependency(ServiceName.JBOSS.append("security", "security-domain", domain.asString()), SecurityDomainContext.class, new ConcurrentMapInjector<String,SecurityDomainContext>(engine.securityDomains, domain.asString())); //$NON-NLS-1$ //$NON-NLS-2$
	        }
        }
                  
        engineBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        ServiceController<ClientServiceRegistry> controller = engineBuilder.install(); 
        newControllers.add(controller);
        ServiceContainer container =  controller.getServiceContainer();
        container.addTerminateListener(shutdownListener);
        
        newControllers.add(referenceBuilder.install());
        newControllers.add(engineBinderBuilder.install());
    }

	
	private RuntimeEngineDeployer buildQueryEngine(String engineName, ModelNode node) {
		RuntimeEngineDeployer engine = new RuntimeEngineDeployer(engineName);
    	
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
    	if (node.hasDefined(Configuration.DETECTING_CHANGE_EVENTS)) {
    		engine.setDetectingChangeEvents(node.get(Configuration.DETECTING_CHANGE_EVENTS).asBoolean());
    	}	             
    	if (node.hasDefined(Configuration.SESSION_EXPIRATION_TIME_LIMIT)) {
    		engine.setSessionExpirationTimeLimit(node.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT).asInt());
    	}
    	if (node.hasDefined(Configuration.MAX_SESSIONS_ALLOWED)) {
    		engine.setSessionMaxLimit(node.get(Configuration.MAX_SESSIONS_ALLOWED).asInt());
    	}
    	if (node.hasDefined(Configuration.SECURITY_DOMAIN)) {
    		List<ModelNode> securityDomains = node.get(Configuration.SECURITY_DOMAIN).asList();
    		for (ModelNode domain:securityDomains) {
    			engine.addSecurityDomain(domain.asString());	
    		}
    	}	    	
		return engine;
	}
        
    
	private SocketConfiguration buildSocketConfiguration(String prefix, ModelNode node) {
		
		if (!node.hasDefined(prefix+Configuration.SOCKET_BINDING)) {
			return null;
		}
		
		SocketConfiguration socket = new SocketConfiguration();
		socket.setSocketBinding(node.require(prefix+Configuration.SOCKET_BINDING).asString());

   		if (node.hasDefined(prefix+Configuration.MAX_SOCKET_THREAD_SIZE)) {
    		socket.setMaxSocketThreads(node.get(prefix+Configuration.MAX_SOCKET_THREAD_SIZE).asInt());
    	}
    	if (node.hasDefined(prefix+Configuration.IN_BUFFER_SIZE)) {
    		socket.setInputBufferSize(node.get(prefix+Configuration.IN_BUFFER_SIZE).asInt());
    	}	
    	if (node.hasDefined(prefix+Configuration.OUT_BUFFER_SIZE)) {
    		socket.setOutputBufferSize(node.get(prefix+Configuration.OUT_BUFFER_SIZE).asInt());
    	}		   
    	
    	SSLConfiguration ssl = new SSLConfiguration();
    	ssl.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
    	
    	String sslPrefix = prefix+ Configuration.SSL +TeiidBootServicesAdd.DASH;
    		
    	if (node.hasDefined(sslPrefix+Configuration.SSL_MODE)) {
    		ssl.setMode(node.get(sslPrefix+Configuration.SSL_MODE).asString());
    	}
    	
    	if (node.hasDefined(sslPrefix+Configuration.KEY_STORE_FILE)) {
    		ssl.setKeystoreFilename(node.get(sslPrefix+Configuration.KEY_STORE_FILE).asString());
    	}	
    	
    	if (node.hasDefined(sslPrefix+Configuration.KEY_STORE_PASSWD)) {
    		ssl.setKeystorePassword(node.get(sslPrefix+Configuration.KEY_STORE_PASSWD).asString());
    	}	
    	
    	if (node.hasDefined(sslPrefix+Configuration.KEY_STORE_TYPE)) {
    		ssl.setKeystoreType(node.get(sslPrefix+Configuration.KEY_STORE_TYPE).asString());
    	}		
    	
    	if (node.hasDefined(sslPrefix+Configuration.SSL_PROTOCOL)) {
    		ssl.setSslProtocol(node.get(sslPrefix+Configuration.SSL_PROTOCOL).asString());
    	}	
    	if (node.hasDefined(sslPrefix+Configuration.KEY_MANAGEMENT_ALG)) {
    		ssl.setKeymanagementAlgorithm(node.get(sslPrefix+Configuration.KEY_MANAGEMENT_ALG).asString());
    	}
    	if (node.hasDefined(sslPrefix+Configuration.TRUST_FILE)) {
    		ssl.setTruststoreFilename(node.get(sslPrefix+Configuration.TRUST_FILE).asString());
    	}
    	if (node.hasDefined(sslPrefix+Configuration.TRUST_PASSWD)) {
    		ssl.setTruststorePassword(node.get(sslPrefix+Configuration.TRUST_PASSWD).asString());
    	}
    	if (node.hasDefined(sslPrefix+Configuration.AUTH_MODE)) {
    		ssl.setAuthenticationMode(node.get(sslPrefix+Configuration.AUTH_MODE).asString());
    	}
    	socket.setSSLConfiguration(ssl);
    	
		return socket;
	}	
	
	static void describeQueryEngine(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.MAX_THREADS, type, bundle.getString(Configuration.MAX_THREADS+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ACTIVE_PLANS, type, bundle.getString(Configuration.MAX_ACTIVE_PLANS+DESC), ModelType.INT, false, "20"); //$NON-NLS-1$
		addAttribute(node, Configuration.USER_REQUEST_SOURCE_CONCURRENCY, type, bundle.getString(Configuration.USER_REQUEST_SOURCE_CONCURRENCY+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.TIME_SLICE_IN_MILLI, type, bundle.getString(Configuration.TIME_SLICE_IN_MILLI+DESC), ModelType.INT, false, "2000"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_ROWS_FETCH_SIZE, type, bundle.getString(Configuration.MAX_ROWS_FETCH_SIZE+DESC), ModelType.INT, false, "20480"); //$NON-NLS-1$
		addAttribute(node, Configuration.LOB_CHUNK_SIZE_IN_KB, type, bundle.getString(Configuration.LOB_CHUNK_SIZE_IN_KB+DESC), ModelType.INT, false, "100"); //$NON-NLS-1$
		addAttribute(node, Configuration.QUERY_THRESHOLD_IN_SECS, type, bundle.getString(Configuration.QUERY_THRESHOLD_IN_SECS+DESC), ModelType.INT, false, "600"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_SOURCE_ROWS, type, bundle.getString(Configuration.MAX_SOURCE_ROWS+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS, type, bundle.getString(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ODBC_LOB_SIZE_ALLOWED, type, bundle.getString(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED+DESC), ModelType.INT, false, "5242880"); //$NON-NLS-1$		
		addAttribute(node, Configuration.DETECTING_CHANGE_EVENTS, type, bundle.getString(Configuration.DETECTING_CHANGE_EVENTS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		
		//session stuff
		addAttribute(node, Configuration.SECURITY_DOMAIN, type, bundle.getString(Configuration.SECURITY_DOMAIN+DESC), ModelType.LIST, false, null);
		addAttribute(node, Configuration.MAX_SESSIONS_ALLOWED, type, bundle.getString(Configuration.MAX_SESSIONS_ALLOWED+DESC), ModelType.INT, false, "5000"); //$NON-NLS-1$
		addAttribute(node, Configuration.SESSION_EXPIRATION_TIME_LIMIT, type, bundle.getString(Configuration.SESSION_EXPIRATION_TIME_LIMIT+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		
		//jdbc
		describeSocketConfig(Configuration.JDBC+TeiidBootServicesAdd.DASH, node, type, bundle);
		
		//odbc
		describeSocketConfig(Configuration.ODBC+TeiidBootServicesAdd.DASH, node, type, bundle);			
	}
	
	
	private static void describeSocketConfig(String prefix, ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, prefix+Configuration.MAX_SOCKET_THREAD_SIZE, type, bundle.getString(Configuration.MAX_SOCKET_THREAD_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, prefix+Configuration.IN_BUFFER_SIZE, type, bundle.getString(Configuration.IN_BUFFER_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, prefix+Configuration.OUT_BUFFER_SIZE, type, bundle.getString(Configuration.OUT_BUFFER_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, prefix+Configuration.SOCKET_BINDING, type, bundle.getString(Configuration.SOCKET_BINDING+DESC), ModelType.STRING, false, null);
		
		String sslPrefix = prefix+Configuration.SSL+TeiidBootServicesAdd.DASH;

		addAttribute(node, sslPrefix+Configuration.SSL_MODE, type, bundle.getString(Configuration.SSL_MODE+DESC), ModelType.STRING, false, "login");	//$NON-NLS-1$
		addAttribute(node, sslPrefix+Configuration.KEY_STORE_FILE, type, bundle.getString(Configuration.KEY_STORE_FILE+DESC), ModelType.STRING, false, null);	
		addAttribute(node, sslPrefix+Configuration.KEY_STORE_PASSWD, type, bundle.getString(Configuration.KEY_STORE_PASSWD+DESC), ModelType.STRING, false, null);
		addAttribute(node, sslPrefix+Configuration.KEY_STORE_TYPE, type, bundle.getString(Configuration.KEY_STORE_TYPE+DESC), ModelType.STRING, false, "JKS"); //$NON-NLS-1$
		addAttribute(node, sslPrefix+Configuration.SSL_PROTOCOL, type, bundle.getString(Configuration.SSL_PROTOCOL+DESC), ModelType.STRING, false, "SSLv3");	//$NON-NLS-1$
		addAttribute(node, sslPrefix+Configuration.KEY_MANAGEMENT_ALG, type, bundle.getString(Configuration.KEY_MANAGEMENT_ALG+DESC), ModelType.STRING, false, null);
		addAttribute(node, sslPrefix+Configuration.TRUST_FILE, type, bundle.getString(Configuration.TRUST_FILE+DESC), ModelType.STRING, false, null);	
		addAttribute(node, sslPrefix+Configuration.TRUST_PASSWD, type, bundle.getString(Configuration.TRUST_PASSWD+DESC), ModelType.STRING, false, null);	
		addAttribute(node, sslPrefix+Configuration.AUTH_MODE, type, bundle.getString(Configuration.AUTH_MODE+DESC), ModelType.STRING, false, "anonymous");	//$NON-NLS-1$
	}	
	
	static void populate(ModelNode operation, ModelNode model) {
		//model.get(Configuration.ENGINE_NAME).set(engineName);
		
		if (operation.hasDefined(Configuration.MAX_THREADS)) {
    		model.get(Configuration.MAX_THREADS).set(operation.get(Configuration.MAX_THREADS).asInt());
    	}
    	if (operation.hasDefined(Configuration.MAX_ACTIVE_PLANS)) {
    		model.get(Configuration.MAX_ACTIVE_PLANS).set(operation.get(Configuration.MAX_ACTIVE_PLANS).asInt());
    	}
    	if (operation.hasDefined(Configuration.USER_REQUEST_SOURCE_CONCURRENCY)) {
    		model.get(Configuration.USER_REQUEST_SOURCE_CONCURRENCY).set(operation.get(Configuration.USER_REQUEST_SOURCE_CONCURRENCY).asInt());
    	}	
    	if (operation.hasDefined(Configuration.TIME_SLICE_IN_MILLI)) {
    		model.get(Configuration.TIME_SLICE_IN_MILLI).set(operation.get(Configuration.TIME_SLICE_IN_MILLI).asInt());
    	}
    	if (operation.hasDefined(Configuration.MAX_ROWS_FETCH_SIZE)) {
    		model.get(Configuration.MAX_ROWS_FETCH_SIZE).set(operation.get(Configuration.MAX_ROWS_FETCH_SIZE).asInt());
    	}
    	if (operation.hasDefined(Configuration.LOB_CHUNK_SIZE_IN_KB)) {
    		model.get(Configuration.LOB_CHUNK_SIZE_IN_KB).set(operation.get(Configuration.LOB_CHUNK_SIZE_IN_KB).asInt());
    	}
    	if (operation.hasDefined(Configuration.QUERY_THRESHOLD_IN_SECS)) {
    		model.get(Configuration.QUERY_THRESHOLD_IN_SECS).set(operation.get(Configuration.QUERY_THRESHOLD_IN_SECS).asInt());
    	}
    	if (operation.hasDefined(Configuration.MAX_SOURCE_ROWS)) {
    		model.get(Configuration.MAX_SOURCE_ROWS).set(operation.get(Configuration.MAX_SOURCE_ROWS).asInt());
    	}
    	if (operation.hasDefined(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS)) {
    		model.get(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS).set(operation.get(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS).asBoolean());
    	}
    	if (operation.hasDefined(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED)) {
    		model.get(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED).set(operation.get(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED).asInt());
    	}
    	if (operation.hasDefined(Configuration.SECURITY_DOMAIN)) {
    		List<ModelNode> domains = operation.get(Configuration.SECURITY_DOMAIN).asList();
    		for (ModelNode domain: domains) {
    			model.get(Configuration.SECURITY_DOMAIN).add(domain.asString());	
    		}
    	}
    	if (operation.hasDefined(Configuration.DETECTING_CHANGE_EVENTS)) {
    		model.get(Configuration.DETECTING_CHANGE_EVENTS).set(operation.get(Configuration.DETECTING_CHANGE_EVENTS).asBoolean());
    	}	             
    	if (operation.hasDefined(Configuration.SESSION_EXPIRATION_TIME_LIMIT)) {
    		model.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT).set(operation.get(Configuration.SESSION_EXPIRATION_TIME_LIMIT).asInt());
    	}
    	if (operation.hasDefined(Configuration.MAX_SESSIONS_ALLOWED)) {
    		model.get(Configuration.MAX_SESSIONS_ALLOWED).set(operation.get(Configuration.MAX_SESSIONS_ALLOWED).asInt());
    	}		 
    	
		populateSocketConfiguration(Configuration.JDBC+TeiidBootServicesAdd.DASH, operation, model);
	
		populateSocketConfiguration(Configuration.ODBC+TeiidBootServicesAdd.DASH, operation, model);
	}

	private static void populateSocketConfiguration(String prefix, ModelNode operation, ModelNode model) {
		if (operation.hasDefined(prefix+Configuration.SOCKET_BINDING)) {
			model.get(prefix+Configuration.SOCKET_BINDING).set(operation.get(prefix+Configuration.SOCKET_BINDING).asString());
		}
   		if (operation.hasDefined(prefix+Configuration.MAX_SOCKET_THREAD_SIZE)) {
    		model.get(prefix+Configuration.MAX_SOCKET_THREAD_SIZE).set(operation.get(prefix+Configuration.MAX_SOCKET_THREAD_SIZE).asInt());
    	}
    	if (operation.hasDefined(prefix+Configuration.IN_BUFFER_SIZE)) {
    		model.get(prefix+Configuration.IN_BUFFER_SIZE).set(operation.get(prefix+Configuration.IN_BUFFER_SIZE).asInt());
    	}	
    	if (operation.hasDefined(prefix+Configuration.OUT_BUFFER_SIZE)) {
    		model.get(prefix+Configuration.OUT_BUFFER_SIZE).set(operation.get(prefix+Configuration.OUT_BUFFER_SIZE).asInt());
    	}		   
    	
    	String sslPrefix = prefix+Configuration.SSL+TeiidBootServicesAdd.DASH;
    	
    	if (operation.hasDefined(sslPrefix+Configuration.SSL_MODE)) {
    		model.get(sslPrefix+Configuration.SSL_MODE).set(operation.get(sslPrefix+Configuration.SSL_MODE).asString());
    	}
    	
    	if (operation.hasDefined(sslPrefix+Configuration.KEY_STORE_FILE)) {
    		model.get(sslPrefix+Configuration.KEY_STORE_FILE).set(operation.get(sslPrefix+Configuration.KEY_STORE_FILE).asString());
    	}	
    	
    	if (operation.hasDefined(sslPrefix+Configuration.KEY_STORE_PASSWD)) {
    		model.get(sslPrefix+Configuration.KEY_STORE_PASSWD).set(operation.get(sslPrefix+Configuration.KEY_STORE_PASSWD).asString());
    	}	
    	
    	if (operation.hasDefined(sslPrefix+Configuration.KEY_STORE_TYPE)) {
    		model.get(sslPrefix+Configuration.KEY_STORE_TYPE).set(operation.get(sslPrefix+Configuration.KEY_STORE_TYPE).asString());
    	}		
    	
    	if (operation.hasDefined(sslPrefix+Configuration.SSL_PROTOCOL)) {
    		model.get(sslPrefix+Configuration.SSL_PROTOCOL).set(operation.get(sslPrefix+Configuration.SSL_PROTOCOL).asString());
    	}	
    	if (operation.hasDefined(sslPrefix+Configuration.KEY_MANAGEMENT_ALG)) {
    		model.get(sslPrefix+Configuration.KEY_MANAGEMENT_ALG).set(operation.get(sslPrefix+Configuration.KEY_MANAGEMENT_ALG).asString());
    	}
    	if (operation.hasDefined(sslPrefix+Configuration.TRUST_FILE)) {
    		model.get(sslPrefix+Configuration.TRUST_FILE).set(operation.get(sslPrefix+Configuration.TRUST_FILE).asString());
    	}
    	if (operation.hasDefined(sslPrefix+Configuration.TRUST_PASSWD)) {
    		model.get(sslPrefix+Configuration.TRUST_PASSWD).set(operation.get(sslPrefix+Configuration.TRUST_PASSWD).asString());
    	}
    	if (operation.hasDefined(sslPrefix+Configuration.AUTH_MODE)) {
    		model.get(sslPrefix+Configuration.AUTH_MODE).set(operation.get(sslPrefix+Configuration.AUTH_MODE).asString());
    	}
	}	
}
