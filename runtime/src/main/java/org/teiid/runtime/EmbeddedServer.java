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

package org.teiid.runtime;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.TransactionManager;
import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.BundleUtil.Event;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.deployers.CompositeGlobalTableStore;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.BufferService;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidPreparedStatement;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.index.IndexMetadataRepository;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.PureZipFileSystem;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.services.AbstractEventDistributorFactoryService;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.transport.*;
import org.teiid.vdb.runtime.VDBKey;
import org.xml.sax.SAXException;

/**
 * A simplified server environment for embedded use.
 * 
 * Needs to be started prior to use with a call to {@link #start(EmbeddedConfiguration)}
 */
@SuppressWarnings("serial")
public class EmbeddedServer extends AbstractVDBDeployer implements EventDistributorFactory, ExecutionFactoryProvider {

	private EmbeddedProfile embeddedProfile = new EmbeddedProfile() {
		@Override
		public ConnectionImpl connect(String url, Properties info)
				throws TeiidSQLException {
			ServerConnection conn;
			try {
				conn = createServerConnection(info);
			} catch (TeiidException e) {
				throw TeiidSQLException.create(e);
			}
			return new EmbeddedConnectionImpl(conn, info, url);
		}

		@Override
		public ServerConnection createServerConnection(Properties info)
				throws TeiidException {
			LocalServerConnection conn = new LocalServerConnection(info, useCallingThread) {
				@Override
				protected ClientServiceRegistry getClientServiceRegistry(String name) {
					return services;
				}
				
				@Override
				public void addListener(VDBLifeCycleListener listener) {
					EmbeddedServer.this.repo.addListener(listener);
				}
				@Override
				public void removeListener(VDBLifeCycleListener listener) {
					EmbeddedServer.this.repo.removeListener(listener);
				}
			};
			conn.getWorkContext().setConnectionProfile(this);
			return conn;
		}
	};

	private final class EmbeddedConnectionImpl extends ConnectionImpl implements EmbeddedConnection {

		public EmbeddedConnectionImpl(ServerConnection serverConn,
				Properties info, String url) {
			super(serverConn, info, url);
		}

		@Override
		public CallableStatement prepareCall(Command command, EmbeddedRequestOptions options)
				throws SQLException {
			CallableStatementImpl csi = this.prepareCall(command.toString(), options.getResultSetType(), ResultSet.CONCUR_READ_ONLY);
			csi.setCommand(command);
			return csi;
		}
		
		@Override
		public TeiidPreparedStatement prepareStatement(Command command,
				EmbeddedRequestOptions options) throws SQLException {
			PreparedStatementImpl psi = this.prepareStatement(command.toString(), options.getResultSetType(), ResultSet.CONCUR_READ_ONLY);
			psi.setCommand(command);
			return psi;
		}
		
	}

	protected class ProviderAwareConnectorManagerRepository extends
			ConnectorManagerRepository {
		
		public ProviderAwareConnectorManagerRepository() {
			super(true);
		}
		
		@Override
		protected ConnectorManager createConnectorManager(
				String translatorName, String connectionName, ExecutionFactory<Object, Object> ef) {
			return new ConnectorManager(translatorName, connectionName, ef) {
				@Override
				public Object getConnectionFactory() throws TranslatorException {
					if (getConnectionName() == null) {
						return null;
					}
					ConnectionFactoryProvider<?> connectionFactoryProvider = connectionFactoryProviders.get(getConnectionName());
					if (connectionFactoryProvider != null) {
						return connectionFactoryProvider.getConnectionFactory();
					}
					return super.getConnectionFactory();
				}
			};
		}
	}

	public interface ConnectionFactoryProvider<T> {
		T getConnectionFactory() throws TranslatorException;
	}
	
	public static class SimpleConnectionFactoryProvider<T> implements ConnectionFactoryProvider<T> {
		
		private T connectionFactory;
		
		public SimpleConnectionFactoryProvider(T connectionFactory) {
			this.connectionFactory = connectionFactory;
		}

		@Override
		public T getConnectionFactory() throws TranslatorException {
			return connectionFactory;
		}
		
	}
	
	private static class VDBValidationError extends TeiidRuntimeException {
		
		private VDBValidationError(Event event, String message) {
			super(event, message);
		}
	}
	
	protected DQPCore dqp = new DQPCore();
	/**
	 * Custom vdb repository that will immediately throw exceptions for metadata validation errors
	 */
	protected VDBRepository repo = new VDBRepository() {
		@Override
		protected boolean processMetadataValidatorReport(VDBKey key, ValidatorReport report) {
			if (throwMetadataErrors) {
				super.processMetadataValidatorReport(key, report); //remove
				ValidatorFailure firstFailure = report.getItems().iterator().next();
				throw new VDBValidationError(RuntimePlugin.Event.TEIID40095, firstFailure.getMessage());
			}
			return true;
		}
	};
	protected boolean throwMetadataErrors = true;
	private ConcurrentHashMap<String, ExecutionFactory<?, ?>> translators = new ConcurrentHashMap<String, ExecutionFactory<?, ?>>();
	private ConcurrentHashMap<String, ConnectionFactoryProvider<?>> connectionFactoryProviders = new ConcurrentHashMap<String, ConnectionFactoryProvider<?>>();
	protected SessionServiceImpl sessionService = new SessionServiceImpl();
	protected ObjectReplicator replicator;
	protected BufferServiceImpl bufferService = new BufferServiceImpl();
	protected TransactionServerImpl transactionService = new TransactionServerImpl();
	protected boolean waitForLoad;
	protected ClientServiceRegistryImpl services = new ClientServiceRegistryImpl() {
		@Override
		public void waitForFinished(String vdbName, int vdbVersion, int timeOutMillis) throws ConnectionException {
			if (waitForLoad) {
				repo.waitForFinished(vdbName, vdbVersion, timeOutMillis);
			}
		}
		@Override
		public ClassLoader getCallerClassloader() {
			return this.getClass().getClassLoader();
		};
	};
	protected LogonImpl logon;
	private TeiidDriver driver = new TeiidDriver();
	protected ConnectorManagerRepository cmr = new ProviderAwareConnectorManagerRepository();
	
	protected AbstractEventDistributorFactoryService eventDistributorFactoryService = new AbstractEventDistributorFactoryService() {
		
		@Override
		protected VDBRepository getVdbRepository() {
			return repo;
		}
		
		@Override
		protected ObjectReplicator getObjectReplicator() {
			return replicator;
		}
	};
	protected boolean useCallingThread = true;
	private Boolean running;
	private EmbeddedConfiguration config;
	private SessionAwareCache<CachedResults> rs;
	private SessionAwareCache<PreparedPlan> ppc;
	protected ArrayList<SocketListener> transports = new ArrayList<SocketListener>();
	
	public EmbeddedServer() {

	}

	/**
	 * Adds the {@link ConnectionFactoryProvider} with the given connection name to replace the default JNDI lookup strategy.
	 * @param name
	 * @param connectionFactoryProvider
	 * @see SimpleConnectionFactoryProvider for a basic wrapper
	 */
	public void addConnectionFactoryProvider(String name,
			ConnectionFactoryProvider<?> connectionFactoryProvider) {
		this.connectionFactoryProviders.put(name, connectionFactoryProvider);
	}
	
	/**
	 * Adds the object as the named connection factory to replace the default JNDI lookup strategy.
	 * @param name
	 * @param connectionFactoryr
	 */
	public void addConnectionFactory(String name, Object connectionFactory) {
		this.connectionFactoryProviders.put(name, new SimpleConnectionFactoryProvider<Object>(connectionFactory));
	}
	
	public synchronized void start(@SuppressWarnings("hiding") EmbeddedConfiguration config) {
		if (running != null) {
			throw new IllegalStateException();
		}
		this.config = config;
		this.eventDistributorFactoryService.start();
		this.dqp.setEventDistributor(this.eventDistributorFactoryService.getReplicatedEventDistributor());
		this.replicator = config.getObjectReplicator();
		if (config.getTransactionManager() == null) {
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
			this.transactionService.setTransactionManager((TransactionManager) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {TransactionManager.class}, new InvocationHandler() {
				
				@Override
				public Object invoke(Object proxy, Method method, Object[] args)
						throws Throwable {
					throw new UnsupportedOperationException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
				}
			}));
		} else {
			this.transactionService.setDetectTransactions(true);
			this.transactionService.setTransactionManager(config.getTransactionManager());
		}
		if (config.getSecurityHelper() != null) {
			this.sessionService.setSecurityHelper(config.getSecurityHelper());
		} else {
			this.sessionService.setSecurityHelper(new DoNothingSecurityHelper());
		}
		if (config.getSecurityDomain() != null) {
			this.sessionService.setSecurityDomain(config.getSecurityDomain());
		} else {
			this.sessionService.setSecurityDomain("teiid-security"); //$NON-NLS-1$
		}

		this.sessionService.setVDBRepository(repo);
		this.bufferService.setUseDisk(config.isUseDisk());
		if (config.isUseDisk()) {
			if (config.getBufferDirectory() == null) {
				config.setBufferDirectory(System.getProperty("java.io.tmpdir")); //$NON-NLS-1$
			}
			this.bufferService.setDiskDirectory(config.getBufferDirectory());
		}
		BufferService bs = getBufferService();
		this.dqp.setBufferManager(bs.getBufferManager());

		startVDBRepository();

		rs = new SessionAwareCache<CachedResults>("resultset", config.getCacheFactory(), SessionAwareCache.Type.RESULTSET, config.getMaxResultSetCacheStaleness()); //$NON-NLS-1$
		ppc = new SessionAwareCache<PreparedPlan>("preparedplan", config.getCacheFactory(), SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$
		rs.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setResultsetCache(rs);

		ppc.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setPreparedPlanCache(ppc);

		this.dqp.setTransactionService(this.transactionService);

		this.dqp.start(config);
		this.sessionService.setDqp(this.dqp);
		this.services.setSecurityHelper(this.sessionService.getSecurityHelper());
		this.services.setVDBRepository(this.repo);
		this.logon = new LogonImpl(sessionService, null);
		services.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
		services.registerClientService(DQP.class, dqp, LogConstants.CTX_DQP);
		initDriver();
		List<SocketConfiguration> transports = config.getTransports();
		if ( transports != null && !transports.isEmpty()) {
			for (SocketConfiguration socketConfig:transports) {
				SocketListener socketConnection = startTransport(socketConfig, bs.getBufferManager(), config.getMaxODBCLobSizeAllowed());
				if (socketConnection != null) {
					this.transports.add(socketConnection);
				}
			}
		}
		running = true;
	}

	private void initDriver() {
		driver.setEmbeddedProfile(embeddedProfile);
	}
	
	private SocketListener startTransport(SocketConfiguration socketConfig, BufferManager bm, int maxODBCLobSize) {
		InetSocketAddress address = null;
		try {
			address = new InetSocketAddress(socketConfig.getResolvedHostAddress(), socketConfig.getPortNumber());
		} catch (UnknownHostException e) {
			throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40065, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40065));
		}
		if (socketConfig.getProtocol() == WireProtocol.teiid) {
			return new SocketListener(address, socketConfig, this.services, bm) {
				@Override
				public ChannelListener createChannelListener(ObjectChannel channel) {
					//TODO: this is a little dirty, but allows us to inject the appropriate connection profile
					SocketClientInstance instance = (SocketClientInstance) super.createChannelListener(channel);
					instance.getWorkContext().setConnectionProfile(embeddedProfile);
					return instance;
				}
			};
		}
		else if (socketConfig.getProtocol() == WireProtocol.pg) {
    		this.repo.odbcEnabled();
    		ODBCSocketListener odbc = new ODBCSocketListener(address, socketConfig, this.services, bm, maxODBCLobSize, this.logon, driver);
    		return odbc;
		}
		return null;
	}

	private void startVDBRepository() {
		this.repo.addListener(new VDBLifeCycleListener() {

			@Override
			public void added(String name, int version, CompositeVDB vdb, boolean reloading) {

			}

			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				if (replicator != null) {
					replicator.stop(vdb.getVDB().getAttachment(GlobalTableStore.class));
				}
				rs.clearForVDB(name, 1);
				ppc.clearForVDB(name, 1);
			}

			@Override
			public void finishedDeployment(String name, int version, CompositeVDB vdb, boolean reloading) {
				if (!vdb.getVDB().getStatus().equals(Status.ACTIVE)) {
					return;
				}
				GlobalTableStore gts = CompositeGlobalTableStore.createInstance(vdb, dqp.getBufferManager(), replicator);
				
				vdb.getVDB().addAttchment(GlobalTableStore.class, gts);
			}

			@Override
			public void beforeRemove(String name, int version, CompositeVDB vdb) {
			}
		});
		this.repo.setSystemFunctionManager(new SystemFunctionManager());
		this.repo.start();
	}

	protected BufferService getBufferService() {
		bufferService.start();
		if (replicator != null) {
			try {
				final TupleBufferCache tbc = replicator.replicate("$BM$", TupleBufferCache.class, bufferService.getBufferManager(), 0); //$NON-NLS-1$
				return new BufferService() {

					@Override
					public BufferManager getBufferManager() {
						return bufferService.getBufferManager();
					}

					@Override
					public TupleBufferCache getTupleBufferCache() {
						return tbc;
					}

				};
			} catch (Exception e) {
				throw new TeiidRuntimeException(e);
			}
		}
		return bufferService;
	}

	/**
	 * Adds a default instance of the {@link ExecutionFactory} using the default name either from the {@link Translator} annotation or the class name.  
	 * @param ef
	 * @throws TranslatorException 
	 */
	public void addTranslator(Class<? extends ExecutionFactory> clazz) throws TranslatorException {
		Translator t = clazz.getAnnotation(Translator.class);
		String name = clazz.getName();
		if (t != null) {
			name = t.name();
		}
		try {
			ExecutionFactory<?, ?> instance = clazz.newInstance();
			instance.start();
			addTranslator(name, instance);
		} catch (InstantiationException e) {
			throw new TeiidRuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	/**
	 * Add an {@link ExecutionFactory} using the default name either from the {@link Translator} annotation or the class name.
	 * @param ef the already started ExecutionFactory
	 * @deprecated
	 * @see {@link #addTranslator(String, ExecutionFactory)} or {@link #addTranslator(Class)}
	 * if the translator has overrides or multiple translators of a given type are needed.
	 */
	@Deprecated
	public void addTranslator(ExecutionFactory<?, ?> ef) {
		Translator t = ef.getClass().getAnnotation(Translator.class);
		String name = ef.getClass().getName();
		if (t != null) {
			name = t.name();
		}
		addTranslator(name, ef);
	}
	
	/**
	 * Add a named {@link ExecutionFactory}.
	 * @param name
	 * @param ef the already started ExecutionFactory
	 */
	public void addTranslator(String name, ExecutionFactory<?, ?> ef) {
		translators.put(name, ef);
	}

	/**
	 * Deploy the given set of models as vdb name.1
	 * @param name
	 * @param models
	 * @throws ConnectorManagerException
	 * @throws VirtualDatabaseException
	 * @throws TranslatorException
	 */
	public void deployVDB(String name, ModelMetaData... models)
			throws ConnectorManagerException, VirtualDatabaseException, TranslatorException {
		VDBMetaData vdb = new VDBMetaData();
		vdb.setXmlDeployment(true);
		vdb.setName(name);
		vdb.setModels(Arrays.asList(models));
		//TODO: the api should be hardened to prevent the creation of invalid metadata
		//missing source/translator names will cause issues
		deployVDB(vdb, null);
	}
	
	/**
	 * Deploy a vdb.xml file.  The name and version will be derived from the xml.
	 * @param is, which will be closed by this deployment
	 * @throws TranslatorException 
	 * @throws ConnectorManagerException 
	 * @throws VirtualDatabaseException 
	 * @throws IOException 
	 */
	public void deployVDB(InputStream is) throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, IOException {
		if (is == null) {
			return;
		}
		byte[] bytes = ObjectConverterUtil.convertToByteArray(is);
		try {
			//TODO: find a way to do this off of the stream
			VDBMetadataParser.validate(new ByteArrayInputStream(bytes));
		} catch (SAXException e) {
			throw new VirtualDatabaseException(e);
		}
		VDBMetaData metadata;
		try {
			metadata = VDBMetadataParser.unmarshell(new ByteArrayInputStream(bytes));
		} catch (XMLStreamException e) {
			throw new VirtualDatabaseException(e);
		}
		metadata.setXmlDeployment(true);
		deployVDB(metadata, null);
	}
	
	/**
	 * Deploy a vdb zip file.  The name and version will be derived from the xml.
	 * @param url
	 * @throws TranslatorException 
	 * @throws ConnectorManagerException 
	 * @throws VirtualDatabaseException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public void deployVDBZip(URL url) throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, IOException, URISyntaxException {
		VirtualFile root = PureZipFileSystem.mount(url);
		VirtualFile vdbMetadata = root.getChild("/META-INF/vdb.xml"); //$NON-NLS-1$
		try {
			VDBMetadataParser.validate(vdbMetadata.openStream());
		} catch (SAXException e) {
			throw new VirtualDatabaseException(e);
		}
		InputStream is = vdbMetadata.openStream();
		VDBMetaData metadata;
		try {
			metadata = VDBMetadataParser.unmarshell(is);
		} catch (XMLStreamException e) {
			throw new VirtualDatabaseException(e);
		}
		VDBResources resources = new VDBResources(root, metadata);
		deployVDB(metadata, resources);
	}
	
	protected void deployVDB(VDBMetaData vdb, VDBResources resources) 
			throws ConnectorManagerException, VirtualDatabaseException, TranslatorException {
		checkStarted();
		if (!vdb.getOverrideTranslators().isEmpty()) {
			throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40106, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40106, vdb.getName()));
		}
		cmr.createConnectorManagers(vdb, this);
		MetadataStore metadataStore = new MetadataStore();
		UDFMetaData udfMetaData = new UDFMetaData();
		udfMetaData.setFunctionClassLoader(Thread.currentThread().getContextClassLoader());
		MetadataRepository<?, ?> defaultRepo = null;
		LinkedHashMap<String, VDBResources.Resource> visibilityMap = null;
		if (resources != null) {
			//check to see if there is an index file.  if there is then we assume
			//that index is the default metadata repo
			for (String s : resources.getEntriesPlusVisibilities().keySet()) {
				if (s.endsWith(VDBResources.INDEX_EXT)) {
					defaultRepo = new IndexMetadataRepository();
					break;
				}
			}
			visibilityMap = resources.getEntriesPlusVisibilities();
		} else {
			visibilityMap = new LinkedHashMap<String, VDBResources.Resource>();
		}
		this.assignMetadataRepositories(vdb, defaultRepo);
		repo.addVDB(vdb, metadataStore, visibilityMap, udfMetaData, cmr, false);
		try {
			this.loadMetadata(vdb, cmr, metadataStore, resources, false);
		} catch (VDBValidationError e) {
			throw new VirtualDatabaseException(RuntimePlugin.Event.valueOf(e.getCode()), e.getMessage());
		}
	}
	
	@Override
	protected MetadataRepository<?, ?> getMetadataRepository(String repoType)
			throws VirtualDatabaseException {
		if ("index".equals(repoType)) { //$NON-NLS-1$
			//return a new instance since the repos are globally scoped
			//this does not support MMX style index files organized by type
			return new IndexMetadataRepository(); 
		}
		return super.getMetadataRepository(repoType);
	}
	
	/**
	 * TODO: consolidate this logic more into the abstract deployer
	 */
	@Override
	protected void loadMetadata(VDBMetaData vdb, ModelMetaData model,
			ConnectorManagerRepository cmr,
			MetadataRepository metadataRepository, MetadataStore store,
			AtomicInteger loadCount, VDBResources vdbResources) throws TranslatorException {
		MetadataFactory factory = createMetadataFactory(vdb, model, vdbResources==null?Collections.EMPTY_MAP:vdbResources.getEntriesPlusVisibilities());
		
		ExecutionFactory ef = null;
		Object cf = null;
		
		TranslatorException te = null;
		for (ConnectorManager cm : getConnectorManagers(model, cmr)) {
			if (te != null) {
				LogManager.logDetail(LogConstants.CTX_RUNTIME, te, "Failed to get metadata, trying next source."); //$NON-NLS-1$
				te = null;
			}
			try {
				if (cm != null) {
					ef = cm.getExecutionFactory();
					cf = cm.getConnectionFactory();
				}
			} catch (TranslatorException e) {
				LogManager.logDetail(LogConstants.CTX_RUNTIME, e, "Failed to get a connection factory for metadata load."); //$NON-NLS-1$
			}
		
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_RUNTIME, "CREATE SCHEMA", factory.getSchema().getName(), ";\n", DDLStringVisitor.getDDLString(factory.getSchema(), null, null)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			
			try {
				metadataRepository.loadMetadata(factory, ef, cf);
				break;
			} catch (TranslatorException e) {
				te = e;
			}
		}
		if (te != null) {
			throw te;
		}
		metadataLoaded(vdb, model, store, loadCount, factory, true, false);
	}
	
	public void undeployVDB(String vdbName) {
		checkStarted();
		this.repo.removeVDB(vdbName, 1);
	}

	/**
	 * Stops the server.  Once stopped it cannot be restarted.
	 */
	public synchronized void stop() {
		if (config != null) {
			config.stop();
		}
		if (running == null || !running) {
			return;
		}
		for (SocketListener socket:this.transports) {
			socket.stop();
		}
		this.transports.clear();
		dqp.stop();
		eventDistributorFactoryService.stop();
		config.getCacheFactory().destroy();
		config.setCacheFactory(null);
		if (this.bufferService != null) {
			this.bufferService.stop();
		}
		bufferService = null;
		dqp = null;
		running = false;
	}

	private synchronized void checkStarted() {
		if (running == null || !running) {
			throw new IllegalStateException();
		}
	}

	public TeiidDriver getDriver() {
		checkStarted();
		return driver;
	}
	
	@Override
	public EventDistributor getEventDistributor() {
		return this.eventDistributorFactoryService.getEventDistributor();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ExecutionFactory<Object, Object> getExecutionFactory(String name)
			throws ConnectorManagerException {
		ExecutionFactory<?, ?> ef = translators.get(name);
		return (ExecutionFactory<Object, Object>) ef;
	}
	
	@Override
	protected VDBRepository getVDBRepository() {
		return this.repo;
	}
	
	/**
	 * Get the effective ddl text for the given schema 
	 * @param vdbName
	 * @param schemaName
	 * @return the ddl or null if the vdb/schema does not exist
	 */
	public String getSchemaDdl(String vdbName, String schemaName) {
		VDBMetaData vdb = repo.getVDB(vdbName, 1);
		if (vdb == null) {
			return null;
		}
		TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
		if (metadata == null) {
			return null;
		}
		Schema schema = metadata.getMetadataStore().getSchema(schemaName);
		if (schema == null) {
			return null;
		}
		return DDLStringVisitor.getDDLString(schema, null, null); 
	}
	
	/**
	 * Return the bound port for the transport number
	 * @param transport
	 * @return
	 */
	public int getPort(int transport) {
		return this.transports.get(transport).getPort();
	}
	
}
