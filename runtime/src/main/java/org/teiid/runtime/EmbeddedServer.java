/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.transaction.TransactionManager;
import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.VirtualFile;
import org.teiid.PreParser;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.BundleUtil.Event;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.NamedThreadFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.deployers.CompositeGlobalTableStore;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.LocalProfile;
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
import org.teiid.metadatastore.DeploymentBasedDatabaseStore;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.PureZipFileSystem;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;
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
	
	static {
		LogManager.setLogListener(new JBossLogger());
	}

	private LocalProfile embeddedProfile = new LocalProfile() {
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
				String translatorName, String connectionName, ExecutionFactory<Object, Object> ef) throws ConnectorManagerException {
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
	
	class EmbeddedEventDistributorFactoryService extends AbstractEventDistributorFactoryService {

        @Override
        protected VDBRepository getVdbRepository() {
            return repo;
        }
        
        @Override
        protected ObjectReplicator getObjectReplicator() {
            return replicator;
        }
        
        @Override
        protected DQPCore getDQPCore() {
            return dqp;
        }
    };	
	
	protected boolean throwMetadataErrors = true;
	private ConcurrentHashMap<String, ExecutionFactory<?, ?>> translators = new ConcurrentHashMap<String, ExecutionFactory<?, ?>>();
	private TranslatorRepository translatorRepository = new TranslatorRepository();
	private ConcurrentHashMap<String, ConnectionFactoryProvider<?>> connectionFactoryProviders = new ConcurrentHashMap<String, ConnectionFactoryProvider<?>>();
	protected SessionServiceImpl sessionService = new SessionServiceImpl();
	protected ObjectReplicator replicator;
	protected BufferServiceImpl bufferService = new BufferServiceImpl();
	protected TransactionServerImpl transactionService = new TransactionServerImpl();
	protected boolean waitForLoad;
	protected ClientServiceRegistryImpl services = new ClientServiceRegistryImpl() {
		@Override
		public void waitForFinished(VDBKey vdbKey, int timeOutMillis) throws ConnectionException {
			if (waitForLoad) {
				repo.waitForFinished(vdbKey, timeOutMillis);
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
	
	protected AbstractEventDistributorFactoryService eventDistributorFactoryService;
	protected boolean useCallingThread = true;
	private Boolean running;
	private EmbeddedConfiguration config;
	private SessionAwareCache<CachedResults> rs;
	private SessionAwareCache<PreparedPlan> ppc;
	protected ArrayList<SocketListener> transports = new ArrayList<SocketListener>();
	private ScheduledExecutorService scheduler;
	private MaterializationManager materializationMgr = null;
	private ShutDownListener shutdownListener = new ShutDownListener();
	private SimpleChannelFactory channelFactory;
	private NodeTracker nodeTracker = null;

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
	 * @param connectionFactory
	 */
	public void addConnectionFactory(String name, Object connectionFactory) {
		this.connectionFactoryProviders.put(name, new SimpleConnectionFactoryProvider<Object>(connectionFactory));
	}
	
	public synchronized void start(@SuppressWarnings("hiding") EmbeddedConfiguration config) {
		if (running != null) {
			throw new IllegalStateException();
		}
		this.dqp.setLocalProfile(this.embeddedProfile);
		this.shutdownListener.setBootInProgress(true);
		this.config = config;
		System.setProperty("jboss.node.name", config.getNodeName()==null?"localhost":config.getNodeName());
		this.cmr.setProvider(this);
		this.eventDistributorFactoryService = new EmbeddedEventDistributorFactoryService();
		this.eventDistributorFactoryService.start();
		this.dqp.setEventDistributor(this.eventDistributorFactoryService.getReplicatedEventDistributor());
		this.scheduler = Executors.newScheduledThreadPool(config.getMaxAsyncThreads(), new NamedThreadFactory("Asynch Worker")); //$NON-NLS-1$
		this.replicator = config.getObjectReplicator();
		if (this.replicator == null && config.getJgroupsConfigFile() != null) {
			channelFactory = new SimpleChannelFactory(config);
			this.replicator = new JGroupsObjectReplicator(channelFactory, this.scheduler);
			try {
                this.nodeTracker = new NodeTracker(channelFactory.createChannel("teiid-node-tracker"), config.getNodeName()) {
                    @Override
                    public ScheduledExecutorService getScheduledExecutorService() {
                        return scheduler;
                    }
                };
            } catch (Exception e) {
                LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
            }
		}
		this.eventDistributorFactoryService = new EmbeddedEventDistributorFactoryService();
		//must be called after the replicator is set
        this.eventDistributorFactoryService.start();
        this.dqp.setEventDistributor(this.eventDistributorFactoryService.getReplicatedEventDistributor());
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
		setBufferManagerProperties(config);
		BufferService bs = getBufferService();
		this.dqp.setBufferManager(bs.getBufferManager());

		startVDBRepository();

		rs = new SessionAwareCache<CachedResults>("resultset", config.getCacheFactory(), SessionAwareCache.Type.RESULTSET, config.getMaxResultSetCacheStaleness()); //$NON-NLS-1$
		ppc = new SessionAwareCache<PreparedPlan>("preparedplan", config.getCacheFactory(), SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$
		rs.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setResultsetCache(rs);

		ppc.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setPreparedPlanCache(ppc);

		this.dqp.setTransactionService((TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, this.transactionService, new Class[] {TransactionService.class}, MessageLevel.DETAIL, Thread.currentThread().getContextClassLoader()));
		
		this.dqp.start(config);
		this.sessionService.setDqp(this.dqp);
		this.services.setSecurityHelper(this.sessionService.getSecurityHelper());
		if (this.config.getAuthenticationType() != null) {
		    this.services.setAuthenticationType(this.config.getAuthenticationType());
		    this.sessionService.setAuthenticationType(this.config.getAuthenticationType());
		}
		this.sessionService.start();
		this.services.setVDBRepository(this.repo);
		this.materializationMgr = getMaterializationManager();		
		this.repo.addListener(this.materializationMgr);
		this.repo.setAllowEnvFunction(this.config.isAllowEnvFunction());
		if (this.nodeTracker != null) {
		    this.nodeTracker.addNodeListener(this.materializationMgr);
		}
		this.logon = new LogonImpl(sessionService, null);
		services.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
		DQP dqpProxy = DQP.class.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {DQP.class}, new SessionCheckingProxy(dqp, LogConstants.CTX_DQP, MessageLevel.TRACE)));
		services.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);
		initDriver();
		List<SocketConfiguration> transports = config.getTransports();
		if ( transports != null && !transports.isEmpty()) {
			for (SocketConfiguration socketConfig:transports) {
				SocketListener socketConnection = startTransport(socketConfig, bs.getBufferManager(), config.getMaxODBCLobSizeAllowed());
				if (socketConfig.getSSLConfiguration() != null) {
					try {
						socketConfig.getSSLConfiguration().getServerSSLEngine();
					} catch (Exception e) {
						throw new TeiidRuntimeException(e);
					}
				}
				this.transports.add(socketConnection);
			}
		}
		this.shutdownListener.setBootInProgress(false);
		this.shutdownListener.started();
		running = true;
	}

	private void setBufferManagerProperties(EmbeddedConfiguration config) {
		
		this.bufferService.setUseDisk(config.isUseDisk());
		if (config.isUseDisk()) {
			if (config.getBufferDirectory() == null) {
				config.setBufferDirectory(System.getProperty("java.io.tmpdir")); //$NON-NLS-1$
			}
			this.bufferService.setDiskDirectory(config.getBufferDirectory());
		}
		
		if(config.getProcessorBatchSize() != -1)
			this.bufferService.setProcessorBatchSize(config.getProcessorBatchSize());
		if(config.getMaxReserveKb() != -1)
			this.bufferService.setMaxReserveKb(config.getMaxReserveKb());
		if(config.getMaxProcessingKb() != -1)
			this.bufferService.setMaxProcessingKb(config.getMaxProcessingKb());
		this.bufferService.setInlineLobs(config.isInlineLobs());
		if(config.getMaxOpenFiles() != -1)
			this.bufferService.setMaxOpenFiles(config.getMaxOpenFiles());
		
		if(config.getMaxBufferSpace() != -1)
			this.bufferService.setMaxBufferSpace(config.getMaxBufferSpace());
		if(config.getMaxFileSize() != -1) 
			this.bufferService.setMaxFileSize(config.getMaxFileSize());
		this.bufferService.setEncryptFiles(config.isEncryptFiles());
		if(config.getMaxStorageObjectSize() != -1) {
			this.bufferService.setMaxStorageObjectSize(config.getMaxStorageObjectSize());
		}
		this.bufferService.setMemoryBufferOffHeap(config.isMemoryBufferOffHeap());
		if(config.getMemoryBufferSpace() != -1)
			this.bufferService.setMemoryBufferSpace(config.getMemoryBufferSpace());
		
	}

	private void initDriver() {
		driver.setLocalProfile(embeddedProfile);
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
    		ODBCSocketListener odbc = new ODBCSocketListener(address, socketConfig, this.services, bm, maxODBCLobSize, this.logon, driver);
    		return odbc;
		}
		throw new AssertionError("Unknown protocol " + socketConfig.getProtocol()); //$NON-NLS-1$
	}

	private void startVDBRepository() {
		this.repo.addListener(new VDBLifeCycleListener() {

			@Override
			public void added(String name, CompositeVDB vdb) {

			}

			@Override
			public void removed(String name, CompositeVDB vdb) {
				if (replicator != null) {
					replicator.stop(vdb.getVDB().getAttachment(GlobalTableStore.class));
				}
				rs.clearForVDB(vdb.getVDBKey()); 
				ppc.clearForVDB(vdb.getVDBKey()); 
				for (SessionMetadata session : sessionService.getSessionsLoggedInToVDB(vdb.getVDBKey())) { 
					try {
						sessionService.closeSession(session.getSessionId());
					} catch (InvalidSessionException e) {
					}
				}
			}

			@Override
			public void finishedDeployment(String name, CompositeVDB vdb) {
				if (!vdb.getVDB().getStatus().equals(Status.ACTIVE)) {
					return;
				}
				GlobalTableStore gts = CompositeGlobalTableStore.createInstance(vdb, dqp.getBufferManager(), replicator);
				
				vdb.getVDB().addAttchment(GlobalTableStore.class, gts);
			}

			@Override
			public void beforeRemove(String name, CompositeVDB vdb) {
			}
		});
		this.repo.setSystemFunctionManager(SystemMetadata.getInstance().getSystemFunctionManager());
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

	protected MaterializationManager getMaterializationManager() {
	    return new MaterializationManager(this.shutdownListener) {
	    	
	    	@Override
	    	public ScheduledExecutorService getScheduledExecutorService() {
	    		return scheduler;
	    	}

            @Override
            public DQPCore getDQP() {
            	return dqp;
            }

            @Override
            public VDBRepository getVDBRepository() {
                return repo;
            }
        };
	}

	/**
	 * Adds a definition of the {@link ExecutionFactory} using the default name either from the {@link Translator} annotation or the class name.
	 * Only {@link ExecutionFactory} classes with a {@link Translator} annotation can be referenced by {@link #addTranslator(String, String, Map)}
	 * @param ef
	 * @throws TranslatorException 
	 */
	public void addTranslator(Class<? extends ExecutionFactory> clazz) throws TranslatorException {
		try {
            VDBTranslatorMetaData vdbTranslatorMetaData = TranslatorUtil.buildTranslatorMetadata(clazz.newInstance(), null);
            if (vdbTranslatorMetaData != null) {
                translatorRepository.addTranslatorMetadata(vdbTranslatorMetaData.getName(), vdbTranslatorMetaData);
            } else {
                //not a well defined translator
                ExecutionFactory<?, ?> instance = clazz.newInstance();
                instance.start();
                addTranslator(clazz.getName(), instance);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new TranslatorException(e);
        }
	}
	
	/**
	 * Add an override translator
	 * @param name
	 * @param type the name of an existing translator to override
	 * @param properties
	 */
	public void addTranslator(String name, String type, Map<String, String> properties) throws TranslatorException {
	    VDBTranslatorMetaData vdbTranslatorMetaData = new VDBTranslatorMetaData();
	    vdbTranslatorMetaData.setName(name);
	    VDBTranslatorMetaData parent = translatorRepository.getTranslatorMetaData(type);
	    if (parent == null) {
	        throw new TranslatorException(RuntimePlugin.Event.TEIID40136, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40136, type));
	    }
	    vdbTranslatorMetaData.setParent(parent);
	    if (properties != null) {
            Properties p = new Properties();
    	    p.putAll(properties);
    	    vdbTranslatorMetaData.setProperties(p);
	    }
	    this.translatorRepository.addTranslatorMetadata(name, vdbTranslatorMetaData);
	}
	
	/**
     * Add a named {@link ExecutionFactory}. NOTE: Only this single instance will be shared for all usage.
     * @param name
     * @param ef the already started ExecutionFactory
     * @see {@link #addTranslator(String, String, Map)} or {@link #addTranslator(Class)}
     */
    public void addTranslator(String name, ExecutionFactory<?, ?> ef) {
        translators.put(name, ef);
        VDBTranslatorMetaData vdbTranslatorMetaData = TranslatorUtil.buildTranslatorMetadata(ef, null, true);
        if (vdbTranslatorMetaData != null) {
            this.translatorRepository.addTranslatorMetadata(vdbTranslatorMetaData.getName(), vdbTranslatorMetaData);
        }
        vdbTranslatorMetaData = TranslatorUtil.buildTranslatorMetadata(ef, null, false);
        if (vdbTranslatorMetaData != null) {
            this.translatorRepository.addTranslatorMetadata(name, vdbTranslatorMetaData);
        }
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
		VDBKey key = new VDBKey(name, null);
		vdb.setName(key.getName());
		if (key.isAtMost()) {
			if (name.endsWith(".")) {
				//error
			} else {
				vdb.setVersion("1"); //$NON-NLS-1$
			}
		} else {
			vdb.setVersion(key.getVersion());
		}
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
		deployVDB(is, false);
	}
	
	/**
	 * Deploy a vdb.xml file.  The name and version will be derived from the xml.
	 * @param is, which will be closed by this deployment
	 * @param ddl, true if the file contents are DDL
	 * @throws TranslatorException 
	 * @throws ConnectorManagerException 
	 * @throws VirtualDatabaseException 
	 * @throws IOException 
	 */	
	public void deployVDB(InputStream is, boolean ddl) throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, IOException {
		if (is == null) {
			return;
		}
		byte[] bytes = ObjectConverterUtil.convertToByteArray(is);
		VDBMetaData metadata = null;
		if (ddl) {
			DeploymentBasedDatabaseStore store = new DeploymentBasedDatabaseStore(getVDBRepository());
			metadata = store.getVDBMetadata(new String(bytes));
		} else {
			try {
				//TODO: find a way to do this off of the stream
				VDBMetadataParser.validate(new ByteArrayInputStream(bytes));
			} catch (SAXException e) {
				throw new VirtualDatabaseException(e);
			}			
			try {
				metadata = VDBMetadataParser.unmarshell(new ByteArrayInputStream(bytes));
			} catch (XMLStreamException e) {
				throw new VirtualDatabaseException(e);
			}
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
		VDBMetaData metadata;
		
		VirtualFile vdbMetadata = root.getChild("/META-INF/vdb.xml"); //$NON-NLS-1$
		if (vdbMetadata.exists()) {
    		try {
    			VDBMetadataParser.validate(vdbMetadata.openStream());
    		} catch (SAXException e) {
    			throw new VirtualDatabaseException(e);
    		}
    		InputStream is = vdbMetadata.openStream();
    		try {
    			metadata = VDBMetadataParser.unmarshell(is);
    		} catch (XMLStreamException e) {
    			throw new VirtualDatabaseException(e);
    		}
		} else {
		    vdbMetadata = root.getChild("/META-INF/vdb.ddl"); //$NON-NLS-1$
	        DeploymentBasedDatabaseStore store = new DeploymentBasedDatabaseStore(getVDBRepository());
	        metadata = store.getVDBMetadata(ObjectConverterUtil.convertToString(vdbMetadata.openStream()));
		}
		
		VDBResources resources = new VDBResources(root, metadata);
		deployVDB(metadata, resources);
	}
	
	protected boolean allowOverrideTranslators() {
	    return false;
	}
	
	protected void deployVDB(VDBMetaData vdb, VDBResources resources) 
			throws ConnectorManagerException, VirtualDatabaseException, TranslatorException {
		checkStarted();
		
		if (!vdb.getOverrideTranslators().isEmpty() && !allowOverrideTranslators()) {
			throw new VirtualDatabaseException(RuntimePlugin.Event.TEIID40106, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40106, vdb.getName()));
		}
		
		vdb.addAttchment(ClassLoader.class, Thread.currentThread().getContextClassLoader());
		try {
            createPreParser(vdb);
        } catch (TeiidException e1) {
            throw new VirtualDatabaseException(e1);
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
		repo.addVDB(vdb, metadataStore, visibilityMap, udfMetaData, cmr);
		try {
			this.loadMetadata(vdb, cmr, metadataStore, resources);
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
		MetadataFactory factory = createMetadataFactory(vdb, store, model, vdbResources==null?Collections.EMPTY_MAP:vdbResources.getEntriesPlusVisibilities());
		
		ExecutionFactory ef = null;
		Object cf = null;
		
		Exception te = null;
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
			} catch (Exception e) {
				te = e;
				factory = createMetadataFactory(vdb, store, model, vdbResources==null?Collections.EMPTY_MAP:vdbResources.getEntriesPlusVisibilities());
			}
		}
		if (te != null) {
			if (te instanceof TranslatorException) {
				throw (TranslatorException)te;
			}
			if (te instanceof RuntimeException) {
				throw (RuntimeException)te;
			}
			throw new TranslatorException(te);
		}
		metadataLoaded(vdb, model, store, loadCount, factory, true, cmr, vdbResources);
	}
	
	public void undeployVDB(String vdbName) {
	    undeployVDB(vdbName, "1");
	}
	
    public void undeployVDB(String vdbName, String version) {
        checkStarted();
        this.repo.removeVDB(vdbName, version); //$NON-NLS-1$
    }
	

	EmbeddedConfiguration getConfiguration() {
	    return this.config;
	}
	
	/**
	 * Stops the server.  Once stopped it cannot be restarted.
	 */
	public synchronized void stop() {
		if (running == null || !running) {
			return;
		}
		if (this.channelFactory != null) {
			this.channelFactory.stop();
		}
        this.shutdownListener.setShutdownInProgress(true);
        this.repo.removeListener(this.materializationMgr);
        this.scheduler.shutdownNow();
		for (SocketListener socket:this.transports) {
			socket.stop();
		}
		this.sessionService.stop();
		this.transports.clear();
		dqp.stop();
		if (config != null) {
            config.stop();
        }
		eventDistributorFactoryService.stop();
		config.getCacheFactory().destroy();
		config.setCacheFactory(null);
		if (this.bufferService != null) {
			this.bufferService.stop();
		}
		bufferService = null;
		dqp = null;
		running = false;
		this.shutdownListener.setShutdownInProgress(false);
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
		if (ef == null) {
		    //map to the expected map
            IdentityHashMap<org.teiid.adminapi.Translator, ExecutionFactory<Object, Object>> map = translators
                    .entrySet().stream()
                    .filter(e -> this.translatorRepository
                            .getTranslatorMetaData(e.getKey()) != null)
                    .collect(Collectors.toMap(
                            e -> (org.teiid.adminapi.Translator) this.translatorRepository
                                    .getTranslatorMetaData(e.getKey()),
                            e -> (ExecutionFactory<Object, Object>) e
                                    .getValue(),
                            (x, y) -> x, IdentityHashMap::new));
		    return TranslatorUtil.getExecutionFactory(name, this.translatorRepository, this.translatorRepository, 
		            null, map, new HashSet<String>());
		}
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
		VDBMetaData vdb = repo.getVDB(vdbName, "1"); //$NON-NLS-1$
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
	
	TranslatorRepository getTranslatorRepository() {
	    return translatorRepository;
	}
	
	ConcurrentHashMap<String, ConnectionFactoryProvider<?>> getConnectionFactoryProviders() {
	    return connectionFactoryProviders;
	}
	
	protected SessionAwareCache<CachedResults> getRsCache() {
		return this.rs;
	}
	
	protected SessionAwareCache<PreparedPlan> getPpcCache() {
		return this.ppc;
	}
	
	public Admin getAdmin() {
		return EmbeddedAdminFactory.getInstance().createAdmin(this);
	}

	static class ShutDownListener implements ContainerLifeCycleListener {
	    private boolean shutdownInProgress = false;
	    private boolean bootInProgress = false;
	    private boolean running = false;
	    
        @Override
        public boolean isShutdownInProgress() {
            return shutdownInProgress;
        }

        @Override
        public boolean isBootInProgress() {
            return bootInProgress;
        }

        public void addListener(LifeCycleEventListener listener) {
        }

        public void setBootInProgress(boolean value) {
            this.bootInProgress = value;
        }

        public void setShutdownInProgress(boolean value) {
            this.shutdownInProgress = value;
        }

		public void started() {
			running = true;
		}
		
		@Override
		public boolean isStarted() {
			return running;
		}
	}
	
	public static void createPreParser(VDBMetaData deployment) throws TeiidException {
	    String preparserClass = deployment.getPropertyValue(VDBMetaData.PREPARSER_CLASS);
        if (preparserClass != null) {
            ClassLoader vdbClassLoader = deployment.getAttachment(ClassLoader.class);
            PreParser preParser = (PreParser) ReflectionHelper.create(preparserClass, Collections.emptyList(), vdbClassLoader);
            deployment.addAttchment(PreParser.class, preParser);
        }
	}
}
