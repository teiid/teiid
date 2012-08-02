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


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.teiid.Replicated;
import org.teiid.Replicated.ReplicationMode;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.security.ILogon;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.BundleUtil.Event;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
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
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.ConnectionProfile;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.services.AbstractEventDistributorFactoryService;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.vdb.runtime.VDBKey;

/**
 * A simplified server environment for embedded use.
 * 
 * Needs to be started prior to use with a call to {@link #start(EmbeddedConfiguration)}
 */
@SuppressWarnings("serial")
public class EmbeddedServer extends AbstractVDBDeployer implements EventDistributorFactory, ExecutionFactoryProvider {

	protected final class TransactionDetectingTransactionServer extends
			TransactionServerImpl {
		
		/**
		 * Override to detect existing thread bound transactions.
		 * This may be of interest for local connections as well, but
		 * we assume there that a managed datasource will be used.
		 * A managed datasource is not possible here.
		 */
		public TransactionContext getOrCreateTransactionContext(final String threadId) {
			TransactionContext tc = super.getOrCreateTransactionContext(threadId);
			if (useCallingThread && detectTransactions && tc.getTransaction() == null) {
				try {
					Transaction tx = transactionManager.getTransaction();
					if (tx != null) {
						tx.registerSynchronization(new Synchronization() {
							
							@Override
							public void beforeCompletion() {
							}
							
							@Override
							public void afterCompletion(int status) {
								transactions.removeTransactionContext(threadId);
							}
						});
						tc.setTransaction(tx);
						tc.setTransactionType(Scope.LOCAL);
					}
				} catch (SystemException e) {
				} catch (IllegalStateException e) {
				} catch (RollbackException e) {
				}
			}
			
			return tc;
		}
	}

	protected class ProviderAwareConnectorManagerRepository extends
			ConnectorManagerRepository {
		protected ConnectorManager createConnectorManager(
				String translatorName, String connectionName) {
			return new ConnectorManager(translatorName, connectionName) {
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
	
	/**
	 * Annotated cache for use with the {@link EmbeddedServer} with an {@link ObjectReplicator} instead of Infinispan.
	 * @param <K> key
	 * @param <V> value
	 */
	public interface ReplicatedCache<K, V> extends Cache<K, V> {

		@Replicated(replicateState = ReplicationMode.PULL)
		public V get(K key);

		@Replicated(replicateState = ReplicationMode.PUSH)
		V put(K key, V value, Long ttl);

		@Replicated()
		V remove(K key);

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
		protected void processMetadataValidatorReport(VDBKey key, ValidatorReport report) {
			if (throwMetadataErrors) {
				super.processMetadataValidatorReport(key, report); //remove
				ValidatorFailure firstFailure = report.getItems().iterator().next();
				throw new VDBValidationError(RuntimePlugin.Event.TEIID40095, firstFailure.getMessage());
			}
		}
	};
	protected boolean throwMetadataErrors = true;
	private ConcurrentHashMap<String, ExecutionFactory<?, ?>> translators = new ConcurrentHashMap<String, ExecutionFactory<?, ?>>();
	private ConcurrentHashMap<String, ConnectionFactoryProvider<?>> connectionFactoryProviders = new ConcurrentHashMap<String, ConnectionFactoryProvider<?>>();
	protected SessionServiceImpl sessionService = new SessionServiceImpl();
	protected ObjectReplicator replicator;
	protected BufferServiceImpl bufferService = new BufferServiceImpl();
	protected TransactionDetectingTransactionServer transactionService = new TransactionDetectingTransactionServer();
	protected boolean waitForLoad;
	protected ClientServiceRegistryImpl services = new ClientServiceRegistryImpl() {
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
	protected DefaultCacheFactory dcf = new DefaultCacheFactory() {
		
		List<ReplicatedCache<?, ?>> caches = new ArrayList<ReplicatedCache<?, ?>>();
		
		public boolean isReplicated() {
			return true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <K, V> Cache<K, V> get(String location,
				CacheConfiguration config) {
			Cache<K, V> result = super.get(location, config);
			if (replicator != null) {
				try {
					ReplicatedCache cache = replicator.replicate("$RS$", ReplicatedCache.class, new ReplicatedCacheImpl(result), 0); //$NON-NLS-1$
					caches.add(cache);
					return cache;
				} catch (Exception e) {
					throw new TeiidRuntimeException(e);
				}
			}
			return result;
		}
		
		@Override
		public void destroy() {
			if (replicator != null) {
				for (ReplicatedCache<?, ?> cache : caches) {
					replicator.stop(cache);
				}
				caches.clear();
			}
			super.destroy();
		}
	};
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
	//TODO: allow for configurablity - in environments that support subtransations it would be fine
	//to allow teiid to start a request transaction under an existing thread bound transaction
	protected boolean detectTransactions = true;
	private Boolean running;
	private AtomicLong requestIdentifier = new AtomicLong();
	
	public EmbeddedServer() {

	}

	public void addConnectionFactoryProvider(String name,
			ConnectionFactoryProvider<?> connectionFactoryProvider) {
		this.connectionFactoryProviders.put(name, connectionFactoryProvider);
	}

	public synchronized void start(EmbeddedConfiguration dqpConfiguration) {
		if (running != null) {
			throw new IllegalStateException();
		}
		this.eventDistributorFactoryService.start();
		this.dqp.setEventDistributor(this.eventDistributorFactoryService.getReplicatedEventDistributor());
		this.replicator = dqpConfiguration.getObjectReplicator();
		if (dqpConfiguration.getTransactionManager() == null) {
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
			this.transactionService.setTransactionManager((TransactionManager) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {TransactionManager.class}, new InvocationHandler() {
				
				@Override
				public Object invoke(Object proxy, Method method, Object[] args)
						throws Throwable {
					throw new UnsupportedOperationException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
				}
			}));
			this.detectTransactions = false;
		} else {
			this.transactionService.setTransactionManager(dqpConfiguration.getTransactionManager());
		}
		if (dqpConfiguration.getSecurityHelper() != null) {
			this.sessionService.setSecurityHelper(dqpConfiguration.getSecurityHelper());
		} else {
			this.sessionService.setSecurityHelper(new DoNothingSecurityHelper());
		}
		if (dqpConfiguration.getSecurityDomains() != null) {
			this.sessionService.setSecurityDomains(dqpConfiguration.getSecurityDomains());
		} else {
			this.sessionService.setSecurityDomains(Arrays.asList("teiid-security")); //$NON-NLS-1$
		}

		this.sessionService.setVDBRepository(repo);
		this.bufferService.setUseDisk(dqpConfiguration.isUseDisk());
		if (dqpConfiguration.isUseDisk()) {
			if (dqpConfiguration.getBufferDirectory() == null) {
				dqpConfiguration.setBufferDirectory(System.getProperty("java.io.tmpdir")); //$NON-NLS-1$
			}
			this.bufferService.setDiskDirectory(dqpConfiguration.getBufferDirectory());
		}
		BufferService bs = getBufferService();
		this.dqp.setBufferManager(bs.getBufferManager());

		startVDBRepository();

		SessionAwareCache<CachedResults> rs = new SessionAwareCache<CachedResults>(dcf, SessionAwareCache.Type.RESULTSET, new CacheConfiguration(Policy.LRU, 60, 250, "resultsetcache")); //$NON-NLS-1$
		SessionAwareCache<PreparedPlan> ppc = new SessionAwareCache<PreparedPlan>(dcf, SessionAwareCache.Type.PREPAREDPLAN,	new CacheConfiguration());
		rs.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setResultsetCache(rs);

		ppc.setTupleBufferCache(bs.getTupleBufferCache());
		this.dqp.setPreparedPlanCache(ppc);

		this.dqp.setTransactionService(this.transactionService);

		this.dqp.start(dqpConfiguration);
		this.sessionService.setDqp(this.dqp);
		this.services.setSecurityHelper(this.sessionService.getSecurityHelper());
		this.logon = new LogonImpl(sessionService, null);
		services.registerClientService(ILogon.class, logon, null);
		services.registerClientService(DQP.class, dqp, null);
		initDriver();
		running = true;
	}

	private void initDriver() {
		driver.setEmbeddedProfile(new ConnectionProfile() {

			@Override
			public ConnectionImpl connect(String url, Properties info)
					throws TeiidSQLException {
				LocalServerConnection conn;
				try {
					conn = new LocalServerConnection(info, useCallingThread) {
						@Override
						protected ClientServiceRegistry getClientServiceRegistry() {
							return services;
						}
					};
				} catch (CommunicationException e) {
					throw TeiidSQLException.create(e);
				} catch (ConnectionException e) {
					throw TeiidSQLException.create(e);
				}
				return new ConnectionImpl(conn, info, url);
			}
		});
	}

	private void startVDBRepository() {
		this.repo.addListener(new VDBLifeCycleListener() {

			@Override
			public void added(String name, int version, CompositeVDB vdb) {

			}

			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				if (replicator != null) {
					replicator.stop(vdb.getVDB().getAttachment(GlobalTableStore.class));
				}
			}

			@Override
			public void finishedDeployment(String name, int version,
					CompositeVDB vdb) {
				GlobalTableStore gts = new GlobalTableStoreImpl(dqp.getBufferManager(), vdb.getVDB().getAttachment(TransformationMetadata.class));
				if (replicator != null) {
					try {
						gts = replicator.replicate(name + version, GlobalTableStore.class, gts, 300000);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				vdb.getVDB().addAttchment(GlobalTableStore.class, gts);
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
	 * Add an {@link ExecutionFactory}.  The {@link ExecutionFactory#start()} method
	 * should have already been called.
	 * @param ef
	 */
	public void addTranslator(ExecutionFactory<?, ?> ef) {
		Translator t = ef.getClass().getAnnotation(Translator.class);
		String name = ef.getClass().getName();
		if (t != null) {
			name = t.name();
		}
		addTranslator(name, ef);
	}
	
	void addTranslator(String name, ExecutionFactory<?, ?> ef) {
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
		checkStarted();
		VDBMetaData vdb = new VDBMetaData();
		vdb.setDynamic(true);
		vdb.setName(name);
		vdb.setModels(Arrays.asList(models));
		cmr.createConnectorManagers(vdb, this);
		MetadataStore metadataStore = new MetadataStore();
		UDFMetaData udfMetaData = new UDFMetaData();
		udfMetaData.setFunctionClassLoader(Thread.currentThread().getContextClassLoader());
		this.assignMetadataRepositories(vdb, null);
		repo.addVDB(vdb, metadataStore, new LinkedHashMap<String, Resource>(), udfMetaData, cmr);
		try {
			this.loadMetadata(vdb, cmr, metadataStore);
		} catch (VDBValidationError e) {
			throw new VirtualDatabaseException(RuntimePlugin.Event.valueOf(e.getCode()), e.getMessage());
		}
	}
	
	/**
	 * TODO: consolidate this logic more into the abstract deployer
	 */
	@Override
	protected void loadMetadata(VDBMetaData vdb, ModelMetaData model,
			ConnectorManagerRepository cmr,
			MetadataRepository metadataRepository, MetadataStore store,
			AtomicInteger loadCount) throws TranslatorException {
		Map<String, Datatype> datatypes = this.repo.getBuiltinDatatypes();
		MetadataFactory factory = new MetadataFactory(vdb.getName(), vdb.getVersion(), model.getName(), datatypes, model.getProperties(), model.getSchemaText());
		factory.getSchema().setPhysical(model.isSource());
		
		ExecutionFactory ef = null;
		Object cf = null;
		
		try {
			ConnectorManager cm = getConnectorManager(model, cmr);
			if (cm != null) {
				ef = cm.getExecutionFactory();
				cf = cm.getConnectionFactory();
			}
		} catch (TranslatorException e) {
			//cf not available
		}
		
		metadataRepository.loadMetadata(factory, ef, cf);		
		metadataLoaded(vdb, model, store, loadCount, factory, true);
	}
	
	public void undeployVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
	}

	/**
	 * Stops the server.  Once stopped it cannot be restarted.
	 */
	public synchronized void stop() {
		dqp.stop();
		eventDistributorFactoryService.stop();
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
		if (ef == null) {
			//TODO: consolidate this exception into the connectormanagerrepository
			throw new ConnectorManagerException(name);
		}
		return (ExecutionFactory<Object, Object>) ef;
	}
	
	@Override
	protected VDBRepository getVDBRepository() {
		return this.repo;
	}
	
	public class ExecutionResults {
		private List<String> columnNames;
		private List<Integer> dataTypes = new ArrayList<Integer>();
		private List<? extends List<?>> rows;
		private boolean hasMoreRows = false;
		private int lastRow;
		private SessionMetadata session;
		private long requestId;
		private DQPWorkContext context;
		private int index = 0;
		private int rowsRead = 0;
		private boolean closed = false;
		
		/**
		 * Advances the cursor and gets the next row of results. If this represents a update, then column name will
		 * be "update-count"
		 * @return row; if advanced over the last row of data, TeiidException is thrown.
		 * @throws TeiidException
		 */
		public List<?> next() throws TeiidException {
			if (hasNext()) {
				return this.rows.get(this.index++);
			}
			throw new TeiidProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40098));
		}
		
		/**
		 * Get the name of the columns
		 * @return
		 */
		public List<String> getColumnNames() {
			return columnNames;
		}

		/**
		 * Get SQL Type of the column - 0 indexed
		 * @return SQL type from java.sql.Types
		 */
		public int getDataType(int index) {
			return dataTypes.get(index);
		}
		
		/**
		 * Check to see if the is another row of data available
		 * @return true if data available; false otherwise
		 * @throws TeiidException
		 */
		public boolean hasNext() throws TeiidException {
			if ((this.index+this.rowsRead) >= this.lastRow) {
				if (this.hasMoreRows) {
					batchNext();
				}
				else {
					return false;
				}
			}
			return true;
		}
		
		private boolean batchNext() throws TeiidException {
			try {
				if (this.hasMoreRows) {
					Future<ResultsMessage> message = dqp.processCursorRequest(requestId, this.lastRow+1, 1024);
					ResultsMessage rm = message.get();
					this.columnNames = Arrays.asList(rm.getColumnNames());
					this.rows = rm.getResultsList(); 
					this.rowsRead = this.lastRow;
					this.lastRow = rm.getLastRow();
					this.index = 0;
				    if (rm.getFinalRow() == -1 || rm.getLastRow() < rm.getFinalRow()) {
				    	this.hasMoreRows = true;
				    }
				    else {
				    	this.hasMoreRows = false;
				    }
				    return true;
				}
				return false;
			} catch (InterruptedException e) {
				throw new TeiidProcessingException(e);
			} catch (ExecutionException e) {
				throw new TeiidProcessingException(e);
			}
		}
		
		public void close() throws TeiidException {
			if (!this.closed) {
				try {
			        ResultsFuture<?> response = dqp.closeRequest(this.requestId);
			        response.get();
			        this.closed = true;
			        
					context.runInContext(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							dqp.terminateSession(session.getSessionId());
							return null;
						}
					});
				} catch (Throwable e) {
					throw new TeiidException(e);
				}			
			}
		}
	}
	
	/**
	 * Internal use only. Subject to change in next versions.
	 * 
	 * Execute the query directly in the engine by submitting the AST form of the SQL and omit the whole JDBC 
	 * layer. The returned object contain the results, which is designed like java.sql.ResultSet.
	 * 
	 * @param vdbName
	 * @param version
	 * @param command
	 * @param timoutInMilli
	 * @return
	 * @throws TeiidException
	 */
	public ExecutionResults executeQuery(final String vdbName, final int version, final QueryCommand command, final long timoutInMilli) throws TeiidException {
		String user = "embedded"; //$NON-NLS-1$
		
        VDBMetaData vdb = this.repo.getLiveVDB(vdbName, version);
        if (vdb == null) {
        	throw new TeiidException(RuntimePlugin.Util.getString("wrong_vdb"));//$NON-NLS-1$
        }
        
        final SessionMetadata session = TempTableDataManager.createTemporarySession(user, "embedded", vdb); //$NON-NLS-1$

		final long requestID =  this.requestIdentifier.getAndIncrement();
		
		final DQPWorkContext context = new DQPWorkContext();
		context.setUseCallingThread(true);
		context.setSession(session);
		
		try {
			return context.runInContext(new Callable<ExecutionResults>() {
				@Override
				public ExecutionResults call() throws Exception {
					
					ExecutionResults results = new ExecutionResults();
					results.session = session;
					results.requestId = requestID;
					results.context = context;
					
					RequestMessage request = new RequestMessage() {
						@Override
						public QueryCommand getQueryCommand() {
							return command;
						}	
						
						@Override
						public String[] getCommands() {
							return new String[] {buildStringForm()};
						}
						
						@Override
						public String getCommandString() {
							return buildStringForm();
						}	
						
						private String buildStringForm() {
							return SQLStringVisitor.getSQLString(getQueryCommand());
						}						
					};
					request.setExecutionId(requestID);
					request.setRowLimit(0);
					Future<ResultsMessage> message = dqp.executeRequest(requestID, request);
					ResultsMessage rm = null;
					if (timoutInMilli < 0) {
						rm = message.get();
					} else {
						rm = message.get(timoutInMilli, TimeUnit.MILLISECONDS);
					}
			        if (rm.getException() != null) {
			             throw rm.getException();
			        }
			        
			        if (rm.isUpdateResult()) {
			        	results.columnNames = Arrays.asList("update-count");//$NON-NLS-1$
			        	results.dataTypes.add(JDBCSQLTypeInfo.getSQLType("integer"));//$NON-NLS-1$
			        	results.rows = rm.getResultsList(); 
			        	results.lastRow = 1;
			        	results.hasMoreRows = false;
			        	results.close();
			        }
			        else {
			        	results.columnNames = Arrays.asList(rm.getColumnNames());
			        	for (String type:rm.getDataTypes()) {
			        		results.dataTypes.add(JDBCSQLTypeInfo.getSQLType(type));
			        	}
			        	results.rows = rm.getResultsList(); 	
			        	results.lastRow = rm.getLastRow();
				        if (rm.getFinalRow() == -1 || rm.getLastRow() < rm.getFinalRow()) {
				        	results.hasMoreRows = true;
				        }
				        else {
				        	results.close();
				        }
			        }
					return results;
				}
			});
		} catch (Throwable t) {
			throw new TeiidException(t);
		}
	}
}
