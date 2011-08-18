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
package org.teiid.jboss.deployers;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.security.auth.login.LoginException;
import javax.transaction.TransactionManager;

import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.modules.Module;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.teiid.adminapi.Admin.Cache;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.impl.CacheStatisticsMetadata;
import org.teiid.adminapi.impl.DQPManagement;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.cache.CacheFactory;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DataTierManagerImpl;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.logging.Log4jListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.net.TeiidURL;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.processor.DdlPlan;
import org.teiid.security.SecurityHelper;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.ODBCSocketListener;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketListener;
import org.teiid.vdb.runtime.VDBKey;


public class RuntimeEngineDeployer extends DQPConfiguration implements DQPManagement, Serializable , ClientServiceRegistry, EventDistributor, EventDistributorFactory, Service<ClientServiceRegistry>  {
	private static final long serialVersionUID = -4676205340262775388L;
		
	private transient SocketConfiguration jdbcSocketConfiguration;
	private transient SocketConfiguration odbcSocketConfiguration;
	private transient SocketListener jdbcSocket;	
	private transient SocketListener odbcSocket;
	private transient TransactionServerImpl transactionServerImpl = new TransactionServerImpl();
		
	private transient DQPCore dqpCore = new DQPCore();
	private transient SessionServiceImpl sessionService;
	private transient ILogon logon;
	private transient ClientServiceRegistryImpl csr = new ClientServiceRegistryImpl();	
	private transient VDBRepository vdbRepository;
	private transient TranslatorRepository translatorRepository;

    private long sessionMaxLimit = SessionService.DEFAULT_MAX_SESSIONS;
	private long sessionExpirationTimeLimit = SessionService.DEFAULT_SESSION_EXPIRATION;

	private String eventDistributorName;
	private transient EventDistributor eventDistributor;	
	private transient EventDistributor eventDistributorProxy;
	private transient ContainerLifeCycleListener lifecycleListener;

	// TODO: remove public?
	public final InjectedValue<WorkManager> workManagerInjector = new InjectedValue<WorkManager>();
	public final InjectedValue<XATerminator> xaTerminatorInjector = new InjectedValue<XATerminator>();
	public final InjectedValue<TransactionManager> txnManagerInjector = new InjectedValue<TransactionManager>();
	public final InjectedValue<Executor> threadPoolInjector = new InjectedValue<Executor>();
	public final InjectedValue<SocketBinding> jdbcSocketBindingInjector = new InjectedValue<SocketBinding>();
	public final InjectedValue<BufferServiceImpl> bufferServiceInjector = new InjectedValue<BufferServiceImpl>();
	public final InjectedValue<SocketBinding> odbcSocketBindingInjector = new InjectedValue<SocketBinding>();
	public final ConcurrentMap<String, SecurityDomainContext> securityDomains = new ConcurrentHashMap<String, SecurityDomainContext>();
	private LinkedList<String> securityDomainNames = new LinkedList<String>();

	
    public RuntimeEngineDeployer() {
		// TODO: this does not belong here
		LogManager.setLogListener(new Log4jListener());
    }
	
	@Override
	public <T> T getClientService(Class<T> iface)
			throws ComponentNotFoundException {
		return this.csr.getClientService(iface);
	}
	
	@Override
	public SecurityHelper getSecurityHelper() {
		return this.csr.getSecurityHelper();
	}
	
	@Override
    public void start(StartContext context) {
		setWorkManager(this.workManagerInjector.getValue());
		setXATerminator(xaTerminatorInjector.getValue());
		setTransactionManager(txnManagerInjector.getValue());
		
		this.sessionService = new SessionServiceImpl();
		if (!this.securityDomainNames.isEmpty()) {
			this.sessionService.setSecurityDomains(this.securityDomainNames, this.securityDomains);			
		}
		this.sessionService.setSessionExpirationTimeLimit(this.sessionExpirationTimeLimit);
		this.sessionService.setSessionMaxLimit(this.sessionMaxLimit);
		this.sessionService.setDqp(this.dqpCore);
		this.sessionService.setVDBRepository(this.vdbRepository);
		this.sessionService.start();
		
		this.setBufferService(bufferServiceInjector.getValue());
		
		if (this.jdbcSocketConfiguration != null) {
			this.jdbcSocketConfiguration.setHostAddress(this.jdbcSocketBindingInjector.getValue().getAddress());
			this.jdbcSocketConfiguration.setPortNumber(this.jdbcSocketBindingInjector.getValue().getPort());
		}
		
		if (this.odbcSocketConfiguration != null) {
			this.odbcSocketConfiguration.setHostAddress(this.odbcSocketBindingInjector.getValue().getAddress());
			this.odbcSocketConfiguration.setPortNumber(this.odbcSocketBindingInjector.getValue().getPort());
		}
		
		dqpCore.setTransactionService((TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, transactionServerImpl, new Class[] {TransactionService.class}, MessageLevel.DETAIL, Module.getCallerModule().getClassLoader()));

		if (this.eventDistributorName != null) {
			try {
				InitialContext ic = new InitialContext();
				this.eventDistributor = (EventDistributor) ic.lookup(this.eventDistributorName);
			} catch (NamingException ne) {
				//log at a detail level since we may not be in the all profile
				LogManager.logDetail(LogConstants.CTX_RUNTIME, ne, IntegrationPlugin.Util.getString("jndi_failed", new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
			}
		}
		this.dqpCore.setMetadataRepository(this.vdbRepository.getMetadataRepository());
		this.dqpCore.setEventDistributor(this.eventDistributor);
		this.dqpCore.start(this);
		this.eventDistributorProxy = (EventDistributor)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {
			
			@Override
			public Object invoke(Object proxy, Method method, Object[] args)
					throws Throwable {
				method.invoke(RuntimeEngineDeployer.this, args);
				if (eventDistributor != null) {
					method.invoke(eventDistributor, args);
				}
				return null;
			}
		});
    	// create the necessary services
    	createClientServices();
    	
    	int offset = 0;
    	String portBinding = System.getProperty("jboss.service.binding.set"); //$NON-NLS-1$
    	if (portBinding != null && portBinding.startsWith("ports-")) { //$NON-NLS-1$
    		if (portBinding.equals("ports-default")) { //$NON-NLS-1$
    			offset = 0;
    		}
    		else {
    			try {
					offset = Integer.parseInt(portBinding.substring(portBinding.length()-2))*100;
				} catch (NumberFormatException e) {
					offset = 0;
				}
    		}
    	}
    	
    	this.csr.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
    	DQP dqpProxy = proxyService(DQP.class, this.dqpCore, LogConstants.CTX_DQP);
    	this.csr.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);
    	
    	ClientServiceRegistryImpl jdbcCsr = new ClientServiceRegistryImpl();
    	jdbcCsr.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
    	jdbcCsr.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);
    	
    	if (this.jdbcSocketConfiguration != null) {
	    	this.jdbcSocket = new SocketListener(this.jdbcSocketConfiguration, jdbcCsr, this.dqpCore.getBufferManager(), offset);
	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_enabled","Teiid JDBC = ",(this.jdbcSocketConfiguration.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+this.jdbcSocketConfiguration.getHostAddress().getHostName()+":"+(this.jdbcSocketConfiguration.getPortNumber()+offset))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    	} else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_not_enabled", "jdbc connections")); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	if (this.odbcSocketConfiguration != null) {
    		this.vdbRepository.odbcEnabled();
    		ODBCSocketListener odbc = new ODBCSocketListener(this.odbcSocketConfiguration, this.dqpCore.getBufferManager(), offset, getMaxODBCLobSizeAllowed(), this.logon);
    		odbc.setAuthenticationType(sessionService.getAuthType());
	    	this.odbcSocket = odbc;
	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("odbc_enabled","Teiid ODBC - SSL=", (this.odbcSocketConfiguration.getSSLConfiguration().isSslEnabled()?"ON":"OFF")+" Host = "+this.odbcSocketConfiguration.getHostAddress().getHostName()+" Port = "+(this.odbcSocketConfiguration.getPortNumber()+offset))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    	} else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("odbc_not_enabled")); //$NON-NLS-1$
    	}    	
    	
    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_started", getRuntimeVersion(), new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$

    	// add vdb life cycle listeners
		this.vdbRepository.addListener(new VDBLifeCycleListener() {
			
			private Set<VDBKey> recentlyRemoved = Collections.newSetFromMap(new LRUCache<VDBKey, Boolean>(10000));
			
			@Override
			public void removed(String name, int version) {
				recentlyRemoved.add(new VDBKey(name, version));
			}
			
			@Override
			public void added(String name, int version) {
				if (!recentlyRemoved.remove(new VDBKey(name, version))) {
					return;
				}
				// terminate all the previous sessions
				try {
					Collection<SessionMetadata> sessions = sessionService.getActiveSessions();
					for (SessionMetadata session:sessions) {
						if (name.equalsIgnoreCase(session.getVDBName()) && version == session.getVDBVersion()){
							sessionService.terminateSession(session.getSessionId(), null);
						}
					}
				} catch (SessionServiceException e) {
					//ignore
				}

				// dump the caches. 
				dqpCore.clearCache(Cache.PREPARED_PLAN_CACHE.toString(), name, version);
				dqpCore.clearCache(Cache.QUERY_SERVICE_RESULT_SET_CACHE.toString(), name, version);
			}			
		});
		
		synchronizeMaterializeViews();
	}	
	
	@Override
	public ClientServiceRegistry getValue() throws IllegalStateException, IllegalArgumentException {
		return this;
	}
    
	@Override
    public void stop(StopContext context) {
    	try {
	    	this.dqpCore.stop();
    	} catch(TeiidRuntimeException e) {
    		// this bean is already shutdown
    	}
    	
    	// Stop socket transport(s)
    	if (this.jdbcSocket != null) {
    		this.jdbcSocket.stop();
    		this.jdbcSocket = null;
    	}
    	
    	if (this.odbcSocket != null) {
    		this.odbcSocket.stop();
    		this.odbcSocket = null;
    	}      	
    	
    	this.sessionService.stop();
    	
    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_stopped", new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
    }
    
	private void createClientServices() {
		this.logon = new LogonImpl(this.sessionService, "teiid-cluster"); //$NON-NLS-1$
	}    
	
	/**
	 * Creates an proxy to validate the incoming session
	 */
	private <T> T proxyService(final Class<T> iface, final T instance, String context) {

		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new LogManager.LoggingProxy(instance, context, MessageLevel.TRACE) {

			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				Throwable exception = null;
				try {
					sessionService.validateSession(DQPWorkContext.getWorkContext().getSessionId());
					return super.invoke(proxy, method, args);
				} catch (InvocationTargetException e) {
					exception = e.getTargetException();
				} catch(Throwable t){
					exception = t;
				}
				throw ExceptionUtil.convertException(method, exception);
			}
		}));
	}
	
	public void setJdbcSocketConfiguration(SocketConfiguration socketConfig) {
		this.jdbcSocketConfiguration = socketConfig;
	}
	
	public void setOdbcSocketConfiguration(SocketConfiguration socketConfig) {
		this.odbcSocketConfiguration = socketConfig;
	}
    
    public void setXATerminator(XATerminator xaTerminator){
    	this.transactionServerImpl.setXaTerminator(xaTerminator);
    }   
    
    public void setTransactionManager(TransactionManager transactionManager) {
    	this.transactionServerImpl.setTransactionManager(transactionManager);
    }
    
    public void setWorkManager(WorkManager mgr) {
    	this.transactionServerImpl.setWorkManager(mgr);
    }
		
	public void setBufferService(BufferService service) {
		this.dqpCore.setBufferService(service);
	}
	
	public void setSecurityHelper(SecurityHelper helper) {
		this.csr.setSecurityHelper(helper);
	}
	
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	@Override
    public List<RequestMetadata> getRequestsForSession(String sessionId) {
		return this.dqpCore.getRequestsForSession(sessionId);
	}
	
	@Override
    public List<RequestMetadata> getRequestsUsingVDB(String vdbName, int vdbVersion) throws AdminException {
		List<RequestMetadata> requests = new ArrayList<RequestMetadata>();
		try {
			Collection<SessionMetadata> sessions = this.sessionService.getActiveSessions();
			for (SessionMetadata session:sessions) {
				if (session.getVDBName().equals(vdbName) && session.getVDBVersion() == vdbVersion) {
					requests.addAll(this.dqpCore.getRequestsForSession(session.getSessionId()));
				}
			}
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
		return requests;
	}
	
    
	@Override
    public List<RequestMetadata> getRequests() {
		return this.dqpCore.getRequests();
	}
	
	@Override
    public List<RequestMetadata> getLongRunningRequests() {
		return this.dqpCore.getLongRunningRequests();
	}
	
	@Override
    public WorkerPoolStatisticsMetadata getWorkerPoolStatistics(){
		return this.dqpCore.getWorkerPoolStatistics();
	}
	
	@Override
    public void terminateSession(String terminateeId) {
		this.sessionService.terminateSession(terminateeId, DQPWorkContext.getWorkContext().getSessionId());
    }
    
	@Override
    public boolean cancelRequest(String sessionId, long executionId) throws AdminException {
    	try {
			return this.dqpCore.cancelRequest(sessionId, executionId);
		} catch (TeiidComponentException e) {
			throw new AdminComponentException(e);
		}
    }
    
	@Override
    public Collection<String> getCacheTypes(){
		return this.dqpCore.getCacheTypes();
	}
	
	@Override
	public void clearCache(String cacheType) {
		this.dqpCore.clearCache(cacheType);
	}
	
	@Override
	public void clearCache(String cacheType, String vdbName, int version) {
		this.dqpCore.clearCache(cacheType, vdbName, version);
	}	
	
	@Override
	public CacheStatisticsMetadata getCacheStatistics(String cacheType) {
		return this.dqpCore.getCacheStatistics(cacheType);
	}
	
	@Override
	public Collection<SessionMetadata> getActiveSessions() throws AdminException {
		try {
			return this.sessionService.getActiveSessions();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	public int getActiveSessionsCount() throws AdminException{
		try {
			return this.sessionService.getActiveSessionsCount();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	public Collection<TransactionMetadata> getTransactions() {
		return this.dqpCore.getTransactions();
	}
	
	@Override
	public void terminateTransaction(String xid) throws AdminException {
		this.dqpCore.terminateTransaction(xid);
	}

	@Override
	public void mergeVDBs(String sourceVDBName, int sourceVDBVersion,
			String targetVDBName, int targetVDBVersion) throws AdminException {
		this.vdbRepository.mergeVDBs(sourceVDBName, sourceVDBVersion, targetVDBName, targetVDBVersion);
	}	
	
	public void setCacheFactory(CacheFactory factory) {
		this.dqpCore.setCacheFactory(factory);
	}
	
	@Override
	public List<List> executeQuery(final String vdbName, final int version, final String command, final long timoutInMilli) throws AdminException {
		
		String user = "JOPR ADMIN"; //$NON-NLS-1$
		LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("admin_executing", user, command)); //$NON-NLS-1$
		
		SessionMetadata session = createTemporarySession(vdbName, version, user);

		final long requestID =  0L;
		
		DQPWorkContext context = new DQPWorkContext();
		context.setSession(session);
		
		try {
			return context.runInContext(new Callable<List<List>>() {
				@Override
				public List<List> call() throws Exception {
					ArrayList<List> results = new ArrayList<List>();
					
					long start = System.currentTimeMillis();
					RequestMessage request = new RequestMessage(command);
					request.setExecutionId(0L);
					request.setRowLimit(getMaxRowsFetchSize()); // this would limit the number of rows that are returned.
					Future<ResultsMessage> message = dqpCore.executeRequest(requestID, request);
					ResultsMessage rm = null;
					if (timoutInMilli < 0) {
						rm = message.get();
					} else {
						rm = message.get(timoutInMilli, TimeUnit.MILLISECONDS);
					}
			        if (rm.getException() != null) {
			            throw new AdminProcessingException(rm.getException());
			        }
			        
			        if (rm.isUpdateResult()) {
			        	results.addAll(new ArrayList(Arrays.asList("update count"))); //$NON-NLS-1$
			        	results.addAll(Arrays.asList(rm.getResults()));			        	
			        }
			        else {
				        results.addAll(new ArrayList(Arrays.asList(rm.getColumnNames())));
				        results.addAll(Arrays.asList(fixResults(rm.getResults())));
				        
				        while (rm.getFinalRow() == -1 || rm.getLastRow() < rm.getFinalRow()) {
				        	long elapsed = System.currentTimeMillis() - start;
							message = dqpCore.processCursorRequest(requestID, rm.getLastRow()+1, 1024);
							rm = message.get(timoutInMilli-elapsed, TimeUnit.MILLISECONDS);
							results.addAll(Arrays.asList(fixResults(rm.getResults())));
				        }
			        }

			        long elapsed = System.currentTimeMillis() - start;
			        ResultsFuture<?> response = dqpCore.closeRequest(requestID);
			        response.get(timoutInMilli-elapsed, TimeUnit.MILLISECONDS);
					return results;
				}
			});
		} catch (Throwable t) {
			throw new AdminProcessingException(t);
		} finally {
			try {
				sessionService.closeSession(session.getSessionId());
			} catch (InvalidSessionException e) { //ignore
			}			
		}
	}

	private SessionMetadata createTemporarySession(final String vdbName, final int version, final String user)
			throws AdminProcessingException {
		Properties properties = new Properties();
		properties.setProperty(TeiidURL.JDBC.VDB_NAME, vdbName);
		properties.setProperty(TeiidURL.JDBC.VDB_VERSION, String.valueOf(version));
		
		SessionMetadata session = null;
		try {
			session = this.sessionService.createSession(user, null, "JOPR", properties, false); //$NON-NLS-1$
		} catch (SessionServiceException e1) {
			throw new AdminProcessingException(e1);
		} catch (LoginException e1) {
			throw new AdminProcessingException(e1);
		}
		return session;
	}	
	
	/**
	 * Managed Object framework has bug that does not currently allow 
	 * sending a NULL in the Collection Value, so sending literal string "null". 
	 * If you send them as Array Value, the MO is packaged as composite object and would like 
	 * all the elements in array to be same type which is not the case with results. 
	 */
	List[] fixResults(List[] rows) throws SQLException {
		List[] newResults = new List[rows.length];
		
		for(int i = 0; i < rows.length; i++) {
			List row = rows[i];
			ArrayList<Object> newRow = new ArrayList<Object>();
			for (Object col:row) {
				if (col == null) {
					newRow.add("null"); //$NON-NLS-1$ 
				}
				else {
					if (col instanceof Number || col instanceof String || col instanceof Character) {
						newRow.add(col);
					}
					else if (col instanceof Blob) {
						newRow.add("blob"); //$NON-NLS-1$
					}
					else if (col instanceof Clob) {
						newRow.add("clob"); //$NON-NLS-1$
					}
					else if (col instanceof SQLXML) {
						SQLXML xml = (SQLXML)col;
						newRow.add(xml.getString());
					}
					else {
						newRow.add(col.toString());
					}
				}
			}
			newResults[i] = newRow;
		}		
		return newResults;
	}
	
	public String getEventDistributorName() {
		return eventDistributorName;
	}
	
	public void setEventDistributorName(String eventDistributorName) {
		this.eventDistributorName = eventDistributorName;
	}
	
	@Override
	public void updateMatViewRow(String vdbName, int vdbVersion, String schema,
			String viewName, List<?> tuple, boolean delete) {
		this.dqpCore.updateMatViewRow(getcontextProvider(), vdbName, vdbVersion, schema, viewName, tuple, delete);
	}
	
	@Override
	public void refreshMatView(final String vdbName, final int vdbVersion, final String viewName) {
		this.dqpCore.refreshMatView(getcontextProvider(), vdbName, vdbVersion, viewName);
	}
	
	@Override
	public void dataModification(String vdbName, int vdbVersion, String schema,
			String... tableNames) {
		updateModified(true, vdbName, vdbVersion, schema, tableNames);
	}
	
	private void updateModified(boolean data, String vdbName, int vdbVersion, String schema,
			String... objectNames) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		long ts = System.currentTimeMillis();
		for (String name:objectNames) {
			Table table = s.getTables().get(name.toUpperCase());
			if (table == null) {
				continue;
			}
			if (data) {
				table.setLastDataModification(ts);
			} else {
				table.setLastModified(ts);
			}
		}
	}
	
	@Override
	public void setColumnStats(String vdbName, int vdbVersion,
			String schemaName, String tableName, String columnName,
			ColumnStats stats) {
		Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		for (Column c : t.getColumns()) {
			if (c.getName().equalsIgnoreCase(columnName)) {
				c.setColumnStats(stats);
				t.setLastModified(System.currentTimeMillis());
				break;
			}
		}
	}
	
	@Override
	public void setTableStats(String vdbName, int vdbVersion,
			String schemaName, String tableName, TableStats stats) {
		Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		t.setTableStats(stats);
		t.setLastModified(System.currentTimeMillis());
	}

	private Table getTable(String vdbName, int vdbVersion, String schemaName,
			String tableName) {
		Schema s = getSchema(vdbName, vdbVersion, schemaName);
		if (s == null) {
			return null;
		}
		return s.getTables().get(tableName.toUpperCase());
	}

	private Schema getSchema(String vdbName, int vdbVersion, String schemaName) {
		VDBMetaData vdb = this.vdbRepository.getVDB(vdbName, vdbVersion);
		if (vdb == null) {
			return null;
		}
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		if (tm == null) {
			return null;
		}
		return tm.getMetadataStore().getSchemas().get(schemaName.toUpperCase());
	}
	
	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
			String schema, String viewName, TriggerEvent triggerEvent,
			String triggerDefinition, Boolean enabled) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterInsteadOfTrigger(this.vdbRepository.getVDB(vdbName, vdbVersion), t, triggerDefinition, enabled, triggerEvent);
	}
	
	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,
			String schema, String procName, String definition) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		Procedure p = s.getProcedures().get(procName.toUpperCase());
		if (p == null) {
			return;
		}
		DdlPlan.alterProcedureDefinition(this.vdbRepository.getVDB(vdbName, vdbVersion), p, definition);
	}
	
	@Override
	public void setViewDefinition(String vdbName, int vdbVersion,
			String schema, String viewName, String definition) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterView(this.vdbRepository.getVDB(vdbName, vdbVersion), t, definition);
	}
	
	@Override
	public void setProperty(String vdbName, int vdbVersion, String uuid,
			String name, String value) {
		VDBMetaData vdb = this.vdbRepository.getVDB(vdbName, vdbVersion);
		if (vdb == null) {
			return;
		}
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		if (tm == null) {
			return;
		}
		AbstractMetadataRecord record = DataTierManagerImpl.getByUuid(tm.getMetadataStore(), uuid);
		if (record != null) {
			record.setProperty(name, value);
		}
	}
	
	@Override
	public EventDistributor getEventDistributor() {
		return this.eventDistributorProxy;
	}
	
	private void synchronizeMaterializeViews() {
		this.lifecycleListener.addListener(new ContainerLifeCycleListener.LifeCycleEventListener() {
			@Override
			public void onStartupFinish() {
				dqpCore.synchronizeInternalMaterializedViews(getcontextProvider());
			}
			@Override
			public void onShutdownStart() {
			}
		});
	}
	
	public String getRuntimeVersion() {
		return ApplicationInfo.getInstance().getBuildNumber();
	}
	
	public void setSessionMaxLimit(long limit) {
		this.sessionMaxLimit = limit;
	}
		
	public void setSessionExpirationTimeLimit(long limit) {
		this.sessionExpirationTimeLimit = limit;
	}		
	
	public void addSecurityDomain(String domain) {
		this.securityDomainNames.add(domain);
	}
	
	public List<VDBMetaData> getVDBs(){
		return this.vdbRepository.getVDBs();
	}
	
	public VDBMetaData getVDB(String vdbName, int version){
		return this.vdbRepository.getVDB(vdbName, version);
	}	
	
	public List<VDBTranslatorMetaData> getTranslators(){
		return this.translatorRepository.getTranslators();
	}

	public VDBTranslatorMetaData getTranslator(String translatorName) {
		return (VDBTranslatorMetaData)this.translatorRepository.getTranslatorMetaData(translatorName);
	}
	
	public void setTranslatorRepository(TranslatorRepository translatorRepo) {
		this.translatorRepository = translatorRepo;
	}

	private DQPCore.ContextProvider getcontextProvider() {
		return new DQPCore.ContextProvider() {
			@Override
			public DQPWorkContext getContext(final String vdbName, final int vdbVersion) {
				return new DQPWorkContext() {
					public VDBMetaData getVDB() {
						return vdbRepository.getVDB(vdbName, vdbVersion);
					}
				    public String getVdbName() {
				        return vdbName;
				    }
				    public int getVdbVersion() {
				        return vdbVersion;
				    }					
				};
			}
		};
	}
	
	public void setContainerLifeCycleListener(ContainerLifeCycleListener listener) {
		this.lifecycleListener = listener;
	}	
}
