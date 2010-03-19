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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.managed.api.ManagedOperation.Impact;
import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementOperation;
import org.jboss.managed.api.annotation.ManagementParameter;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.managed.api.annotation.ViewUse;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.DQPManagement;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.adminapi.jboss.AdminProvider;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.transaction.TransactionServerImpl;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.logging.LogConfigurationProvider;
import org.teiid.logging.LogListernerProvider;
import org.teiid.security.SecurityHelper;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketTransport;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.client.DQP;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.SessionService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.platform.security.api.ILogon;

@ManagementObject(isRuntime=true, componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class RuntimeEngineDeployer extends DQPConfiguration implements DQPManagement, Serializable , ClientServiceRegistry  {
	private static final long serialVersionUID = -4676205340262775388L;

	private transient SocketConfiguration jdbcSocketConfiguration;
	private transient SocketConfiguration adminSocketConfiguration;	
	private transient SocketTransport jdbcSocket;	
	private transient SocketTransport adminSocket;
	private transient TransactionServerImpl transactionServerImpl = new TransactionServerImpl();
		
	private transient DQPCore dqpCore = new DQPCore();
	private transient SessionService sessionService;
	private transient ILogon logon;
	private transient Admin admin;
	private transient ClientServiceRegistryImpl csr = new ClientServiceRegistryImpl();	

    public RuntimeEngineDeployer() {
		// TODO: this does not belong here
		LogManager.setLogConfiguration(new LogConfigurationProvider().get());
		LogManager.setLogListener(new LogListernerProvider().get());
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
	
    public void start() {
		dqpCore.setTransactionService((TransactionService)LogManager.createLoggingProxy(com.metamatrix.common.log.LogConstants.CTX_TXN_LOG, transactionServerImpl, new Class[] {TransactionService.class}, MessageLevel.DETAIL));

    	// create the necessary services
    	createClientServices();
    	
    	this.csr.registerClientService(ILogon.class, logon, com.metamatrix.common.log.LogConstants.CTX_SESSION);
    	this.csr.registerClientService(DQP.class, proxyService(DQP.class, this.dqpCore), com.metamatrix.common.log.LogConstants.CTX_DQP);
    	this.csr.registerClientService(Admin.class, proxyService(Admin.class, admin), com.metamatrix.common.log.LogConstants.CTX_ADMIN_API);
    	
    	if (this.jdbcSocketConfiguration.isEnabled()) {
	    	this.jdbcSocket = new SocketTransport(this.jdbcSocketConfiguration, csr);
	    	this.jdbcSocket.start();
	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_enabled","Teiid JDBC = ",(this.jdbcSocketConfiguration.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+this.jdbcSocketConfiguration.getHostAddress().getHostName()+":"+this.jdbcSocketConfiguration.getPortNumber())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    	} else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_not_enabled", "jdbc connections")); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	if (this.adminSocketConfiguration.isEnabled()) {
	    	this.adminSocket = new SocketTransport(this.adminSocketConfiguration, csr);
	    	this.adminSocket.start(); 
	    	LogManager.logInfo(com.metamatrix.common.log.LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_enabled","Teiid Admin", (this.adminSocketConfiguration.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+this.adminSocketConfiguration.getHostAddress().getHostName()+":"+this.adminSocketConfiguration.getPortNumber())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    	} else {
    		LogManager.logInfo(com.metamatrix.common.log.LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_not_enabled", "admin connections")); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	LogManager.logInfo(com.metamatrix.common.log.LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_started", new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
	}	
    
    public void stop() {
    	
    	try {
	    	this.dqpCore.stop();
    	} catch(MetaMatrixRuntimeException e) {
    		// this bean is already shutdown
    	}
    	
    	// Stop socket transport(s)
    	if (this.jdbcSocket != null) {
    		this.jdbcSocket.stop();
    		this.jdbcSocket = null;
    	}
    	
    	if (this.adminSocket != null) {
    		this.adminSocket.stop();
    		this.adminSocket = null;
    	}    	
    	LogManager.logInfo(com.metamatrix.common.log.LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_stopped", new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
    }
    
	private void createClientServices() {
		
		this.dqpCore.start(this);
		
		this.logon = new LogonImpl(this.sessionService, "teiid-cluster"); //$NON-NLS-1$
    	try {
    		this.admin = AdminProvider.getLocal();
    	} catch (AdminComponentException e) {
    		throw new MetaMatrixRuntimeException(e.getCause());
    	}		        
	}    
	
	/**
	 * Creates an proxy to validate the incoming session
	 */
	private <T> T proxyService(final Class<T> iface, final T instance) {

		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

			public Object invoke(Object arg0, Method arg1, Object[] arg2) throws Throwable {
				Throwable exception = null;
				try {
					sessionService.validateSession(DQPWorkContext.getWorkContext().getSessionId());
					return arg1.invoke(instance, arg2);
				} catch (InvocationTargetException e) {
					exception = e.getTargetException();
				} catch(Throwable t){
					exception = t;
				}
				throw ExceptionUtil.convertException(arg1, exception);
			}
		}));
	}
	
	public void setJdbcSocketConfiguration(SocketConfiguration socketConfig) {
		this.jdbcSocketConfiguration = socketConfig;
	}
	
	public void setAdminSocketConfiguration(SocketConfiguration socketConfig) {
		this.adminSocketConfiguration = socketConfig;
	}
    
    public void setXATerminator(XATerminator xaTerminator){
    	this.transactionServerImpl.setXaTerminator(xaTerminator);
    }   
    
    public void setTransactionManager(TransactionManager transactionManager) {
    	this.transactionServerImpl.setTransactionManager(transactionManager);
    }
    
    public void setWorkManager(WorkManager mgr) {
    	this.dqpCore.setWorkManager(mgr);
    }
	
	public void setSessionService(SessionService service) {
		this.sessionService = service;
		service.setDqp(this.dqpCore);
	}
	
	public void setBufferService(BufferService service) {
		this.dqpCore.setBufferService(service);
	}
	
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.dqpCore.setConnectorManagerRepository(repo);
	}
	
	public void setSecurityHelper(SecurityHelper helper) {
		this.csr.setSecurityHelper(helper);
	}
	
	@Override
    @ManagementOperation(description="Requests for perticular session", impact=Impact.ReadOnly,params={@ManagementParameter(name="sessionId",description="The session Identifier")})
    public List<RequestMetadata> getRequestsForSession(long sessionId) {
		return this.dqpCore.getRequestsForSession(sessionId);
	}
    
	@Override
    @ManagementOperation(description="Active requests", impact=Impact.ReadOnly)
    public List<RequestMetadata> getRequests() {
		return this.dqpCore.getRequests();
	}
	
	@Override
	@ManagementOperation(description="Get Runtime workmanager statistics", impact=Impact.ReadOnly,params={@ManagementParameter(name="identifier",description="Use \"runtime\" for engine, or connector name for connector")})
    public WorkerPoolStatisticsMetadata getWorkManagerStatistics(String identifier) {
		if ("runtime".equalsIgnoreCase(identifier)) { //$NON-NLS-1$
			return this.dqpCore.getWorkManagerStatistics();
		}
		/*ConnectorManager cm = this.dqpCore.getConnectorManagerRepository().getConnectorManager(identifier);
		if (cm != null) {
			return cm.getWorkManagerStatistics();
		}*/
		return null;
	}
	
	@Override
    @ManagementOperation(description="Terminate a Session",params={@ManagementParameter(name="terminateeId",description="The session to be terminated")})
    public void terminateSession(long terminateeId) {
		this.sessionService.terminateSession(terminateeId, DQPWorkContext.getWorkContext().getSessionId());
    }
    
	@Override
    @ManagementOperation(description="Cancel a Request",params={@ManagementParameter(name="sessionId",description="The session Identifier"), @ManagementParameter(name="requestId",description="The request Identifier")})    
    public boolean cancelRequest(long sessionId, long requestId) throws AdminException {
    	try {
			return this.dqpCore.cancelRequest(sessionId, requestId);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }
    
	@Override
    @ManagementOperation(description="Get Cache types in the system", impact=Impact.ReadOnly)
    public Collection<String> getCacheTypes(){
		return this.dqpCore.getCacheTypes();
	}
	
	@Override
	@ManagementOperation(description="Clear the caches in the system", impact=Impact.ReadOnly)
	public void clearCache(String cacheType) {
		this.dqpCore.clearCache(cacheType);
	}
	
	@Override
	@ManagementOperation(description="Active sessions", impact=Impact.ReadOnly)
	public Collection<SessionMetadata> getActiveSessions() throws AdminException {
		try {
			return this.sessionService.getActiveSessions();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	@ManagementProperty(description="Active session count", use={ViewUse.STATISTIC}, readOnly=true)
	public int getActiveSessionsCount() throws AdminException{
		try {
			return this.sessionService.getActiveSessionsCount();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	@ManagementOperation(description="Active Transactions", impact=Impact.ReadOnly)
	public Collection<org.teiid.adminapi.Transaction> getTransactions() {
		return this.dqpCore.getTransactions();
	}
	
	@Override
	@ManagementOperation(description="Clear the caches in the system", impact=Impact.ReadOnly)
	public void terminateTransaction(String xid) throws AdminException {
		this.dqpCore.terminateTransaction(xid);
	}	
}
