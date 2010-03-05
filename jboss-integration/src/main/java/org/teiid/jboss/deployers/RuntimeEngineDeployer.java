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

import org.jboss.logging.Logger;
import org.jboss.managed.api.ManagedOperation.Impact;
import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementOperation;
import org.jboss.managed.api.annotation.ManagementParameter;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.managed.api.annotation.ViewUse;
import org.teiid.SecurityHelper;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.adminapi.jboss.AdminProvider;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.transaction.ContainerTransactionProvider;
import org.teiid.dqp.internal.transaction.TransactionServerImpl;
import org.teiid.dqp.internal.transaction.XidFactory;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketTransport;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.DQPManagement;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.jdbc.LogConfigurationProvider;
import com.metamatrix.jdbc.LogListernerProvider;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.SessionService;

@ManagementObject(isRuntime=true, componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class RuntimeEngineDeployer extends DQPConfiguration implements DQPManagement, Serializable , ClientServiceRegistry  {
	private static final long serialVersionUID = -4676205340262775388L;

	protected Logger log = Logger.getLogger(getClass());
	
	private transient SocketConfiguration jdbcSocketConfiguration;
	private transient SocketConfiguration adminSocketConfiguration;	
	private transient SocketTransport jdbcSocket;	
	private transient SocketTransport adminSocket;
	private transient SecurityHelper securityHelper;
		
	private transient DQPCore dqpCore = new DQPCore();
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
	
    public void start() {

    	// create the necessary services
    	createClientServices();
    	
    	this.csr.registerClientService(ILogon.class, proxyService(ILogon.class, logon), com.metamatrix.common.util.LogConstants.CTX_SERVER);
    	this.csr.registerClientService(ClientSideDQP.class, proxyService(ClientSideDQP.class, this.dqpCore), LogConstants.CTX_QUERY_SERVICE);
    	this.csr.registerClientService(Admin.class, proxyService(Admin.class, admin), LogConstants.CTX_ADMIN_API);
    	
    	if (this.jdbcSocketConfiguration.isEnabled()) {
	    	this.jdbcSocket = new SocketTransport(this.jdbcSocketConfiguration, csr);
	    	this.jdbcSocket.start();
	    	log.info("Teiid JDBC = " + (this.jdbcSocketConfiguration.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+this.jdbcSocketConfiguration.getHostAddress().getHostName()+":"+this.jdbcSocketConfiguration.getPortNumber()); //$NON-NLS-1$
    	} else {
    		log.debug(DQPEmbeddedPlugin.Util.getString("SocketTransport.3")); //$NON-NLS-1$
    	}
    	
    	if (this.adminSocketConfiguration.isEnabled()) {
	    	this.adminSocket = new SocketTransport(this.adminSocketConfiguration, csr);
	    	this.adminSocket.start(); 
	    	log.info("Teiid Admin = " + (this.adminSocketConfiguration.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+this.adminSocketConfiguration.getHostAddress().getHostName()+":"+this.adminSocketConfiguration.getPortNumber()); //$NON-NLS-1$
    	} else {
    		log.debug(DQPEmbeddedPlugin.Util.getString("SocketTransport.3")); //$NON-NLS-1$
    	}
    	log.info("Teiid Engine Started = " + new Date(System.currentTimeMillis()).toString()); //$NON-NLS-1$
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
    	
    	log.info("Teiid Engine Stopped = " + new Date(System.currentTimeMillis()).toString()); //$NON-NLS-1$
    }
    
	private void createClientServices() {
		
		this.dqpCore.start(this);
		
		this.logon = new LogonImpl(this.dqpCore.getSessionService(), "teiid-cluster"); //$NON-NLS-1$
    	try {
    		this.admin = AdminProvider.getLocal();
    	} catch (AdminComponentException e) {
    		throw new MetaMatrixRuntimeException(e.getCause());
    	}		        
	}    
	
    private TransactionService getTransactionService(String processName, XATerminator terminator) {
		TransactionServerImpl txnService = new TransactionServerImpl();
		txnService.setTransactionProvider(new ContainerTransactionProvider(terminator));
		txnService.setProcessName(processName);
		txnService.setXidFactory(new XidFactory());
		return (TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, txnService, new Class[] {TransactionService.class}, MessageLevel.DETAIL);
    }	
	
	private <T> T proxyService(final Class<T> iface, final T instance) {

		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

			public Object invoke(Object arg0, Method arg1, Object[] arg2) throws Throwable {
				
				Throwable exception = null;
				ClassLoader current = Thread.currentThread().getContextClassLoader();
				try {
					if (!(iface.equals(ILogon.class))) {					
						logon.assertIdentity(SessionToken.getSession());
						assosiateSecurityContext();
					}
					
					return arg1.invoke(instance, arg2);
				} catch (InvocationTargetException e) {
					exception = e.getTargetException();
				} catch(Throwable t){
					exception = t;
				} finally {
					clearSecurityContext();
					DQPWorkContext.releaseWorkContext();
					Thread.currentThread().setContextClassLoader(current);
				}
				throw ExceptionUtil.convertException(arg1, exception);
			}
		}));
	}	
	
    private boolean assosiateSecurityContext() {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		if (context.getSubject() != null) {
        	return securityHelper.assosiateSecurityContext(context.getSecurityDomain(), context.getSecurityContext());			
		}
		return false;
	}
    
    private void clearSecurityContext() {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		if (context.getSubject() != null) {
			securityHelper.clearSecurityContext(context.getSecurityDomain());			
		}
	}	   
    
	public void setJdbcSocketConfiguration(SocketConfiguration socketConfig) {
		this.jdbcSocketConfiguration = socketConfig;
	}
	
	public void setAdminSocketConfiguration(SocketConfiguration socketConfig) {
		this.adminSocketConfiguration = socketConfig;
	}
    
    public void setXATerminator(XATerminator xaTerminator){
       this.dqpCore.setTransactionService(getTransactionService("localhost", xaTerminator));
    }    
    
    public void setWorkManager(WorkManager mgr) {
    	this.dqpCore.setWorkManager(mgr);
    }
	
	public void setAuthorizationService(AuthorizationService service) {
		this.dqpCore.setAuthorizationService(service);
	}
	
	public void setSessionService(SessionService service) {
		this.dqpCore.setSessionService(service);
	}
	
	public void setBufferService(BufferService service) {
		this.dqpCore.setBufferService(service);
	}
	
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.dqpCore.setConnectorManagerRepository(repo);
	}
	
	public void setSecurityHelper(SecurityHelper helper) {
		this.securityHelper = helper;
		this.dqpCore.setSecurityHelper(helper);
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
		if ("runtime".equalsIgnoreCase(identifier)) {
			return this.dqpCore.getWorkManagerStatistics();
		}
		ConnectorManager cm = this.dqpCore.getConnectorManagerRepository().getConnectorManager(identifier);
		if (cm != null) {
			return cm.getWorkManagerStatistics();
		}
		return null;
	}
	
	@Override
    @ManagementOperation(description="Terminate a Session",params={@ManagementParameter(name="terminateeId",description="The session to be terminated")})
    public void terminateSession(long terminateeId) {
		this.dqpCore.getSessionService().terminateSession(terminateeId, SessionToken.getSession().getSessionID());
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
			return this.dqpCore.getActiveSessions();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	@Override
	@ManagementProperty(description="Active session count", use={ViewUse.STATISTIC}, readOnly=true)
	public int getActiveSessionsCount() throws AdminException{
		try {
			return this.dqpCore.getActiveSessionsCount();
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
