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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.security.SecurityHelper;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.ODBCSocketListener;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketListener;
import org.teiid.vdb.runtime.VDBKey;

public class Transport implements Service<ClientServiceRegistry>, ClientServiceRegistry {
	private enum Protocol {teiid, pg};
	private ClientServiceRegistryImpl csr = new ClientServiceRegistryImpl();
	private transient ILogon logon;
	private SocketConfiguration socketConfig;
	final ConcurrentMap<String, SecurityDomainContext> securityDomains = new ConcurrentHashMap<String, SecurityDomainContext>();
	private List<String> authenticationDomains;;	
	private long sessionMaxLimit;
	private long sessionExpirationTimeLimit;
	private SocketListener socketListener;
	private transient SessionServiceImpl sessionService;
	private String authenticationType;
	private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
	private boolean embedded;
	
	private final InjectedValue<SocketBinding> socketBindingInjector = new InjectedValue<SocketBinding>();
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<SessionAwareCache> preparedPlanCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<SessionAwareCache> resultSetCacheInjector = new InjectedValue<SessionAwareCache>();	
	private final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();	
	private final InjectedValue<BufferServiceImpl> bufferServiceInjector = new InjectedValue<BufferServiceImpl>();
	
	@Override
	public <T> T getClientService(Class<T> iface) throws ComponentNotFoundException {
		return csr.getClientService(iface);
	}

	@Override
	public SecurityHelper getSecurityHelper() {
		return csr.getSecurityHelper();
	}
	
	@Override
	public ClientServiceRegistry getValue() throws IllegalStateException, IllegalArgumentException {
		return this;
	}

	@Override
	public void start(StartContext context) throws StartException {
		this.csr.setSecurityHelper(new JBossSecurityHelper());
		
		this.sessionService = new SessionServiceImpl();
		if (this.authenticationDomains != null && !this.authenticationDomains.isEmpty()) {
			this.sessionService.setSecurityDomains(this.authenticationDomains, this.securityDomains);			
		}
		this.sessionService.setSessionExpirationTimeLimit(this.sessionExpirationTimeLimit);
		this.sessionService.setSessionMaxLimit(this.sessionMaxLimit);
		this.sessionService.setDqp(getDQP());
		this.sessionService.setVDBRepository(getVdbRepository());
		this.sessionService.setSecurityHelper(this.csr.getSecurityHelper());
		this.sessionService.start();
		
    	// create the necessary services
		this.logon = new LogonImpl(this.sessionService, "teiid-cluster"); //$NON-NLS-1$
		
    	if (this.socketConfig != null) {
    		InetSocketAddress address = getSocketBindingInjector().getValue().getSocketAddress();
    		Protocol protocol = Protocol.valueOf(socketConfig.getProtocol());
    		if (protocol == Protocol.teiid) {
    	    	this.socketListener = new SocketListener(address, this.socketConfig, this.csr, getBufferServiceInjector().getValue().getBufferManager());
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("socket_enabled","Teiid JDBC = ",(this.socketConfig.getSSLConfiguration().isSslEnabled()?"mms://":"mm://")+address.getHostName()+":"+address.getPort())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    		}
    		else if (protocol == Protocol.pg) {
        		getVdbRepository().odbcEnabled();
        		
        		ODBCSocketListener odbc = new ODBCSocketListener(address, this.socketConfig, this.csr, getBufferServiceInjector().getValue().getBufferManager(), getMaxODBCLobSizeAllowed(), this.logon);
        		if (getAuthenticationType() != null) {
	        		this.sessionService.setAuthenticationType(getAuthenticationType());
	        		odbc.setAuthenticationType(this.sessionService.getAuthType());
        		}
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("odbc_enabled","Teiid ODBC - SSL=", (this.socketConfig.getSSLConfiguration().isSslEnabled()?"ON":"OFF")+" Host = "+address.getHostName()+" Port = "+address.getPort())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    		}
    		else {
    			throw new StartException(IntegrationPlugin.Util.getString("wrong_protocol")); //$NON-NLS-1$
    		}
    	}
    	else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("embedded_enabled", LocalServerConnection.TEIID_RUNTIME_CONTEXT)); //$NON-NLS-1$    		
    	}
    			
		DQP dqpProxy = proxyService(DQP.class, getDQP(), LogConstants.CTX_DQP);
    	this.csr.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
    	this.csr.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);
    	
    	// add vdb life cycle listeners
    	getVdbRepository().addListener(new VDBLifeCycleListener() {
			
			private Set<VDBKey> recentlyRemoved = Collections.newSetFromMap(new LRUCache<VDBKey, Boolean>(10000));
			
			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				recentlyRemoved.add(new VDBKey(name, version));
			}
			
			@Override
			public void added(String name, int version, CompositeVDB vdb) {
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
				getResultSetCacheInjector().getValue().clearForVDB(name, version);
				getPreparedPlanCacheInjector().getValue().clearForVDB(name, version);
			}			
		});    	
	}

	@Override
	public void stop(StopContext context) {
    	// Stop socket transport(s)
    	if (this.socketListener != null) {
    		this.socketListener.stop();
    		this.socketListener = null;
    	}
    	this.sessionService.stop();
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
	
    public List<RequestMetadata> getRequestsUsingVDB(String vdbName, int vdbVersion) throws AdminException {
		List<RequestMetadata> requests = new ArrayList<RequestMetadata>();
		try {
			Collection<SessionMetadata> sessions = this.sessionService.getActiveSessions();
			for (SessionMetadata session:sessions) {
				if (session.getVDBName().equals(vdbName) && session.getVDBVersion() == vdbVersion) {
					requests.addAll(getDQP().getRequestsForSession(session.getSessionId()));
				}
			}
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
		return requests;
	}	
    
    public void terminateSession(String terminateeId) {
		this.sessionService.terminateSession(terminateeId, DQPWorkContext.getWorkContext().getSessionId());
    }    
    
	public Collection<SessionMetadata> getActiveSessions() throws AdminException {
		try {
			return this.sessionService.getActiveSessions();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}
	
	public int getActiveSessionsCount() throws AdminException{
		try {
			return this.sessionService.getActiveSessionsCount();
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		}
	}	
	
	public InjectedValue<SocketBinding> getSocketBindingInjector() {
		return this.socketBindingInjector;
	}

	public SocketConfiguration getSocketConfig() {
		return socketConfig;
	}

	public void setSocketConfig(SocketConfiguration socketConfig) {
		this.socketConfig = socketConfig;
	}

	public List<String> getAuthenticationDomains() {
		return authenticationDomains;
	}

	public void setAuthenticationDomains(List<String> authenticationDomains) {
		this.authenticationDomains = new LinkedList(authenticationDomains);
	}
	
	public void setSessionMaxLimit(long limit) {
		this.sessionMaxLimit = limit;
	}	

	public void setSessionExpirationTimeLimit(long limit) {
		this.sessionExpirationTimeLimit = limit;
	}

	public String getAuthenticationType() {
		return authenticationType;
	}

	public void setAuthenticationType(String authenticationType) {
		this.authenticationType = authenticationType;
	}
	
	public InjectedValue<VDBRepository> getVdbRepositoryInjector() {
		return vdbRepositoryInjector;
	}
	
	private VDBRepository getVdbRepository() {
		return vdbRepositoryInjector.getValue();
	}	
	
	public InjectedValue<SessionAwareCache> getResultSetCacheInjector() {
		return resultSetCacheInjector;
	}
	
	public InjectedValue<SessionAwareCache> getPreparedPlanCacheInjector() {
		return preparedPlanCacheInjector;
	}

	private DQPCore getDQP() {
		return getDqpInjector().getValue();
	}
	
	public InjectedValue<DQPCore> getDqpInjector() {
		return dqpInjector;
	}	
	
	public InjectedValue<BufferServiceImpl> getBufferServiceInjector() {
		return bufferServiceInjector;
	}	
	
	private int getMaxODBCLobSizeAllowed() {
		return this.maxODBCLobSizeAllowed;
	}
	
	public void setMaxODBCLobSizeAllowed(int lobSize) {
		this.maxODBCLobSizeAllowed = lobSize;
	}

	public void setEmbedded(boolean v) {
		this.embedded = v;
	}
	
	public boolean isEmbedded() {
		return this.embedded;
	}
}
