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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.modules.Module;
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
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.ConnectionProfile;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.SecurityHelper;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.ODBCSocketListener;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketListener;

public class TransportService implements Service<ClientServiceRegistry>, ClientServiceRegistry {
	private enum Protocol {teiid, pg};
	private ClientServiceRegistryImpl csr;
	private transient ILogon logon;
	private SocketConfiguration socketConfig;
	final ConcurrentMap<String, SecurityDomainContext> securityDomains = new ConcurrentHashMap<String, SecurityDomainContext>();
	private List<String> authenticationDomains;;	
	private long sessionMaxLimit;
	private long sessionExpirationTimeLimit;
	private SocketListener socketListener;
	private transient SessionServiceImpl sessionService;
	private AuthenticationType authenticationType;
	private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
	private boolean embedded;
	private String krb5Domain;
	private InetSocketAddress address = null;
	private String transportName;
	
	private final InjectedValue<SocketBinding> socketBindingInjector = new InjectedValue<SocketBinding>();
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();	
	private final InjectedValue<BufferManager> bufferManagerInjector = new InjectedValue<BufferManager>();
	
	public TransportService(String transportName) {
		this.transportName = transportName;
	}
	
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
	public void waitForFinished(String vdbName, int vdbVersion,
			int timeOutMillis) throws ConnectionException {
		VDBRepository repo = this.vdbRepositoryInjector.getValue();
		repo.waitForFinished(vdbName, vdbVersion, timeOutMillis);
	}
	
	@Override
	public ClassLoader getCallerClassloader() {
		return csr.getCallerClassloader();
	}	

	@Override
	public void start(StartContext context) throws StartException {
		this.csr = new ClientServiceRegistryImpl() {
			@Override
			public ClassLoader getCallerClassloader() {
				return Module.getCallerModule().getClassLoader();
			}
		};
		this.csr.setSecurityHelper(new JBossSecurityHelper());
		
		this.sessionService = new JBossSessionService(this.securityDomains);
		if (this.authenticationDomains != null && !this.authenticationDomains.isEmpty()) {
			this.sessionService.setSecurityDomains(this.authenticationDomains);			
		}
		this.sessionService.setSessionExpirationTimeLimit(this.sessionExpirationTimeLimit);
		this.sessionService.setSessionMaxLimit(this.sessionMaxLimit);
		this.sessionService.setDqp(getDQP());
		this.sessionService.setVDBRepository(getVdbRepository());
		this.sessionService.setSecurityHelper(this.csr.getSecurityHelper());
		this.sessionService.setAuthenticationType(getAuthenticationType());
		this.sessionService.setGssSecurityDomain(this.krb5Domain);
		this.sessionService.start();
		this.csr.setAuthenticationType(this.sessionService.getAuthenticationType());
		
    	// create the necessary services
		this.logon = new LogonImpl(this.sessionService, "teiid-cluster"); //$NON-NLS-1$
		
    	if (this.socketConfig != null) {
    		/*
    		try {
				// this is to show the bound socket port in the JMX console
				SocketBinding socketBinding = getSocketBindingInjector().getValue();
				ManagedServerSocketBinding ss = (ManagedServerSocketBinding)socketBinding.getSocketBindings().getServerSocketFactory().createServerSocket(socketBinding.getName());
				socketBinding.getSocketBindings().getNamedRegistry().registerBinding(ss);
			}  catch (IOException e) {
				throw new StartException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50013));
			}
    		*/
    		this.address = getSocketBindingInjector().getValue().getSocketAddress();
    		Protocol protocol = Protocol.valueOf(socketConfig.getProtocol());
    		boolean sslEnabled = false;
    		if (this.socketConfig.getSSLConfiguration() != null) {
    			sslEnabled = this.socketConfig.getSSLConfiguration().isSslEnabled();
    		}
    		if (protocol == Protocol.teiid) {
    	    	this.socketListener = new SocketListener(address, this.socketConfig, this.csr, getBufferManagerInjector().getValue());
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50012, this.transportName, address.getHostName(), String.valueOf(address.getPort()), (sslEnabled?"ON":"OFF"), authenticationDomains)); //$NON-NLS-1$ //$NON-NLS-2$ 
    		}
    		else if (protocol == Protocol.pg) {
        		getVdbRepository().odbcEnabled();
        		TeiidDriver driver = new TeiidDriver();
        		driver.setEmbeddedProfile(new ConnectionProfile() {
					@Override
					public ConnectionImpl connect(String url, Properties info) throws TeiidSQLException {
						try {
							LocalServerConnection sc = new LocalServerConnection(info, true){
								@Override
								protected ClientServiceRegistry getClientServiceRegistry(String name) {
									return csr;
								}
							};
							return new ConnectionImpl(sc, info, url);
						} catch (CommunicationException e) {
							throw TeiidSQLException.create(e);
						} catch (ConnectionException e) {
							throw TeiidSQLException.create(e);
						}
					}
				});
        		ODBCSocketListener odbc = new ODBCSocketListener(address, this.socketConfig, this.csr, getBufferManagerInjector().getValue(), getMaxODBCLobSizeAllowed(), this.logon, driver);
        		odbc.setAuthenticationType(this.sessionService.getAuthenticationType());
        		this.socketListener = odbc;
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50037, this.transportName, address.getHostName(), String.valueOf(address.getPort()), (sslEnabled?"ON":"OFF"), authenticationDomains)); //$NON-NLS-1$ //$NON-NLS-2$
    		}
    		else {
    			throw new StartException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50013));
    		}
    	}
    	else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50038, LocalServerConnection.jndiNameForRuntime(transportName)));   		
    	}
    			
		DQP dqpProxy = proxyService(DQP.class, getDQP(), LogConstants.CTX_DQP);
    	this.csr.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
    	this.csr.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);    	
	 	this.csr.registerClientService(VDBRepository.class, getVdbRepository(), LogConstants.CTX_DQP);    	
	}

	@Override
	public void stop(StopContext context) {
    	// Stop socket transport(s)
    	if (this.socketListener != null) {
    		this.socketListener.stop();
    		this.socketListener = null;
    	}
    	this.sessionService.stop();
    	
    	if (this.socketConfig != null) {
    		Protocol protocol = Protocol.valueOf(socketConfig.getProtocol());
    		if (protocol == Protocol.teiid) {
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50039, this.transportName, this.address.getHostName(), String.valueOf(this.address.getPort()))); 
    		}
    		else if (protocol == Protocol.pg) {
    	    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50040, this.address.getHostName(), String.valueOf(this.address.getPort())));
    		}
    	}
    	else {
    		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50041, LocalServerConnection.jndiNameForRuntime(transportName))); 
    	}
	}	
	
	/**
	 * Creates an proxy to validate the incoming session
	 */
	private <T> T proxyService(final Class<T> iface, final T instance, String context) {

		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new LogManager.LoggingProxy(instance, context, MessageLevel.TRACE) {

			@Override
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
	
    public List<RequestMetadata> getRequestsUsingVDB(String vdbName, int vdbVersion) {
		List<RequestMetadata> requests = new ArrayList<RequestMetadata>();
		Collection<SessionMetadata> sessions = this.sessionService.getActiveSessions();
		for (SessionMetadata session:sessions) {
			if (vdbName.equals(session.getVDBName()) && session.getVDBVersion() == vdbVersion) {
				requests.addAll(getDQP().getRequestsForSession(session.getSessionId()));
			}
		}
		return requests;
	}	
    
    public void terminateSession(String terminateeId) {
		this.sessionService.terminateSession(terminateeId, DQPWorkContext.getWorkContext().getSessionId());
    }    
    
	public Collection<SessionMetadata> getActiveSessions(){
		return this.sessionService.getActiveSessions();
	}
	
	public int getActiveSessionsCount() throws AdminException{
		try {
			return this.sessionService.getActiveSessionsCount();
		} catch (SessionServiceException e) {
			 throw new AdminComponentException(IntegrationPlugin.Event.TEIID50056, e);
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

	public AuthenticationType getAuthenticationType() {
		return authenticationType;
	}

	public void setAuthenticationType(AuthenticationType authenticationType) {
		this.authenticationType = authenticationType;
	}
	
	public InjectedValue<VDBRepository> getVdbRepositoryInjector() {
		return vdbRepositoryInjector;
	}
	
	private VDBRepository getVdbRepository() {
		return vdbRepositoryInjector.getValue();
	}	

	private DQPCore getDQP() {
		return getDqpInjector().getValue();
	}
	
	public InjectedValue<DQPCore> getDqpInjector() {
		return dqpInjector;
	}	
	
	public InjectedValue<BufferManager> getBufferManagerInjector() {
		return bufferManagerInjector;
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
	
	public void setKrb5Domain(String domain) {
		this.krb5Domain = domain;
	}
}
