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
package org.teiid.jboss;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.jboss.as.network.SocketBinding;
import org.jboss.modules.Module;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.SessionService;
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
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.ODBCSocketListener;
import org.teiid.transport.SessionCheckingProxy;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.SocketListener;
import org.teiid.transport.WireProtocol;
import org.teiid.vdb.runtime.VDBKey;

public class TransportService extends ClientServiceRegistryImpl implements Service<ClientServiceRegistry> {
    private transient LogonImpl logon;
    private SocketConfiguration socketConfig;
    private SocketListener socketListener;
    private AuthenticationType authenticationType;
    private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
    private boolean local;
    private InetSocketAddress address = null;
    private String transportName;

    private final InjectedValue<SocketBinding> socketBindingInjector = new InjectedValue<SocketBinding>();
    private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
    private final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();
    private final InjectedValue<BufferManager> bufferManagerInjector = new InjectedValue<BufferManager>();
    private final InjectedValue<SessionService> sessionServiceInjector = new InjectedValue<SessionService>();

    public TransportService(String transportName) {
        this.transportName = transportName;
    }

    @Override
    public ClientServiceRegistry getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    @Override
    public void waitForFinished(VDBKey vdbKey,
            int timeOutMillis) throws ConnectionException {
        VDBRepository repo = this.vdbRepositoryInjector.getValue();
        repo.waitForFinished(vdbKey, timeOutMillis);
    }

    @Override
    public ClassLoader getCallerClassloader() {
        return Module.getCallerModule().getClassLoader();
    }

    @Override
    public void start(StartContext context) throws StartException {
        this.setVDBRepository(this.getVdbRepository());
        SessionService ss = sessionServiceInjector.getValue();
        this.setSecurityHelper(ss.getSecurityHelper());

        // create the necessary services
        this.logon = new LogonImpl(ss, "teiid-cluster"); //$NON-NLS-1$

        DQP dqpProxy = proxyService(DQP.class, getDQP(), LogConstants.CTX_DQP);
        this.registerClientService(ILogon.class, logon, LogConstants.CTX_SECURITY);
        this.registerClientService(DQP.class, dqpProxy, LogConstants.CTX_DQP);
        this.setAuthenticationType(ss.getDefaultAuthenticationType());
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
            this.socketConfig.setBindAddress(this.address.getHostName());
            this.socketConfig.setPortNumber(this.address.getPort());
            boolean sslEnabled = false;
            if (this.socketConfig.getSSLConfiguration() != null) {
                sslEnabled = this.socketConfig.getSSLConfiguration().isSslEnabled();
            }
            if (socketConfig.getProtocol() == WireProtocol.teiid) {
                this.socketListener = new SocketListener(address, this.socketConfig, this, getBufferManagerInjector().getValue());
                LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50012, this.transportName, address.getHostName(), String.valueOf(address.getPort()), (sslEnabled?"ON":"OFF"))); //$NON-NLS-1$ //$NON-NLS-2$
            }
            else if (socketConfig.getProtocol() == WireProtocol.pg) {
                TeiidDriver driver = new TeiidDriver();
                driver.setLocalProfile(new ConnectionProfile() {
                    @Override
                    public ConnectionImpl connect(String url, Properties info) throws TeiidSQLException {
                        try {
                            LocalServerConnection sc = new LocalServerConnection(info, true){
                                @Override
                                protected ClientServiceRegistry getClientServiceRegistry(String name) {
                                    return TransportService.this;
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
                ODBCSocketListener odbc = new ODBCSocketListener(address, this.socketConfig, this, getBufferManagerInjector().getValue(), getMaxODBCLobSizeAllowed(), this.logon, driver);
                this.socketListener = odbc;
                LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50037, this.transportName, address.getHostName(), String.valueOf(address.getPort()), (sslEnabled?"ON":"OFF"))); //$NON-NLS-1$ //$NON-NLS-2$
            }
            else {
                throw new StartException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50013));
            }
        }
        else {
            LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50038, LocalServerConnection.jndiNameForRuntime(transportName)));
        }
    }

    @Override
    public void stop(StopContext context) {
        // Stop socket transport(s)
        if (this.socketListener != null) {
            this.socketListener.stop();
            this.socketListener = null;
        }

        if (this.socketConfig != null) {
            if (socketConfig.getProtocol() == WireProtocol.teiid) {
                LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50039, this.transportName, this.address.getHostName(), String.valueOf(this.address.getPort())));
            }
            else if (socketConfig.getProtocol() == WireProtocol.pg) {
                LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50040, this.transportName, this.address.getHostName(), String.valueOf(this.address.getPort())));
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

        return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new SessionCheckingProxy(instance, context, MessageLevel.TRACE)));
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

    @Override
    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    @Override
    public void setAuthenticationType(AuthenticationType authenticationType) {
        this.authenticationType = authenticationType;
    }

    public InjectedValue<SessionService> getSessionServiceInjector() {
        return sessionServiceInjector;
    }

    private int getMaxODBCLobSizeAllowed() {
        return this.maxODBCLobSizeAllowed;
    }

    public void setMaxODBCLobSizeAllowed(int lobSize) {
        this.maxODBCLobSizeAllowed = lobSize;
    }

    public void setLocal(boolean v) {
        this.local = v;
    }

    public boolean isLocal() {
        return this.local;
    }
}
