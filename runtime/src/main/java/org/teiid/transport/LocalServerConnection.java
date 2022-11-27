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

package org.teiid.transport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.gss.MakeGSS;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.jdbc.LocalProfile;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.Handshake;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;


public class LocalServerConnection implements ServerConnection {
    private static final String TEIID_RUNTIME_CONTEXT = "teiid/queryengine"; //$NON-NLS-1$

    private LogonResult result;
    private boolean shutdown;
    private ClientServiceRegistry csr;
    private DQPWorkContext workContext = new DQPWorkContext();
    private Properties connectionProperties;
    private boolean passthrough;
    private boolean derived;
    private static String serverVersion = new Handshake().getVersion();
    private AutoConnectListener autoConnectListener = null;
    private volatile boolean reconnect;

    private Method cancelMethod;

    public static String jndiNameForRuntime(String embeddedTransportName) {
        return TEIID_RUNTIME_CONTEXT+"/"+embeddedTransportName; //$NON-NLS-1$
    }

    public LocalServerConnection(Properties connectionProperties, boolean useCallingThread) throws CommunicationException, ConnectionException{
        this.connectionProperties = connectionProperties;
        this.csr = getClientServiceRegistry(connectionProperties.getProperty(LocalProfile.TRANSPORT_NAME, "local")); //$NON-NLS-1$

        DQPWorkContext context = (DQPWorkContext)connectionProperties.get(LocalProfile.DQP_WORK_CONTEXT);
        if (context == null) {
            String vdbVersion = connectionProperties.getProperty(TeiidURL.JDBC.VDB_VERSION);
            String vdbName = connectionProperties.getProperty(TeiidURL.JDBC.VDB_NAME);
            VDBKey key = new VDBKey(vdbName, vdbVersion);

            if (!key.isAtMost()) {
                int waitForLoad = PropertiesUtils.getIntProperty(connectionProperties, TeiidURL.CONNECTION.LOGIN_TIMEOUT, -1);
                if (waitForLoad == -1) {
                    waitForLoad = PropertiesUtils.getIntProperty(connectionProperties, LocalProfile.WAIT_FOR_LOAD, -1);
                } else {
                    waitForLoad *= 1000; //seconds to milliseconds
                }
                if (waitForLoad != 0) {
                    this.csr.waitForFinished(key, waitForLoad);
                }
            }

            workContext.setSecurityHelper(csr.getSecurityHelper());
            workContext.setUseCallingThread(useCallingThread);
            SSLSession sslSession = (SSLSession)connectionProperties.get(LocalProfile.SSL_SESSION);
            workContext.setSSLSession(sslSession);
            authenticate();
            passthrough = Boolean.valueOf(connectionProperties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "false")); //$NON-NLS-1$
        } else {
            derived = true;
            workContext = context;
            this.result = new LogonResult(context.getSessionToken(), context.getVdbName(), null);
            passthrough = true;
        }

        try {
            cancelMethod = DQP.class.getMethod("cancelRequest", new Class[] {long.class}); //$NON-NLS-1$
        } catch (SecurityException e) {
            throw new TeiidRuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new TeiidRuntimeException(e);
        }
        boolean autoFailOver = Boolean.parseBoolean(connectionProperties.getProperty(TeiidURL.CONNECTION.AUTO_FAILOVER));
        if (autoFailOver) {
            this.autoConnectListener = new AutoConnectListener();
            addListener(this.autoConnectListener);
        }
    }

    protected ClientServiceRegistry getClientServiceRegistry(String transport) {
        try {
            InitialContext ic = new InitialContext();
            return (ClientServiceRegistry)ic.lookup(jndiNameForRuntime(transport));
        } catch (NamingException e) {
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
        }
    }

    public synchronized void authenticate() throws ConnectionException, CommunicationException {
        Object previousSecurityContext = workContext.getSecurityHelper().associateSecurityContext(workContext.getSession().getSecurityContext());
        try {
            logoff();
        } finally {
            workContext.getSecurityHelper().associateSecurityContext(previousSecurityContext);
        }
        workContext.setSecurityContext(previousSecurityContext);
        try {
            this.result = this.getService(ILogon.class).logon(this.connectionProperties);

            AuthenticationType type = (AuthenticationType) this.result.getProperty(ILogon.AUTH_TYPE);

            if (type != null) {
                //server has issued an additional challenge
                if (type == AuthenticationType.GSS) {
                    try {
                        this.result = MakeGSS.authenticate(this.getService(ILogon.class), this.connectionProperties);
                    } catch (LogonException e) {
                        if (!passthrough) {
                            throw new LogonException(RuntimePlugin.Event.TEIID40150, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40150));
                        }
                        throw e;
                    }
                } else {
                    throw new LogonException(JDBCPlugin.Event.TEIID20034, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20034, type));
                }
            }

        } catch (LogonException e) {
            //TODO: above we make a special check for gss if not passthrough, we could do the same in general here or in sessionserviceimpl

            // Propagate the original message as it contains the message we want
            // to give to the user
             throw new ConnectionException(e);
        } catch (TeiidComponentException e) {
            if (e.getCause() instanceof CommunicationException) {
                throw (CommunicationException)e.getCause();
            }
             throw new CommunicationException(RuntimePlugin.Event.TEIID40069, e);
        }
    }

    public <T> T getService(final Class<T> iface) {
        return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

            boolean logon = iface.equals(ILogon.class);

            public Object invoke(Object arg0, final Method arg1, final Object[] arg2) throws Throwable {
                if (shutdown) {
                    throw ExceptionUtil.convertException(arg1, new TeiidComponentException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40074)));
                }
                try {
                    if (derived) {
                        workContext.setDerived(true);
                    }
                    // check to make sure the current security context same as logged one
                    if (!logon && (reconnect
                            || (passthrough
                                    && !arg1.equals(cancelMethod) // -- it's ok to use another thread to cancel
                                    && !workContext.getSession().isClosed()
                                    //if configured without a security domain the context will be null
                                    && workContext.getSession().getSecurityDomain() != null
                                    && !sameSubject(workContext)))) {
                        //TODO: this is an implicit changeUser - we may want to make this explicit, but that would require pools to explicitly use changeUser
                        LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40115, workContext.getSession().getSessionId()));
                        reconnect = false;
                        authenticate();
                    }

                    final T service = csr.getClientService(iface);
                    return workContext.runInContext(new Callable<Object>() {
                        public Object call() throws Exception {
                            return arg1.invoke(service, arg2);
                        }
                    });
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                } catch (Throwable e) {
                    throw ExceptionUtil.convertException(arg1, e);
                } finally {
                    workContext.setDerived(false);
                }
            }
        }));
    }

    public static boolean sameSubject(DQPWorkContext workContext) {
        Object currentContext = workContext.getSecurityHelper().getSecurityContext(workContext.getSecurityDomain());

        Subject currentUser = null;
        if (currentContext != null) {
            currentUser = workContext.getSecurityHelper().getSubjectInContext(currentContext);
            if (workContext.getSubject() != null && currentUser != null && workContext.getSubject().equals(currentUser)) {
                return true;
            }
            if (workContext.getSecurityContext() == null) {
                return false;
            }
        } else if (workContext.getSecurityContext() != null) {
            return false;
        }
        if (currentUser == null && workContext.getSubject() == null) {
            return true; //unauthenticated
        }

        return false;
    }

    @Override
    public boolean isOpen(long msToTest) {
        if (shutdown) {
            return false;
        }
        try {
            ResultsFuture<?> ping = this.getService(ILogon.class).ping();
            ping.get(msToTest, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void close() {
        if (this.autoConnectListener != null) {
            removeListener(this.autoConnectListener);
        }
        shutdown(true);
    }

    private void shutdown(boolean logoff) {
        if (shutdown) {
            return;
        }

        if (logoff) {
            logoff();
        }
        this.shutdown = true;
    }

    private void logoff() {
        if (derived) {
            return; //not the right place to kill the session
        }
        try {
            //make a best effort to send the logoff
            Future<?> writeFuture = this.getService(ILogon.class).logoff();
            if (writeFuture != null) {
                writeFuture.get(5000, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            //ignore
        }
    }

    public LogonResult getLogonResult() {
        return result;
    }

    @Override
    public boolean isSameInstance(ServerConnection conn) throws CommunicationException {
        return (conn instanceof LocalServerConnection);
    }

    @Override
    public boolean supportsContinuous() {
        return true;
    }

    public DQPWorkContext getWorkContext() {
        return workContext;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    public void addListener(VDBLifeCycleListener listener) {
        VDBRepository repo = csr.getVDBRepository();
        if (repo != null) {
            repo.addListener(listener);
        }
    }

    public void removeListener(VDBLifeCycleListener listener) {
        VDBRepository repo = csr.getVDBRepository();
        if (repo != null) {
            repo.removeListener(listener);
        }
    }

    @Override
    public String getServerVersion() {
        return serverVersion;
    }

    private class AutoConnectListener implements VDBLifeCycleListener {
        @Override
        public void finishedDeployment(String name, CompositeVDB cvdb) {
            VDBMetaData vdb = cvdb.getVDB();
            if (workContext.getSession().getVdb().getName().equals(vdb.getName())
                    && workContext.getSession().getVdb().getVersion().equals(vdb.getVersion())) {
                reconnect = true;
            }
        }
    }
}
