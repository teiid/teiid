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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSCredential;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Base64;
import org.teiid.core.util.LRUCache;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DQPWorkContext.Version;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.jdbc.BaseDataSource;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.CommunicationException;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;


public class LogonImpl implements ILogon {

    private SessionService service;
    private String clusterName;
    protected Map<String, Object> gssServiceTickets = Collections.synchronizedMap(new LRUCache<String, Object>());

    public LogonImpl(SessionService service, String clusterName) {
        this.service = service;
        this.clusterName = clusterName;
    }

    public LogonResult logon(Properties connProps) throws LogonException {
        String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
        String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
        String user = connProps.getProperty(TeiidURL.CONNECTION.USER_NAME, CoreConstants.DEFAULT_ANON_USERNAME);
        boolean onlyAllowPassthrough = Boolean.valueOf(connProps.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION,
                "false")); //$NON-NLS-1$

        AuthenticationType authType = AuthenticationType.USERPASSWORD;

        if (!onlyAllowPassthrough) {
            authType = this.service.getAuthenticationType(vdbName, vdbVersion, user);
        }

        switch (authType) {
        case GSS:
            if (connProps.get(ILogon.KRB5TOKEN) != null) {
                Object previous = null;
                boolean assosiated = false;
                SecurityHelper securityHelper = service.getSecurityHelper();
                try {
                    byte[] krb5Token = (byte[])connProps.get(ILogon.KRB5TOKEN);
                    Object securityContext = this.gssServiceTickets.remove(Base64.encodeBytes(MD5(krb5Token)));
                    if (securityContext == null) {
                         throw new LogonException(RuntimePlugin.Event.TEIID40054, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40054));
                    }
                    previous = securityHelper.associateSecurityContext(securityContext);
                    assosiated = true;
                    LogonResult result = logon(connProps, AuthenticationType.GSS, user);
                    result.addProperty(ILogon.KRB5TOKEN, krb5Token);
                    return result;
                } finally {
                    if (assosiated) {
                        securityHelper.associateSecurityContext(previous);
                    }
                }
            }
            Version v = DQPWorkContext.getWorkContext().getClientVersion();
            //send a login result with a GSS challange
            if (v.compareTo(Version.EIGHT_7) >= 0) {
                LogonResult result = new LogonResult();
                result.addProperty(ILogon.AUTH_TYPE, authType);
                return result;
            }
            //client not compatible, throw an exception
            throw new LogonException(RuntimePlugin.Event.TEIID40149, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149));
        case USERPASSWORD:
        case SSL:
            return logon(connProps, authType, user);
        default:
            throw new LogonException(RuntimePlugin.Event.TEIID40055, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40055, authType));
        }
    }

    private LogonResult logon(Properties connProps, AuthenticationType authType, String user) throws LogonException {
        String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
        String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
        String applicationName = connProps.getProperty(TeiidURL.CONNECTION.APP_NAME);
        Object credential = null;
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        if (authType == AuthenticationType.SSL) {
            SSLSession session = workContext.getSSLSession();
            if (session == null) {
                throw new LogonException(RuntimePlugin.Event.TEIID40170, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40170, authType));
            }
            Certificate[] certs;
            try {
                certs = session.getPeerCertificates();
            } catch (SSLPeerUnverifiedException e) {
                throw new LogonException(RuntimePlugin.Event.TEIID40170, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40170, authType));
            }
            if (certs != null && certs.length > 0) {
                credential = certs[0];
            } //else not expected, and should fail auth
        } else {
            credential = connProps.getProperty(TeiidURL.CONNECTION.PASSWORD);
        }

        //this is just to match the old expectations.
        //this class does not do much for us...
        Credentials credentials = null;
        if (credential != null) {
            credentials = new Credentials(credential);
        }

        try {
            SessionMetadata sessionInfo = service.createSession(vdbName, vdbVersion, authType, user,credentials, applicationName, connProps);

            if (connProps.get(GSSCredential.class.getName()) != null) {
                addCredentials(sessionInfo.getSubject(), (GSSCredential)connProps.get(GSSCredential.class.getName()));
            }

            updateDQPContext(sessionInfo);
            if (workContext.getClientAddress() == null) {
                sessionInfo.setEmbedded(true);
            }
            //if (oldSessionId != null) {
                //TODO: we should be smarter about disassociating the old sessions from the client.  we'll just rely on
                //ping based clean up
            //}
            return new LogonResult(sessionInfo.getSessionToken(), sessionInfo.getVDBName(), clusterName);
        } catch (LoginException|SessionServiceException e) {
             throw new LogonException(e);
        }
    }

    @Override
    public LogonResult neogitiateGssLogin(Properties connProps, byte[] serviceTicket, boolean createSession) throws LogonException {
        String vdbName = connProps.getProperty(BaseDataSource.VDB_NAME);
        String vdbVersion = connProps.getProperty(BaseDataSource.VDB_VERSION);
        String user = connProps.getProperty(BaseDataSource.USER_NAME);

        AuthenticationType authType = this.service.getAuthenticationType(vdbName, vdbVersion, user);

        if (!AuthenticationType.GSS.equals(authType)) {
             throw new LogonException(RuntimePlugin.Event.TEIID40055, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40055, "Kerberos")); //$NON-NLS-1$
        }

        // Using SPENGO security domain establish a token and subject.
        GSSResult result = neogitiateGssLogin(serviceTicket, vdbName, vdbVersion, user);

        if (!result.isAuthenticated() || !createSession) {
            LogonResult logonResult = new LogonResult(new SessionToken(0, "temp"), "internal", "internal"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            logonResult.addProperty(ILogon.KRB5TOKEN, result.getServiceToken());
            logonResult.addProperty(ILogon.KRB5_ESTABLISHED, new Boolean(result.isAuthenticated()));
            if (result.isAuthenticated()) {
                logonResult.addProperty(GSSCredential.class.getName(), result.getDelegationCredential());
            }
            return logonResult;
        }

        // GSS API (jdbc) will make the session in one single call
        connProps.setProperty(TeiidURL.CONNECTION.USER_NAME, result.getUserName());
        connProps.put(ILogon.KRB5TOKEN, result.getServiceToken());
        if(result.getDelegationCredential() != null){
            connProps.put(GSSCredential.class.getName(), result.getDelegationCredential());
        }
        LogonResult logonResult =  logon(connProps);
        return logonResult;
    }

    public GSSResult neogitiateGssLogin(byte[] serviceTicket, String vdbName,
            String vdbVersion, String user) throws LogonException {
        GSSResult result;
        try {
            result = service.neogitiateGssLogin(user, vdbName, vdbVersion, serviceTicket);
        } catch (LoginException e) {
            throw new LogonException(RuntimePlugin.Event.TEIID40014, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40014));
        }

        if (result == null) {
             throw new LogonException(RuntimePlugin.Event.TEIID40014, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40014));
        }

        if (result.isAuthenticated()) {
            LogManager.logDetail(LogConstants.CTX_SECURITY, "Kerberos context established"); //$NON-NLS-1$
            this.gssServiceTickets.put(Base64.encodeBytes(MD5(result.getServiceToken())), result.getSecurityContext());
        }
        return result;
    }

    protected static byte[] MD5(byte[] content) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5"); //$NON-NLS-1$
            return md.digest(content);
        } catch (java.security.NoSuchAlgorithmException e) {
            return content;
        }
    }

    private void updateDQPContext(SessionMetadata s) {
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        SessionMetadata old = workContext.getSession();
        if (old.getSessionId() != null) {
            old.setActive(false);
        }
        workContext.setSession(s);
        if (s.getSessionId() != null) {
            s.setActive(true);
        }
    }

    public ResultsFuture<?> logoff() throws InvalidSessionException {
        DQPWorkContext workContext = DQPWorkContext.getWorkContext();
        if (workContext.getSession().isClosed() || workContext.getSessionId() == null) {
            if (workContext.getSessionId() != null) {
                this.updateDQPContext(new SessionMetadata());
            }
            return ResultsFuture.NULL_FUTURE;
        }
        try {
            this.service.closeSession(workContext.getSessionId());
        } finally {
            this.updateDQPContext(new SessionMetadata());
        }
        return ResultsFuture.NULL_FUTURE;
    }

    public ResultsFuture<?> ping() throws InvalidSessionException,TeiidComponentException {
        // ping is double used to alert the aliveness of the client, as well as check the server instance is
        // alive by socket server instance, so that they can be cached.
        String id = DQPWorkContext.getWorkContext().getSessionId();
        if (id != null) {
            this.service.pingServer(id);
        }
        LogManager.logTrace(LogConstants.CTX_SECURITY, "Ping", id); //$NON-NLS-1$
        return ResultsFuture.NULL_FUTURE;
    }

    @Override
    public ResultsFuture<?> ping(Collection<String> sessions)
            throws TeiidComponentException, CommunicationException {
        for (String string : sessions) {
            try {
                this.service.pingServer(string);
            } catch (InvalidSessionException e) {
            }
        }
        return ResultsFuture.NULL_FUTURE;
    }

    @Override
    public void assertIdentity(SessionToken checkSession) throws InvalidSessionException, TeiidComponentException {
        if (checkSession == null) {
            //disassociate
            this.updateDQPContext(new SessionMetadata());
            return;
        }
        SessionMetadata sessionInfo = null;
        try {
            sessionInfo = this.service.validateSession(checkSession.getSessionID());
        } catch (SessionServiceException e) {
             throw new TeiidComponentException(RuntimePlugin.Event.TEIID40062, e);
        }

        if (sessionInfo == null) {
             throw new InvalidSessionException(RuntimePlugin.Event.TEIID40063);
        }

        SessionToken st = sessionInfo.getSessionToken();
        if (!st.equals(checkSession)) {
             throw new InvalidSessionException(RuntimePlugin.Event.TEIID40064);
        }
        this.updateDQPContext(sessionInfo);
    }

    public SessionService getSessionService() {
        return service;
    }

    static void addCredentials(final Subject subject, final GSSCredential cred) {
        if (System.getSecurityManager() == null) {
            subject.getPrivateCredentials().add(cred);
        }

        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                subject.getPrivateCredentials().add(cred);
                return null;
            }
        });
    }
}
