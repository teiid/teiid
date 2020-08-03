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

package org.teiid.services;


import java.security.Principal;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.SessionToken;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.logging.AuditMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.vdb.runtime.VDBKey;


/**
 * This class serves as the primary implementation of the Session Service.
 */
public class SessionServiceImpl implements SessionService {

    public enum Authentication {
        GSS("gss-patternKey", AuthenticationType.GSS),  //$NON-NLS-1$
        SSL("ssl-patternKey", AuthenticationType.SSL), //$NON-NLS-1$
        PASSWORD("password-patternKey", AuthenticationType.USERPASSWORD); //$NON-NLS-1$

        String patternKey;
        AuthenticationType type;

        private Authentication(String pattern, AuthenticationType type) {
            this.patternKey = pattern;
            this.type = type;
        }

        public String getPatternKey() {
            return patternKey;
        }

        public AuthenticationType getType() {
            return type;
        }
    }
    public static final String SECURITY_DOMAIN_PROPERTY = "security-domain"; //$NON-NLS-1$
    public static final String AUTHENTICATION_TYPE_PROPERTY = "authentication-type"; //$NON-NLS-1$
    public static final String MAX_SESSIONS_PER_USER = "max-sessions-per-user"; //$NON-NLS-1$
    public static final String AT = "@"; //$NON-NLS-1$
    /*
     * Configuration state
     */
    private long sessionMaxLimit = DEFAULT_MAX_SESSIONS;
    private long sessionExpirationTimeLimit = DEFAULT_SESSION_EXPIRATION;
    private AuthenticationType defaultAuthenticationType = AuthenticationType.USERPASSWORD;

    private static boolean CHECK_PING = PropertiesUtils.getHierarchicalProperty("org.teiid.checkPing", true, Boolean.class); //$NON-NLS-1$
    /*
     * Injected state
     */
    private VDBRepository vdbRepository;
    protected SecurityHelper securityHelper;

    private DQPCore dqp;

    private Map<String, SessionMetadata> sessionCache = new ConcurrentHashMap<String, SessionMetadata>();
    private Timer sessionMonitor = null;
    private String securityDomainName;
    private boolean trustAllLocal = true;

    public void setSecurityDomain(String domainName) {
        this.securityDomainName = domainName;
    }

    // -----------------------------------------------------------------------------------
    // S E R V I C E - R E L A T E D M E T H O D S
    // -----------------------------------------------------------------------------------

    private void monitorSessions() {
        long currentTime = System.currentTimeMillis();
        for (SessionMetadata info : sessionCache.values()) {
            try {
                if (CHECK_PING && !info.isEmbedded() && !info.isActive() && currentTime - info.getLastPingTime() > ServerConnection.PING_INTERVAL * 5) {
                    LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40007, info.getSessionId()));
                    closeSession(info.getSessionId());
                } else if (sessionExpirationTimeLimit > 0 && currentTime - info.getCreatedTime() > sessionExpirationTimeLimit) {
                    LogManager.logInfo(LogConstants.CTX_SECURITY,RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40008, info.getSessionId()));
                    closeSession(info.getSessionId());
                }
            } catch (Exception e) {
                LogManager.logDetail(LogConstants.CTX_SECURITY, e, "error running session monitor, unable to monitor:", info.getSessionId()); //$NON-NLS-1$
            }
        }
    }

    @Override
    public void closeSession(String sessionID) throws InvalidSessionException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_SECURITY, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"closeSession", sessionID}); //$NON-NLS-1$
        }
        SessionMetadata info = getSessionInfo(sessionID, true);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, new AuditMessage("session", "logoff", info)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (info.getVDBName() != null) {
            try {
                dqp.terminateSession(info.getSessionId());
            } catch (Exception e) {
                LogManager.logWarning(LogConstants.CTX_SECURITY,e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40018));
            }
        }
        info.setSecurityContext(null);
        info.setClosed();
        info.getSessionVariables().clear();
    }

    @Override
    public SessionMetadata createSession(String vdbName,
            String vdbVersion, AuthenticationType authType, String userName,
            Credentials credentials, String applicationName, Properties properties)
            throws LoginException, SessionServiceException {
        ArgCheck.isNotNull(applicationName);
        ArgCheck.isNotNull(properties);

        Object securityContext = null;
        Subject subject = null;

        String hostName = properties.getProperty(TeiidURL.CONNECTION.CLIENT_HOSTNAME);
        String ipAddress = properties.getProperty(TeiidURL.CONNECTION.CLIENT_IP_ADDRESS);
        String clientMac = properties.getProperty(TeiidURL.CONNECTION.CLIENT_MAC);
        boolean onlyAllowPassthrough = Boolean.valueOf(properties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION,
                "false")); //$NON-NLS-1$

        AuditMessage.LogonInfo info = new AuditMessage.LogonInfo(vdbName, vdbVersion, authType.toString(), userName, applicationName, hostName, ipAddress, clientMac, onlyAllowPassthrough);

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, new AuditMessage("session", "logon-request", info, null)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        try {

            // Validate VDB and version if logging on to server product...
            VDBMetaData vdb = null;
            if (vdbName != null) {
                vdb = getActiveVDB(vdbName, vdbVersion);
                if (vdb == null) {
                    throw new SessionServiceException(RuntimePlugin.Event.TEIID40046, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40046, vdbName, vdbVersion));
                }
            }

            if (sessionMaxLimit > 0 && getActiveSessionsCount() >= sessionMaxLimit) {
                 throw new SessionServiceException(RuntimePlugin.Event.TEIID40043, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40043, new Long(sessionMaxLimit)));
            }

            String securityDomain = getSecurityDomain(vdbName, vdbVersion, vdb);

            if (securityDomain != null) {
                // Authenticate user...
                // if not authenticated, this method throws exception
                LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"authenticateUser", userName, applicationName}); //$NON-NLS-1$

                if (onlyAllowPassthrough || authType.equals(AuthenticationType.GSS)) {
                    securityContext = this.securityHelper.getSecurityContext(securityDomain);
                    if (securityContext != null) {
                        subject = this.securityHelper.getSubjectInContext(securityContext);
                    }
                    if (subject == null && ((!onlyAllowPassthrough || !(trustAllLocal && DQPWorkContext.getWorkContext().isLocal())))) {
                        throw new LoginException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40087));
                    }
                } else {
                    securityContext = this.securityHelper.authenticate(securityDomain, userName, credentials, applicationName);
                    subject = this.securityHelper.getSubjectInContext(securityContext);
                    //TODO: it may be appropriate here to obtain the username from the subject as well
                }

                if (subject != null) {
                    userName = getUserName(subject, userName);
                }
            }
            else {
                LogManager.logDetail(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40117));
            }

            enforceMaxSessions(userName, vdb);

            long creationTime = System.currentTimeMillis();

            // Return a new session info object
            SessionMetadata newSession = new SessionMetadata();
            newSession.setSessionToken(new SessionToken(userName));
            newSession.setSessionId(newSession.getSessionToken().getSessionID());
            newSession.setUserName(userName);
            newSession.setCreatedTime(creationTime);
            newSession.setApplicationName(applicationName);
            newSession.setClientHostName(hostName);
            newSession.setIPAddress(ipAddress);
            newSession.setClientHardwareAddress(clientMac);
            newSession.setSecurityDomain(securityDomain);
            if (vdb != null) {
                newSession.setVDBName(vdb.getName());
                newSession.setVDBVersion(vdb.getVersion());
            }
            newSession.setSubject(subject);
            newSession.setSecurityContext(securityContext);
            newSession.setVdb(vdb);
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_SECURITY, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful, created", newSession }); //$NON-NLS-1$
            }
            this.sessionCache.put(newSession.getSessionId(), newSession);
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, new AuditMessage("session", "logon-success", newSession)); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return newSession;
        } catch (LoginException e) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, new AuditMessage("session", "logon-fail", info, e)); //$NON-NLS-1$ //$NON-NLS-2$
            }
            throw e;
        } catch (SessionServiceException e) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_AUDITLOGGING, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_AUDITLOGGING, new AuditMessage("session", "logon-fail", info, e)); //$NON-NLS-1$ //$NON-NLS-2$
            }
            throw e;
        }
    }

    static class MaxSessions {
        Integer val;
    }

    private void enforceMaxSessions(String userName, VDBMetaData vdb) throws SessionServiceException {
        if (vdb == null) {
            return;
        }
        MaxSessions max = vdb.getAttachment(MaxSessions.class);
        if (max == null) {
            max = new MaxSessions();
            String maxProp = vdb.getPropertyValue(MAX_SESSIONS_PER_USER);
            if (maxProp != null) {
                try {
                    max.val = Integer.valueOf(maxProp);
                } catch (NumberFormatException e) {
                    LogManager.logDetail(LogConstants.CTX_SECURITY, "Value for max sessions is invalid on", vdb); //$NON-NLS-1$
                }
            }
            vdb.addAttachment(MaxSessions.class, max);
        }
        if (max.val == null || max.val < 0) {
            return;
        }
        //TODO: this assumes user names are case sensitive - they
        //may or may not be in the underlying system.
        //perhaps it would be more accurate to always use the name from the subject
        int count = getSessionsLoggedInToVDB(vdb.getAttachment(VDBKey.class), userName).size();
        if (count >= max.val) {
            throw new SessionServiceException(RuntimePlugin.Event.TEIID40044, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40044, max.val));
        }
    }

    /**
     *
     * @param vdbName
     * @param vdbVersion
     * @return the vdb or null if it doesn't exist
     * @throws SessionServiceException if the version is not valid or the vdb doesn't accept connections
     */
    protected VDBMetaData getActiveVDB(String vdbName, String vdbVersion) throws SessionServiceException {
        VDBMetaData vdb = null;

        try {
            if (vdbVersion == null) {
                vdb = this.vdbRepository.getLiveVDB(vdbName);
            }
            else {
                vdb = this.vdbRepository.getLiveVDB(vdbName, vdbVersion);
            }
        } catch (NumberFormatException e) {
             throw new SessionServiceException(RuntimePlugin.Event.TEIID40045, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40045, vdbVersion));
        }
        if (vdb == null) {
            return null;
        }
        if (vdb.getConnectionType() == ConnectionType.NONE) {
             throw new SessionServiceException(RuntimePlugin.Event.TEIID40048, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40048, vdbName, vdbVersion));
        }
        return vdb;
    }

    @Override
    public Collection<SessionMetadata> getActiveSessions() {
        return new ArrayList<SessionMetadata>(this.sessionCache.values());
    }

    @Override
    public SessionMetadata getActiveSession(String sessionID) {
        return this.sessionCache.get(sessionID);
    }

    @Override
    public int getActiveSessionsCount() throws SessionServiceException{
        return this.sessionCache.size();
    }

    @Override
    public Collection<SessionMetadata> getSessionsLoggedInToVDB(VDBKey key) {
        return getSessionsLoggedInToVDB(key, null);
    }

    public Collection<SessionMetadata> getSessionsLoggedInToVDB(VDBKey key, String username) {
        ArrayList<SessionMetadata> results = new ArrayList<SessionMetadata>();
        for (SessionMetadata info : this.sessionCache.values()) {
            if (info.getVdb() != null && key.equals(info.getVdb().getAttachment(VDBKey.class)) &&
                    (username == null || info.getUserName().equals(username))) {
                results.add(info);
            }
        }
        return results;
    }

    @Override
    public void pingServer(String sessionID) throws InvalidSessionException {
        SessionMetadata info = getSessionInfo(sessionID, false);
        info.setLastPingTime(System.currentTimeMillis());
        LogManager.logDetail(LogConstants.CTX_SECURITY, "Keep-alive ping received for session:", sessionID); //$NON-NLS-1$
    }

    @Override
    public boolean terminateSession(String terminatedSessionID, String adminSessionID) {
        Object[] params = {adminSessionID, terminatedSessionID};
        LogManager.logInfo(LogConstants.CTX_SECURITY, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40009, params));
        try {
            closeSession(terminatedSessionID);
            return true;
        } catch (InvalidSessionException e) {
            LogManager.logDetail(LogConstants.CTX_SECURITY,e,e.getMessage());
            return false;
        }
    }

    @Override
    public SessionMetadata validateSession(String sessionID) throws InvalidSessionException, SessionServiceException {
        SessionMetadata info = getSessionInfo(sessionID, false);
        return info;
    }

    private SessionMetadata getSessionInfo(String sessionID, boolean remove)
            throws InvalidSessionException {
        if (sessionID == null) {
             throw new InvalidSessionException(RuntimePlugin.Event.TEIID40041, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40041));
        }
        SessionMetadata info = remove?this.sessionCache.remove(sessionID):this.sessionCache.get(sessionID);
        if (info == null) {
             throw new InvalidSessionException(RuntimePlugin.Event.TEIID40042, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40042, sessionID));
        }
        return info;
    }

    public long getSessionMaxLimit() {
        return this.sessionMaxLimit;
    }

    public void setSessionMaxLimit(long limit) {
        this.sessionMaxLimit = limit;
    }

    public long getSessionExpirationTimeLimit() {
        return this.sessionExpirationTimeLimit;
    }

    public void setSessionExpirationTimeLimit(long limit) {
        this.sessionExpirationTimeLimit = limit;
    }

    public void setAuthenticationType(AuthenticationType flag) {
        this.defaultAuthenticationType = flag;
    }

    public void start() {
        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Default security domain configured=", this.securityDomainName}); //$NON-NLS-1$
        this.sessionMonitor = new Timer("SessionMonitor", true); //$NON-NLS-1$
        this.sessionMonitor.schedule(new TimerTask() {
            @Override
            public void run() {
                monitorSessions();
            }
        }, ServerConnection.PING_INTERVAL * 3, ServerConnection.PING_INTERVAL * 2);
    }

    public void stop(){
        if (sessionMonitor != null) {
            this.sessionMonitor.cancel();
        }
        for (SessionMetadata session : sessionCache.values()) {
            try {
                closeSession(session.getSessionId());
            } catch (InvalidSessionException e) {

            }
        }
    }

    public void setVDBRepository(VDBRepository repo) {
        this.vdbRepository = repo;
    }

    public void setSecurityHelper(SecurityHelper securityHelper) {
        this.securityHelper = securityHelper;
    }

    public void setDqp(DQPCore dqp) {
        this.dqp = dqp;
    }

    @Override
    public SecurityHelper getSecurityHelper() {
        return securityHelper;
    }

    /**
     * Caches the information related to vdb specific authentication
     */
    private static class AuthenticationPatterns extends HashMap<Authentication, Pattern> {
        AuthenticationType defaultType;

        public AuthenticationPatterns(VDBMetaData vdb) {
            String typeProperty = vdb.getPropertyValue(AUTHENTICATION_TYPE_PROPERTY);
            if (typeProperty != null) {
                defaultType = AuthenticationType.valueOf(typeProperty);
            }
            for (Authentication auth : Authentication.values()) {
                String p = vdb.getPropertyValue(auth.getPatternKey());
                if (p != null) {
                    this.put(auth, Pattern.compile(p));
                }
            }
        }

        AuthenticationType findMatch(String username) {
            for (Authentication auth : Authentication.values()) {
                Pattern p = get(auth);
                if (p != null && p.matcher(username).matches()) {
                    return auth.getType();
                }
            }
            return defaultType;
        }
    }

    @Override
    public AuthenticationType getAuthenticationType(String vdbName, String version, String userName) throws LogonException {
        if (userName == null) {
            userName = CoreConstants.DEFAULT_ANON_USERNAME;
        }
        if (vdbName != null) {
            VDBMetaData vdb = null;
            try {
                vdb = getActiveVDB(vdbName, version);
            } catch (SessionServiceException e) {
                throw new LogonException(e);
            }
            if (vdb != null) {
                AuthenticationPatterns patterns = vdb.getAttachment(AuthenticationPatterns.class);
                if (patterns == null) {
                    patterns = new AuthenticationPatterns(vdb);
                    vdb.addAttachment(AuthenticationPatterns.class, patterns);
                }

                AuthenticationType result = patterns.findMatch(userName);
                if (result != null) {
                    return result;
                }
            }
        }
        return this.defaultAuthenticationType;
    }

    public String getSecurityDomain(String vdbName, String version, VDB vdb) {
        if (vdbName != null) {
            try {
                if (vdb == null) {
                    vdb = getActiveVDB(vdbName, version);
                }
                if (vdb != null) {
                    String typeProperty = vdb.getPropertyValue(SECURITY_DOMAIN_PROPERTY);
                    if (typeProperty != null) {
                        return typeProperty;
                    }
                }
            } catch (SessionServiceException e) {
                // ignore and return default, this only occur if the name and version are wrong
            }
        }

        return this.securityDomainName;
    }

    @Override
    public GSSResult neogitiateGssLogin(String user, String vdbName,
            String vdbVersion, byte[] serviceTicket) throws LoginException, LogonException {
        String securityDomain = getSecurityDomain(vdbName, vdbVersion, null);
        if (securityDomain == null ) {
             throw new LogonException(RuntimePlugin.Event.TEIID40059, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40059));
        }
        return this.securityHelper.negotiateGssLogin(securityDomain, serviceTicket);
    }

    public AuthenticationType getDefaultAuthenticationType() {
        return defaultAuthenticationType;
    }

    private String getUserName(Subject subject, String userName) {
        Set<Principal> principals = subject.getPrincipals();
        for (Principal p:principals) {
            if (p instanceof Group) {
                continue;
            }
            String name = p.getName();
            if (name != null) {
                return name;
            }
        }
        return userName;
    }

    public boolean isTrustAllLocal() {
        return trustAllLocal;
    }

    public void setTrustAllLocal(boolean trustAllLocal) {
        this.trustAllLocal = trustAllLocal;
    }

}
