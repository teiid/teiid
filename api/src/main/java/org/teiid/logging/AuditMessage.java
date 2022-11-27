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

package org.teiid.logging;

import java.util.Arrays;

import org.teiid.CommandContext;
import org.teiid.adminapi.Session;

/**
 * Log format for auditing.
 */
public class AuditMessage {

    /**
     * Contains information related to a logon attempt
     */
    public static class LogonInfo {

        private final String vdbName;
        private final String vdbVersion;
        private final String authType;
        private final String userName;
        private final String applicationName;
        private final String clientHostName;
        private final String clientIpAddress;
        private final String clientMac;
        private final boolean passThrough;

        public LogonInfo(String vdbName, String vdbVersion,
                String authType, String userName,
                String applicationName, String hostName, String ipAddress, String clientMac, boolean onlyAllowPassthrough) {
            this.vdbName = vdbName;
            this.vdbVersion = vdbVersion;
            this.authType = authType;
            this.userName = userName;
            this.applicationName = applicationName;
            this.clientHostName = hostName;
            this.clientIpAddress = ipAddress;
            this.clientMac = clientMac;
            this.passThrough = onlyAllowPassthrough;
        }

        public String getVdbName() {
            return vdbName;
        }

        public String getVdbVersion() {
            return vdbVersion;
        }

        public String getAuthType() {
            return authType;
        }

        public String getUserName() {
            return userName;
        }

        public String getApplicationName() {
            return applicationName;
        }

        public String getClientHostName() {
            return clientHostName;
        }

        public String getClientIpAddress() {
            return clientIpAddress;
        }

        public String getClientMac() {
            return clientMac;
        }

        public boolean isPassThrough() {
            return passThrough;
        }

        @Override
        public String toString() {
            StringBuffer msg = new StringBuffer();
            msg.append( vdbName );
            msg.append(", "); //$NON-NLS-1$
            msg.append( vdbVersion );
            msg.append(' ');
            msg.append( userName );
            msg.append(' ');
            msg.append(authType);
            msg.append(' ');
            msg.append(clientHostName);
            msg.append(' ');
            msg.append(clientIpAddress);
            msg.append(' ');
            msg.append(clientMac);
            msg.append(' ');
            msg.append(passThrough);
            return msg.toString();
        }

    }

    private String context;
    private String activity;

    private LogonInfo logonInfo;
    private Exception exception;

    private Session session;

    private String[] resources;
    private CommandContext commandContext;

    public AuditMessage(String context, String activity, String[] resources, CommandContext commandContext) {
        this.context = context;
        this.activity = activity;
        this.resources = resources;
        this.commandContext = commandContext;
    }

    public AuditMessage(String context, String activity, LogonInfo info, Exception e) {
        this.context = context;
        this.activity = activity;
        this.logonInfo = info;
        this.exception = e;
    }

    public AuditMessage(String context, String activity, Session session) {
        this.context = context;
        this.activity = activity;
        this.session = session;
    }

    /**
     * The related {@link LogonInfo} only if this is a logon related event
     * @return
     */
    public LogonInfo getLogonInfo() {
        return logonInfo;
    }

    /**
     * The {@link Session} for the event or null if one has not been established.
     * @return
     */
    public Session getSession() {
        if (this.commandContext != null) {
            return this.commandContext.getSession();
        }
        return session;
    }

    public String getContext() {
        return this.context;
    }

    public String getActivity() {
        return this.activity;
    }

    /**
     * The user name or null if the session has not yet been established.
     * @return
     */
    public String getPrincipal() {
        Session s = getSession();
        if (s != null) {
            return s.getUserName();
        }
        return null;
    }

    /**
     * The list of relevant resources for the audit event.
     * Will be null for logon/logoff events.
     * @return
     */
    public String[] getResources() {
        return this.resources;
    }

    public CommandContext getCommandContext() {
        return commandContext;
    }

    /**
     * The exception associated with a failed logon attempt.
     * @return
     */
    public Exception getException() {
        return exception;
    }

    public String toString() {
        StringBuffer msg = new StringBuffer();
        if (this.commandContext != null) {
            msg.append( this.commandContext.getRequestId());
        }
        msg.append(" ["); //$NON-NLS-1$
        if (this.logonInfo != null) {
            msg.append(this.logonInfo);
        } else {
            msg.append( getPrincipal() );
        }
        msg.append("] <"); //$NON-NLS-1$
        msg.append( getContext() );
        msg.append('.');
        msg.append( getActivity() );
        msg.append("> "); //$NON-NLS-1$
        if (resources != null) {
            msg.append( Arrays.toString(resources) );
        }
        return msg.toString();
    }

}
