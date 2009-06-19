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

package com.metamatrix.common.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * Utility class which includes the Set of all String logging contexts used by all parts of the server.
 * </p>
 * <p>
 * Due to build implementations, this class must define all logging contexts, which may be redundantly defined elsewhere.
 * </p>
 */
public class LogContextsUtil {

    // *******************************************************************
    // TO ADD NEW CONTEXTS:
    // Please add a context to the appropriate inner class, or create
    // an appropriate new inner class. Then, also add that context to
    // the ALL_CONTEXTS set, which is built in the static block at the
    // end of this file.
    //
    // *******************************************************************

	
    public static class CommonConstants {

        // Contexts
        public static final String CTX_DBIDGEN = "DBIDGEN"; //$NON-NLS-1$
        public static final String CTX_LOGON = "LOGON"; //$NON-NLS-1$
        public static final String CTX_SERVICE = "SERVICE"; //$NON-NLS-1$
        public static final String CTX_PROXY = "PROXY"; //$NON-NLS-1$
        public static final String CTX_CONTROLLER = "CONTROLLER"; //$NON-NLS-1$
        public static final String CTX_CONFIG = "CONFIG"; //$NON-NLS-1$
        public static final String CTX_LOGGING = "LOG"; //$NON-NLS-1$
        public static final String CTX_MESSAGE_BUS = "MESSAGE_BUS"; //$NON-NLS-1$
        public static final String CTX_STANDARD_OUT = "STDOUT"; //$NON-NLS-1$
        public static final String CTX_STANDARD_ERR = "STDERR"; //$NON-NLS-1$
        public static final String CTX_DISTRIB_CACHE = "DISTRIB_CACHE"; //$NON-NLS-1$
        public static final String CTX_POOLING = "RESOURCE_POOLING"; //$NON-NLS-1$
        public static final String CTX_BUFFER_MGR = "BUFFER_MGR"; //$NON-NLS-1$
        public static final String CTX_STORAGE_MGR = "STORAGE_MGR"; //$NON-NLS-1$
        public static final String CTX_XA_TXN = "XA_TXN"; //$NON-NLS-1$
        public static final String CTX_TXN_LOG = "TXN_LOG"; //$NON-NLS-1$
        public static final String CTX_EXTENSION_SOURCE = "EXTENSION_MODULE"; //$NON-NLS-1$
        public static final String CTX_EXTENSION_SOURCE_JDBC = "JDBC_EXT_MODULE_TRANSACTION"; //$NON-NLS-1$
        public static final String CTX_COMMUNICATION = "COMMUNICATION"; //$NON-NLS-1$
        public static final String CTX_COMMANDLOGGING = "COMMAND_LOG"; //$NON-NLS-1$
        public static final String CTX_AUDITLOGGING = "AUDIT_LOG"; //$NON-NLS-1$
    }

    public static class SecurityConstants {

        public static final String CTX_CLIENT_MONITOR = "CLIENT_MONITOR"; //$NON-NLS-1$
        public static final String CTX_SESSION_MONITOR = "SESSION_MONITOR"; //$NON-NLS-1$
        public static final String CTX_SESSION_CLEANUP = "SESSION_CLEANUP"; //$NON-NLS-1$
        public static final String CTX_SESSION = "SESSION"; //$NON-NLS-1$
        public static final String CTX_SESSION_CACHE = "SESSION_CACHE"; //$NON-NLS-1$
        public static final String CTX_MEMBERSHIP = "MEMBERSHIP"; //$NON-NLS-1$
        public static final String CTX_AUTHORIZATION = "AUTHORIZATION"; //$NON-NLS-1$
        public static final String CTX_AUDIT = "AUDIT"; //$NON-NLS-1$
    }

    public static class PlatformConstants {

        public static final String CTX_REGISTRY = "REGISTRY"; //$NON-NLS-1$
        public static final String CTX_VM_CONTROLLER = "VM_CONTROLLER"; //$NON-NLS-1$
        public static final String CTX_SERVICE_CONTROLLER = "SERVICE_CONTROLLER"; //$NON-NLS-1$
        public static final String CTX_RUNTIME_ADMIN = "RUNTIME_ADMIN"; //$NON-NLS-1$
    }

    public static class PlatformAdminConstants {

        // Platform Admin API
        public static final String CTX_ADMIN = "ADMIN"; //$NON-NLS-1$
        public static final String CTX_AUDIT_ADMIN = "AUDIT_ADMIN"; //$NON-NLS-1$
        public static final String CTX_ADMIN_API = "ADMIN_API"; //$NON-NLS-1$
        public static final String CTX_ADMIN_API_CONNECTION = "ADMIN_API_CONNECTION"; //$NON-NLS-1$
        public static final String CTX_AUTHORIZATION_ADMIN_API = "AUTHORIZATION_ADMIN_API"; //$NON-NLS-1$
        public static final String CTX_CONFIGURATION_ADMIN_API = "CONFIGURATION_ADMIN_API"; //$NON-NLS-1$
        public static final String CTX_RUNTIME_STATE_ADMIN_API = "RUNTIME_STATE_ADMIN_API"; //$NON-NLS-1$
        public static final String CTX_EXTENSION_SOURCE_ADMIN_API = "EXTENSION_SOURCE_ADMIN_API"; //$NON-NLS-1$

        // Server Admin API
        public static final String CTX_RUNTIME_METADATA_ADMIN_API = "RUNTIME_METADATA_ADMIN_API"; //$NON-NLS-1$
    }

    public static class RuntimeMetadataConstants {

        public static final String CTX_RUNTIME_METADATA = "RUNTIME_METADATA"; //$NON-NLS-1$
    }

    public static class QueryConstants {

        public static final String CTX_FUNCTION_TREE = "FUNCTION_TREE"; //$NON-NLS-1$
        public static final String CTX_QUERY_PLANNER = "QUERY_PLANNER"; //$NON-NLS-1$
        public static final String CTX_QUERY_RESOLVER = "QUERY_RESOLVER"; //$NON-NLS-1$
        public static final String CTX_XML_PLANNER = "XML_QUERY_PLANNER"; //$NON-NLS-1$
        public static final String CTX_XML_PLAN = "XML_PLAN"; //$NON-NLS-1$
    }

    public static class DQPConstants {

        public static final String CTX_DQP = "DQP"; //$NON-NLS-1$
        public static final String CTX_CONNECTOR = "CONNECTOR"; //$NON-NLS-1$			
    }

    public static class ServerConstants {

        public static final String CTX_ROUTER = "ROUTER"; //$NON-NLS-1$
        public static final String CTX_QUERY_SERVICE = "QUERY_SERVICE"; //$NON-NLS-1$
    }

    /**
     * The Set of all String logging contexts of all parts of the server.
     */
    public static final Set ALL_CONTEXTS;
	public static final String CTX_CONFIG = "CONFIG"; //$NON-NLS-1$

    // this will need to be updated as any new contexts are added to the
    // various "logging contants" interfaces of the different projects
    static {
        Set allContexts = new HashSet();

        allContexts.add(CommonConstants.CTX_DBIDGEN);
        allContexts.add(CommonConstants.CTX_LOGON);
        allContexts.add(CommonConstants.CTX_SERVICE);
        allContexts.add(CommonConstants.CTX_PROXY);
        allContexts.add(CommonConstants.CTX_CONTROLLER);
        allContexts.add(CommonConstants.CTX_CONFIG);
        allContexts.add(CommonConstants.CTX_LOGGING);
        allContexts.add(CommonConstants.CTX_DISTRIB_CACHE);
        allContexts.add(CommonConstants.CTX_MESSAGE_BUS);
        allContexts.add(CommonConstants.CTX_POOLING);
        allContexts.add(CommonConstants.CTX_BUFFER_MGR);
        allContexts.add(CommonConstants.CTX_STORAGE_MGR);
        allContexts.add(CommonConstants.CTX_XA_TXN);
        allContexts.add(CommonConstants.CTX_STANDARD_OUT);
        allContexts.add(CommonConstants.CTX_STANDARD_ERR);
        allContexts.add(CommonConstants.CTX_EXTENSION_SOURCE);
        allContexts.add(CommonConstants.CTX_EXTENSION_SOURCE_JDBC);
        allContexts.add(CommonConstants.CTX_COMMUNICATION);

        allContexts.add(SecurityConstants.CTX_CLIENT_MONITOR);
        allContexts.add(SecurityConstants.CTX_SESSION_MONITOR);
        allContexts.add(SecurityConstants.CTX_SESSION_CLEANUP);
        allContexts.add(SecurityConstants.CTX_SESSION);
        allContexts.add(SecurityConstants.CTX_SESSION_CACHE);
        allContexts.add(SecurityConstants.CTX_MEMBERSHIP);
        allContexts.add(SecurityConstants.CTX_AUTHORIZATION);
        allContexts.add(SecurityConstants.CTX_AUDIT);

        allContexts.add(PlatformConstants.CTX_REGISTRY);
        allContexts.add(PlatformConstants.CTX_VM_CONTROLLER);
        allContexts.add(PlatformConstants.CTX_SERVICE_CONTROLLER);
        allContexts.add(PlatformConstants.CTX_RUNTIME_ADMIN);

        allContexts.add(PlatformAdminConstants.CTX_ADMIN);
        allContexts.add(PlatformAdminConstants.CTX_AUDIT_ADMIN);
        allContexts.add(PlatformAdminConstants.CTX_ADMIN_API);
        allContexts.add(PlatformAdminConstants.CTX_ADMIN_API_CONNECTION);
        allContexts.add(PlatformAdminConstants.CTX_AUTHORIZATION_ADMIN_API);
        allContexts.add(PlatformAdminConstants.CTX_CONFIGURATION_ADMIN_API);
        allContexts.add(PlatformAdminConstants.CTX_RUNTIME_STATE_ADMIN_API);
        allContexts.add(PlatformAdminConstants.CTX_EXTENSION_SOURCE_ADMIN_API);

        allContexts.add(PlatformAdminConstants.CTX_RUNTIME_METADATA_ADMIN_API);

        allContexts.add(RuntimeMetadataConstants.CTX_RUNTIME_METADATA);

        allContexts.add(QueryConstants.CTX_FUNCTION_TREE);
        allContexts.add(QueryConstants.CTX_QUERY_PLANNER);
        allContexts.add(QueryConstants.CTX_QUERY_RESOLVER);
        allContexts.add(QueryConstants.CTX_XML_PLANNER);
        allContexts.add(QueryConstants.CTX_XML_PLAN);

        allContexts.add(DQPConstants.CTX_DQP);
        allContexts.add(DQPConstants.CTX_CONNECTOR);

        allContexts.add(ServerConstants.CTX_ROUTER);
        allContexts.add(ServerConstants.CTX_QUERY_SERVICE);

        ALL_CONTEXTS = Collections.unmodifiableSet(allContexts);
    }
}
