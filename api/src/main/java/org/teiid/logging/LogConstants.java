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


public interface LogConstants {
    // add the new contexts to the Log4JUtil.java class, for configuration purpose
    public static final String CTX_SECURITY = "org.teiid.SECURITY"; //$NON-NLS-1$
    public static final String CTX_TRANSPORT = "org.teiid.TRANSPORT"; //$NON-NLS-1$
    public static final String CTX_QUERY_PLANNER = "org.teiid.PLANNER"; //$NON-NLS-1$
    public static final String CTX_DQP = "org.teiid.PROCESSOR"; //$NON-NLS-1$
    public static final String CTX_CONNECTOR = "org.teiid.CONNECTOR"; //$NON-NLS-1$
    public static final String CTX_BUFFER_MGR = "org.teiid.BUFFER_MGR"; //$NON-NLS-1$
    public static final String CTX_TXN_LOG = "org.teiid.TXN_LOG"; //$NON-NLS-1$
    public static final String CTX_COMMANDLOGGING = "org.teiid.COMMAND_LOG"; //$NON-NLS-1$
    public static final String CTX_COMMANDLOGGING_SOURCE = CTX_COMMANDLOGGING + ".SOURCE"; //$NON-NLS-1$
    public static final String CTX_AUDITLOGGING = "org.teiid.AUDIT_LOG"; //$NON-NLS-1$
    public static final String CTX_ADMIN_API = "org.teiid.ADMIN_API"; //$NON-NLS-1$
    public static final String CTX_RUNTIME = "org.teiid.RUNTIME"; //$NON-NLS-1$
    public static final String CTX_ODBC = "org.teiid.ODBC"; //$NON-NLS-1$
    public static final String CTX_ODATA = "org.teiid.ODATA"; //$NON-NLS-1$
    public static final String CTX_METASTORE = "org.teiid.METASTORE"; //$NON-NLS-1$

    // Query contexts
    public static final String CTX_FUNCTION_TREE = CTX_QUERY_PLANNER + ".FUNCTION_TREE"; //$NON-NLS-1$
    public static final String CTX_QUERY_RESOLVER = CTX_QUERY_PLANNER + ".RESOLVER"; //$NON-NLS-1$
    public static final String CTX_MATVIEWS = CTX_DQP + ".MATVIEWS"; //$NON-NLS-1$

    public static final String CTX_WS = LogConstants.CTX_CONNECTOR + ".WS"; //$NON-NLS-1$
}
