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

package org.teiid.jdbc;

import java.util.Properties;

import org.teiid.core.TeiidException;
import org.teiid.net.ServerConnection;

public interface LocalProfile extends ConnectionProfile {

    public static final String USE_CALLING_THREAD = "useCallingThread"; //$NON-NLS-1$
    public static final String WAIT_FOR_LOAD = "waitForLoad"; //$NON-NLS-1$
    public static final String TRANSPORT_NAME = "transportName"; //$NON-NLS-1$
    public static final Object DQP_WORK_CONTEXT = "dqpWorkContext"; //$NON-NLS-1$
    public static final Object SSL_SESSION = "sslSession"; //$NON-NLS-1$

    public ServerConnection createServerConnection(Properties info) throws TeiidException;

}
