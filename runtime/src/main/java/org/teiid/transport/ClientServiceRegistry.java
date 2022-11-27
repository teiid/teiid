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

import org.teiid.core.ComponentNotFoundException;
import org.teiid.deployers.VDBRepository;
import org.teiid.net.ConnectionException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.SecurityHelper;
import org.teiid.vdb.runtime.VDBKey;


public interface ClientServiceRegistry {

    public enum Type {
        ODBC, JDBC
    }

    <T> T getClientService(Class<T> iface) throws ComponentNotFoundException;

    SecurityHelper getSecurityHelper();

    /**
     * Provides the authentication type for pre-8.7 JDBC clients.
     *
     * @return
     */
    @Deprecated()
    AuthenticationType getAuthenticationType();

    void waitForFinished(VDBKey vdbKey, int timeOutMillis) throws ConnectionException;

    ClassLoader getCallerClassloader();

    VDBRepository getVDBRepository();

}
