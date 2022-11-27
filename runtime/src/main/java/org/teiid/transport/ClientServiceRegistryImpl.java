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

import java.util.HashMap;

import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.deployers.VDBRepository;
import org.teiid.net.ConnectionException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.SecurityHelper;
import org.teiid.vdb.runtime.VDBKey;


public abstract class ClientServiceRegistryImpl implements ClientServiceRegistry {

    public static class ClientService {
        private Object instance;
        private String loggingContext;
        private ReflectionHelper reflectionHelper;

        public ClientService(Object instance, String loggingContext,
                ReflectionHelper reflectionHelper) {
            this.instance = instance;
            this.loggingContext = loggingContext;
            this.reflectionHelper = reflectionHelper;
        }

        public Object getInstance() {
            return instance;
        }
        public String getLoggingContext() {
            return loggingContext;
        }
        public ReflectionHelper getReflectionHelper() {
            return reflectionHelper;
        }
    }

    private HashMap<String, ClientService> clientServices = new HashMap<String, ClientService>();
    private SecurityHelper securityHelper;
    private Type type = Type.JDBC;
    private AuthenticationType authenticationType = AuthenticationType.USERPASSWORD;
    private VDBRepository vdbRepository;

    public ClientServiceRegistryImpl() {

    }

    public ClientServiceRegistryImpl(Type type) {
        this.type = type;
    }

    public void setAuthenticationType(AuthenticationType authenticationType) {
        this.authenticationType = authenticationType;
    }

    public <T> T getClientService(Class<T> iface) throws ComponentNotFoundException {
        ClientService cs = getClientService(iface.getName());
        return iface.cast(cs.getInstance());
    }

    public ClientService getClientService(String iface) throws ComponentNotFoundException {
        ClientService cs = clientServices.get(iface);
        if (cs == null) {
             throw new ComponentNotFoundException(RuntimePlugin.Event.TEIID40070, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40070, type, iface));
        }
        return cs;
    }

    public <T> void registerClientService(Class<T> iface, T instance, String loggingContext) {
        this.clientServices.put(iface.getName(), new ClientService(instance, loggingContext, new ReflectionHelper(iface)));
    }

    @Override
    public SecurityHelper getSecurityHelper() {
        return this.securityHelper;
    }

    public void setSecurityHelper(SecurityHelper securityHelper) {
        this.securityHelper = securityHelper;
    }

    @Override
    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    @Override
    public void waitForFinished(VDBKey vdbKey,
            int timeOutMillis) throws ConnectionException {

    }

    @Override
    public VDBRepository getVDBRepository() {
        return vdbRepository;
    }

    public void setVDBRepository(VDBRepository vdbRepository) {
        this.vdbRepository = vdbRepository;
    }

}
