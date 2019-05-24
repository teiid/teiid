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

package org.teiid.dqp.internal.datamgr;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidException;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.translator.ExecutionFactory;


public class ConnectorManagerRepository implements Serializable{

    @SuppressWarnings("serial")
    public static class ConnectorManagerException extends TeiidException {
        public ConnectorManagerException(String msg) {
            super(msg);
        }
        public ConnectorManagerException(Throwable t) {
            super(t);
        }
    }

    /**
     * Provides {@link ExecutionFactory}s to the {@link ConnectorManagerRepository}
     */
    public interface ExecutionFactoryProvider {
        /**
         *
         * @param name
         * @return the named {@link ExecutionFactory} or throw a {@link ConnectorManagerException} if it does not exist
         * @throws ConnectorManagerException
         */
        ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException;
    }

    private static final long serialVersionUID = -1611063218178314458L;

    private Map<String, ConnectorManager> repo = new ConcurrentHashMap<String, ConnectorManager>();
    private boolean shared;
    private ExecutionFactoryProvider provider;

    public ConnectorManagerRepository() {
    }

    protected ConnectorManagerRepository(boolean b) {
        this.shared = b;
    }

    public boolean isShared() {
        return shared;
    }

    public void addConnectorManager(String connectorName, ConnectorManager mgr) {
        this.repo.put(connectorName, mgr);
    }

    public ConnectorManager getConnectorManager(String connectorName) {
        return this.repo.get(connectorName);
    }

    public Map<String, ConnectorManager> getConnectorManagers() {
        return repo;
    }

    public ConnectorManager removeConnectorManager(String connectorName) {
        return this.repo.remove(connectorName);
    }

    public void createConnectorManagers(VDBMetaData deployment, ExecutionFactoryProvider provider) throws ConnectorManagerException {
        for (ModelMetaData model : deployment.getModelMetaDatas().values()) {
            List<String> sourceNames = model.getSourceNames();
            if (sourceNames.size() != new HashSet<String>(sourceNames).size()) {
                throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31101, model.getName(), deployment.getName(), deployment.getVersion()));
            }
            if (sourceNames.size() > 1 && !model.isSupportsMultiSourceBindings()) {
                throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31102, model.getName(), deployment.getName(), deployment.getVersion()));
            }
            for (SourceMappingMetadata source : model.getSourceMappings()) {
                createConnectorManager(deployment, provider, source, false);
            }
        }
    }

    public void createConnectorManager(
            VDBMetaData deployment, ExecutionFactoryProvider provider,
            SourceMappingMetadata source, boolean replace) throws ConnectorManagerException {
        String name = source.getTranslatorName();
        String connection = source.getConnectionJndiName();
        createConnectorManager(source.getName(), name, connection, provider, replace);
    }

    public void createConnectorManager(String sourceName, String translatorName, String jndiName,
            ExecutionFactoryProvider provider, boolean replace) throws ConnectorManagerException {
        ConnectorManager cm = getConnectorManager(sourceName);
        ExecutionFactory<Object, Object> ef = null;
        if (cm != null) {
            if (!cm.getTranslatorName().equals(translatorName)
                    || !EquivalenceUtil.areEqual(cm.getConnectionName(), jndiName)) {
                if (!replace) {
                    throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31103, sourceName));
                }
                if (cm.getTranslatorName().equals(translatorName)) {
                    ef = cm.getExecutionFactory();
                }
            } else {
                return;
            }
        }
        if (ef == null) {
            ef = provider.getExecutionFactory(translatorName);
            if (ef == null) {
                throw new ConnectorManagerException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31146, translatorName));
            }
        }
        cm = createConnectorManager(translatorName, jndiName, ef);
        addConnectorManager(sourceName, cm);
    }

    protected ConnectorManager createConnectorManager(String name,
            String connection, ExecutionFactory<Object, Object> ef) throws ConnectorManagerException {
        return new ConnectorManager(name, connection, ef);
    }

    public void setProvider(ExecutionFactoryProvider provider) {
        this.provider = provider;
    }

    public ExecutionFactoryProvider getProvider() {
        return provider;
    }
}
