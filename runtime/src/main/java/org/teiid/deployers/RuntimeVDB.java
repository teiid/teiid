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
package org.teiid.deployers;

import java.util.List;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.runtime.RuntimePlugin;

public abstract class RuntimeVDB {
    private VDBMetaData vdb;
    private VDBModificationListener listener;
    private volatile boolean restartInProgress = false;

    public static class ReplaceResult {
        public boolean isNew;
        public String removedDs;
    }

    public interface VDBModificationListener {
        void dataRoleChanged(String policyName) throws AdminProcessingException;
        void connectionTypeChanged() throws AdminProcessingException;
        void dataSourceChanged(String modelName, String sourceName, String translatorName, String dsName) throws AdminProcessingException;
        void onRestart(List<String> modelNames) throws AdminProcessingException;
    }

    public RuntimeVDB(VDBMetaData vdb, VDBModificationListener listener) {
        this.vdb = vdb;
        this.listener = listener;
    }

    public void addDataRole(String policyName, String mappedRole) throws AdminProcessingException {
        synchronized (this.vdb) {
            DataPolicyMetadata policy = getPolicy(policyName);
            List<String> previous = policy.getMappedRoleNames();
            policy.addMappedRoleName(mappedRole);
            try {
                this.listener.dataRoleChanged(policyName);
            } catch(AdminProcessingException e) {
                policy.setMappedRoleNames(previous);
                throw e;
            }
        }
    }

    public void remoteDataRole(String policyName, String mappedRole) throws AdminProcessingException{
        synchronized (this.vdb) {
            DataPolicyMetadata policy = getPolicy(policyName);
            List<String> previous = policy.getMappedRoleNames();
            policy.removeMappedRoleName(mappedRole);
            try {
                this.listener.dataRoleChanged(policyName);
            } catch(AdminProcessingException e) {
                policy.setMappedRoleNames(previous);
                throw e;
            }
        }
    }

    public void addAnyAuthenticated(String policyName) throws AdminProcessingException{
        synchronized (this.vdb) {
            DataPolicyMetadata policy = getPolicy(policyName);
            boolean previous = policy.isAnyAuthenticated();
            policy.setAnyAuthenticated(true);
            try {
                this.listener.dataRoleChanged(policyName);
            } catch(AdminProcessingException e) {
                policy.setAnyAuthenticated(previous);
                throw e;
            }
        }
    }

    public void removeAnyAuthenticated(String policyName) throws AdminProcessingException{
        synchronized (this.vdb) {
            DataPolicyMetadata policy = getPolicy(policyName);
            boolean previous = policy.isAnyAuthenticated();
            policy.setAnyAuthenticated(false);
            try {
                this.listener.dataRoleChanged(policyName);
            } catch(AdminProcessingException e) {
                policy.setAnyAuthenticated(previous);
                throw e;
            }
        }
    }

    public void changeConnectionType(ConnectionType type) throws AdminProcessingException {
        synchronized (this.vdb) {
            ConnectionType previous = this.vdb.getConnectionType();
            this.vdb.setConnectionType(type);
            try {
                this.listener.connectionTypeChanged();
            } catch(AdminProcessingException e) {
                this.vdb.setConnectionType(previous);
                throw e;
            }
        }
    }

    public ReplaceResult updateSource(String sourceName, String translatorName, String dsName) throws AdminProcessingException{
        synchronized (this.vdb) {
            ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
            ConnectorManager cr = cmr.getConnectorManager(sourceName);
            if(cr == null) {
                throw new AdminProcessingException(RuntimePlugin.Event.TEIID40091, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40091, sourceName, this.vdb.getName(), this.vdb.getVersion()));
            }

            String previousTranslatorName = cr.getTranslatorName();
            String previousDsName = cr.getConnectionName();

            //modify all source elements in all models
            for (ModelMetaData m : this.vdb.getModelMetaDatas().values()) {
                SourceMappingMetadata mapping = m.getSourceMapping(sourceName);
                if (mapping != null) {
                    mapping.setTranslatorName(translatorName);
                    mapping.setConnectionJndiName(dsName);
                }
            }

            boolean success = false;
            try {
                this.listener.dataSourceChanged(null, sourceName, translatorName, dsName);

                ReplaceResult rr = new ReplaceResult();
                if (dsName != null) {
                    rr.isNew = !dsExists(dsName, cmr);
                }
                boolean replaced = getVDBStatusChecker().dataSourceReplaced(vdb.getName(), vdb.getVersion(), null, sourceName, translatorName, dsName);
                if (replaced && previousDsName != null && !dsExists(previousDsName, cmr)) {
                    rr.removedDs = previousDsName;
                }
                success = true;
                return rr;
            } finally {
                if (!success) {
                    for (ModelMetaData m : this.vdb.getModelMetaDatas().values()) {
                        SourceMappingMetadata mapping = m.getSourceMapping(sourceName);
                        if (mapping != null) {
                            mapping.setTranslatorName(previousTranslatorName);
                            mapping.setConnectionJndiName(previousDsName);
                        }
                    }
                }
            }
        }
    }

    public ReplaceResult addSource(String modelName, String sourceName, String translatorName, String dsName) throws AdminProcessingException{
        synchronized (this.vdb) {
            ModelMetaData model = this.vdb.getModel(modelName);
            if (model == null) {
                 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40090, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40090, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            if (!model.isSupportsMultiSourceBindings()) {
                throw new AdminProcessingException(RuntimePlugin.Event.TEIID40108, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40108, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            SourceMappingMetadata source = model.getSourceMapping(sourceName);
            if(source != null) {
                 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40107, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40107, sourceName, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            boolean success = false;
            try {
                SourceMappingMetadata mapping = new SourceMappingMetadata(sourceName, translatorName, dsName);
                boolean updated = getVDBStatusChecker().updateSource(vdb.getName(), vdb.getVersion(), mapping, false);

                model.addSourceMapping(mapping);
                this.listener.dataSourceChanged(modelName, sourceName, translatorName, dsName);

                ReplaceResult rr = new ReplaceResult();
                if (dsName != null && updated) {
                    ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
                    rr.isNew = !dsExists(dsName, cmr);
                }
                success = true;
                return rr;
            } finally {
                if (!success) {
                    model.getSources().remove(sourceName);
                }
            }
        }
    }

    public ReplaceResult removeSource(String modelName, String sourceName) throws AdminProcessingException{
        synchronized (this.vdb) {
            ModelMetaData model = this.vdb.getModel(modelName);
            if (model == null) {
                 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40090, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40090, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            if (!model.isSupportsMultiSourceBindings()) {
                throw new AdminProcessingException(RuntimePlugin.Event.TEIID40108, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40108, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            if (model.getSources().size() == 1) {
                throw new AdminProcessingException(RuntimePlugin.Event.TEIID40109, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40109, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            SourceMappingMetadata source = model.getSources().remove(sourceName);
            if(source == null) {
                 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40091, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40091, sourceName, modelName, this.vdb.getName(), this.vdb.getVersion()));
            }
            if (model.getSources().size() == 1) {
                //we default to multi-source with multiple sources, so now we need to explicitly set to true
                model.setSupportsMultiSourceBindings(true);
            }
            String previousDsName = source.getConnectionJndiName();
            boolean success = false;
            try {
                this.listener.dataSourceChanged(modelName, sourceName, null, null);
                ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
                //detect if the ConnectorManager is still used
                boolean exists = false;
                for (ModelMetaData m : this.vdb.getModelMetaDatas().values()) {
                    if (m == model) {
                        continue;
                    }
                    if (m.getSourceMapping(sourceName) != null) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    cmr.removeConnectorManager(sourceName);
                }
                ReplaceResult rr = new ReplaceResult();
                if (!dsExists(previousDsName, cmr)) {
                    rr.removedDs = previousDsName;
                }
                success = true;
                return rr;
            } finally {
                if (!success) {
                    //TODO: this means that the order has changed
                    model.addSourceMapping(source);
                }
            }
        }
    }

    private boolean dsExists(String dsName, ConnectorManagerRepository cmr) {
        String baseDsName = VDBStatusChecker.stripContext(dsName);
        for (ConnectorManager cm : cmr.getConnectorManagers().values()) {
            if (baseDsName.equals(VDBStatusChecker.stripContext(cm.getConnectionName()))) {
                return true;
            }
        }
        return false;
    }

    public void restart(List<String> modelNames) throws AdminProcessingException {
        synchronized(this.vdb) {
            this.restartInProgress = true;
            this.listener.onRestart(modelNames);
        }
    }

    private DataPolicyMetadata getPolicy(String policyName)
            throws AdminProcessingException {
        DataPolicyMetadata policy = vdb.getDataPolicyMap().get(policyName);

        if (policy == null) {
             throw new AdminProcessingException(RuntimePlugin.Event.TEIID40092, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40092, policyName, vdb.getName(), vdb.getVersion()));
        }
        return policy;
    }

    public boolean isRestartInProgress() {
        return this.restartInProgress;
    }

    public VDBMetaData getVdb() {
        return vdb;
    }

    protected abstract VDBStatusChecker getVDBStatusChecker();
}
