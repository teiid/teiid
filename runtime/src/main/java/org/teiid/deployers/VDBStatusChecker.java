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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;


public abstract class VDBStatusChecker {
    private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$

    /**
     * @param translatorName
     */
    public void translatorAdded(String translatorName) {
    }

    /**
     * @param translatorName
     */
    public void translatorRemoved(String translatorName) {
    }

    public void dataSourceAdded(String dataSourceName, VDBKey vdbKey) {
        dataSourceName = stripContext(dataSourceName);
        if (vdbKey == null) {
            //scan all
            resourceAdded(dataSourceName);
        } else {
            CompositeVDB cvdb = getVDBRepository().getCompositeVDB(vdbKey);
            if (cvdb == null) {
                return;
            }
            VDBMetaData vdb = cvdb.getVDB();
            resourceAdded(dataSourceName, new LinkedList<Runnable>(), vdb);
        }
    }

    public static String stripContext(String dataSourceName) {
        if (dataSourceName == null) {
            return null;
        }
        if (dataSourceName.startsWith(JAVA_CONTEXT)) {
            dataSourceName = dataSourceName.substring(5);
        }
        return dataSourceName;
    }

    /**
     *
     * @param dataSourceName
     * @param vdbKey which cannot be null
     */
    public void dataSourceRemoved(String dataSourceName, VDBKey vdbKey) {
        dataSourceName = stripContext(dataSourceName);
        CompositeVDB cvdb = getVDBRepository().getCompositeVDB(vdbKey);
        if (cvdb == null) {
            return;
        }
        VDBMetaData vdb = cvdb.getVDB();
        if (vdb.getStatus() == Status.FAILED) {
            return;
        }
        synchronized (vdb) {
            ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
            for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
                String sourceName = getSourceName(dataSourceName, model);
                if (sourceName == null) {
                    continue;
                }
                Severity severity = Severity.WARNING;
                ConnectorManager cm = cmr.getConnectorManager(sourceName);
                if (cm.getExecutionFactory().isSourceRequired() && vdb.getStatus() == Status.ACTIVE) {
                    severity = Severity.ERROR;
                }
                String msg = RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40012, vdb.getName(), vdb.getVersion(), dataSourceName);
                model.addRuntimeMessage(severity, msg);
                LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);
            }
        }
    }

    public boolean dataSourceReplaced(String vdbName, String vdbVersion,
            String modelName, String sourceName, String translatorName,
            String dsName) throws AdminProcessingException {
        return updateSource(vdbName, vdbVersion, new SourceMappingMetadata(sourceName, translatorName, dsName), true);
    }

    /**
     * @return true if the datasource is new to the vdb
     * @throws AdminProcessingException
     */
    public boolean updateSource(String vdbName, String vdbVersion, SourceMappingMetadata mapping, boolean replace) throws AdminProcessingException {
        String dsName = stripContext(mapping.getConnectionJndiName());

        VDBMetaData vdb = getVDBRepository().getLiveVDB(vdbName, vdbVersion);
        if (vdb == null || vdb.getStatus() == Status.FAILED) {
            return false;
        }

        synchronized (vdb) {
            ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
            ConnectorManager existing = cmr.getConnectorManager(mapping.getName());
            try {
                cmr.createConnectorManager(vdb, cmr.getProvider(), mapping, replace);
            } catch (TeiidException e) {
                throw new AdminProcessingException(RuntimePlugin.Event.TEIID40033, e);
            }
            if (mapping.getConnectionJndiName() != null && (existing == null || !dsName.equals(existing.getConnectionName()))) {
                List<Runnable> runnables = new ArrayList<Runnable>();
                resourceAdded(dsName, runnables, vdb);
                return true;
            }
            return false;
        }
    }

    void resourceAdded(String resourceName) {
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (CompositeVDB cvdb:getVDBRepository().getCompositeVDBs()) {
            VDBMetaData vdb = cvdb.getVDB();
            if (vdb.getStatus() == Status.FAILED) {
                continue;
            }
            resourceAdded(resourceName, runnables, vdb);
        }
    }

    private void resourceAdded(String resourceName, List<Runnable> runnables,
            VDBMetaData vdb) {
        synchronized (vdb) {
            ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
            boolean usesResourse = false;
            for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
                if (!model.hasRuntimeMessages()) {
                    continue;
                }

                String sourceName = getSourceName(resourceName, model);
                if (sourceName == null) {
                    continue;
                }

                usesResourse = true;
                ConnectorManager cm = cmr.getConnectorManager(sourceName);
                checkStatus(runnables, vdb, model, cm);
            }

            if (usesResourse) {
                updateVDB(runnables, vdb);
            }
        }
    }

    private void updateVDB(List<Runnable> runnables, VDBMetaData vdb) {
        if (!runnables.isEmpty()) {
            //the task themselves will set the status on completion/failure
            for (Runnable runnable : runnables) {
                getExecutor().execute(runnable);
            }
            runnables.clear();
        } else if (vdb.hasErrors()) {
            LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,vdb.getName(), vdb.getVersion(), vdb.getStatus()));
        }
    }

    private void checkStatus(List<Runnable> runnables, VDBMetaData vdb,
            ModelMetaData model, ConnectorManager cm) {
        //get the pending metadata load
        model.removeAttachment(VDBStatusChecker.class);
        Runnable r = model.removeAttachment(Runnable.class);
        if (r != null) {
            runnables.add(r);
        } else {
            String status = cm.getStausMessage();
            if (status != null && status.length() > 0) {
                Severity severity = vdb.getStatus() == Status.LOADING?Severity.WARNING:Severity.ERROR;
                model.addRuntimeMessage(severity, status);
                LogManager.logInfo(LogConstants.CTX_RUNTIME, status);
            } else if (vdb.getStatus() != Status.LOADING){
                model.clearRuntimeMessages();
            } else {
                //mark the model to indicate that it should be reloaded if it
                //is currently failing a load
                model.addAttachment(VDBStatusChecker.class, this);
            }
        }
    }

    private String getSourceName(String factoryName, ModelMetaData model) {
        for (SourceMappingMetadata source:model.getSources().values()) {
            String jndiName = source.getConnectionJndiName();
            if (jndiName == null) {
                continue;
            }
            jndiName = stripContext(jndiName);
            if (factoryName.equals(jndiName)) {
                return source.getName();
            }
        }
        return null;
    }

    public abstract Executor getExecutor();

    public abstract VDBRepository getVDBRepository();

}
