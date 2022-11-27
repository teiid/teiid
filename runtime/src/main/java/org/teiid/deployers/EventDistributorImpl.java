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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DataTierManagerImpl;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.SourceTriggerActionPlanner.SourceEventCommand;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.runtime.RuntimePlugin;

public abstract class EventDistributorImpl implements EventDistributor {
    private Set<EventListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<EventListener, Boolean>());

    public abstract VDBRepository getVdbRepository();

    public abstract DQPCore getDQPCore();

    public EventDistributorImpl() {
        getVdbRepository().addListener(new VDBLifeCycleListener() {
            @Override
            public void removed(String name, CompositeVDB vdb) {
                for(EventListener el:EventDistributorImpl.this.listeners) {
                    try {
                        el.vdbUndeployed(name, vdb.getVDB().getVersion());
                    } catch (Exception e) {
                        LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "undeployed", vdb.getVDBKey())); //$NON-NLS-1$
                    }
                }
            }
            @Override
            public void finishedDeployment(String name, CompositeVDB vdb) {
                for(EventListener el:EventDistributorImpl.this.listeners) {
                    try {
                        if (vdb.getVDB().getStatus().equals(Status.ACTIVE)) {
                            el.vdbLoaded(vdb.getVDB());
                        }
                        else {
                            el.vdbLoadFailed(vdb.getVDB());
                        }
                    } catch (Exception e) {
                        LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "finished deployment", vdb.getVDBKey())); //$NON-NLS-1$
                    }
                }
            }
            @Override
            public void added(String name, CompositeVDB vdb) {
                for(EventListener el:EventDistributorImpl.this.listeners) {
                    try {
                        el.vdbDeployed(name, vdb.getVDB().getVersion());
                    } catch (Exception e) {
                        LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "deployed", vdb.getVDBKey())); //$NON-NLS-1$
                    }
                }
            }
            @Override
            public void beforeRemove(String name, CompositeVDB vdb) {
            }
        });
    }

    @Override
    public void updateMatViewRow(String vdbName, int vdbVersion, String schema,
            String viewName, List<?> tuple, boolean delete) {
        updateMatViewRow(vdbName, String.valueOf(vdbVersion), schema, viewName, tuple, delete);
    }

    @Override
    public void updateMatViewRow(String vdbName, String vdbVersion, String schema,
            String viewName, List<?> tuple, boolean delete) {
        VDBMetaData metadata = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        if (metadata != null) {
            GlobalTableStore gts = metadata.getAttachment(GlobalTableStore.class);
            if (gts != null) {
                try {
                    gts.updateMatViewRow((RelationalPlanner.MAT_PREFIX + schema + '.' + viewName).toUpperCase(), tuple, delete);
                } catch (TeiidComponentException e) {
                    LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40013, "updateMatViewRow")); //$NON-NLS-1$
                }
            }
        }
    }

    @Override
    public void dataModification(String vdbName, int vdbVersion, String schema,
            String... tableNames) {
        dataModification(vdbName, String.valueOf(vdbVersion), schema, tableNames);
    }

    @Override
    public void dataModification(String vdbName, String vdbVersion, String schema,    String... tableNames) {
        updateModified(true, vdbName, vdbVersion, schema, tableNames);
    }

    private void updateModified(boolean data, String vdbName, String vdbVersion, String schema,
            String... objectNames) {
        Schema s = getSchema(vdbName, vdbVersion, schema);
        if (s == null) {
            return;
        }
        long ts = System.currentTimeMillis();
        for (String name:objectNames) {
            Table table = s.getTables().get(name);
            if (table == null) {
                continue;
            }
            if (data) {
                table.setLastDataModification(ts);
            } else {
                table.setLastModified(ts);
            }
        }
    }

    @Override
    public void setColumnStats(String vdbName, int vdbVersion,
            String schemaName, String tableName, String columnName,
            ColumnStats stats) {
        setColumnStats(vdbName, String.valueOf(vdbVersion), schemaName, tableName, columnName, stats);
    }

    @Override
    public void setColumnStats(String vdbName, String vdbVersion,
            String schemaName, String tableName, String columnName,
            ColumnStats stats) {
        VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
        if (t == null) {
            return;
        }
        Column c = t.getColumnByName(columnName);
        if (c != null) {
            DdlPlan.setColumnStats(vdb, c, stats);
        }
    }

    @Override
    public void setTableStats(String vdbName, int vdbVersion,
            String schemaName, String tableName, TableStats stats) {
        setTableStats(vdbName, String.valueOf(vdbVersion), schemaName, tableName, stats);
    }

    @Override
    public void setTableStats(String vdbName, String vdbVersion,
            String schemaName, String tableName, TableStats stats) {
        VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
        if (t == null) {
            return;
        }
        DdlPlan.setTableStats(vdb, t, stats);
    }

    private Table getTable(String vdbName, String vdbVersion, String schemaName,
            String tableName) {
        Schema s = getSchema(vdbName, vdbVersion, schemaName);
        if (s == null) {
            return null;
        }
        return s.getTables().get(tableName);
    }

    private Schema getSchema(String vdbName, String vdbVersion, String schemaName) {
        VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        if (vdb == null) {
            return null;
        }
        TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
        if (tm == null) {
            return null;
        }
        return tm.getMetadataStore().getSchemas().get(schemaName);
    }

    @Override
    public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
            String schema, String viewName, TriggerEvent triggerEvent,
            String triggerDefinition, Boolean enabled) {
        setInsteadOfTriggerDefinition(vdbName, String.valueOf(vdbVersion), schema, viewName, triggerEvent, triggerDefinition, enabled);
    }

    @Override
    public void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion,
            String schema, String viewName, TriggerEvent triggerEvent,
            String triggerDefinition, Boolean enabled) {
        Table t = getTable(vdbName, vdbVersion, schema, viewName);
        if (t == null) {
            return;
        }
        DdlPlan.alterInsteadOfTrigger(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, triggerDefinition, enabled, triggerEvent, true);
    }

    @Override
    public void setProcedureDefinition(String vdbName, int vdbVersion,
            String schema, String procName, String definition) {
        setProcedureDefinition(vdbName, String.valueOf(vdbVersion), schema, procName, definition);
    }

    @Override
    public void setProcedureDefinition(String vdbName, String vdbVersion,String schema, String procName, String definition) {
        Schema s = getSchema(vdbName, vdbVersion, schema);
        if (s == null) {
            return;
        }
        Procedure p = s.getProcedures().get(procName);
        if (p == null) {
            return;
        }
        DdlPlan.alterProcedureDefinition(getVdbRepository().getLiveVDB(vdbName, vdbVersion), p, definition, true);
    }

    @Override
    public void setViewDefinition(String vdbName, int vdbVersion,
            String schema, String viewName, String definition) {
        setViewDefinition(vdbName, String.valueOf(vdbVersion), schema, viewName, definition);
    }

    @Override
    public void setViewDefinition(String vdbName, String vdbVersion, String schema, String viewName, String definition) {
        Table t = getTable(vdbName, vdbVersion, schema, viewName);
        if (t == null) {
            return;
        }
        DdlPlan.alterView(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, definition, true);
    }

    @Override
    public void setProperty(String vdbName, int vdbVersion, String uuid,
            String name, String value) {
        setProperty(vdbName, String.valueOf(vdbVersion), uuid, name, value);
    }

    @Override
    public void setProperty(String vdbName, String vdbVersion, String uuid,
            String name, String value) {
        VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        if (vdb == null) {
            return;
        }
        TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
        if (tm == null) {
            return;
        }
        AbstractMetadataRecord record = DataTierManagerImpl.getByUuid(tm.getMetadataStore(), uuid);
        if (record != null) {
            DdlPlan.setProperty(vdb, record, name, value);
        }
    }

    @Override
    public ResultsFuture<?> dataModification(String vdbName, String vdbVersion,
            String schema, String tableName, Object[] oldValues, Object[] newValues,
            String[] columnNames) {
        VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        if (vdb == null) {
            return null;
        }
        TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
        if (tm == null) {
            return null;
        }
        //lookup, call triggers
        Table t = getTable(vdbName, vdbVersion, schema, tableName);
        if (t == null) {
            return null;
        }
        //notify of just the table modification
        dataModification(vdbName, vdbVersion, schema, tableName);
        if (oldValues == null && newValues == null) {
            return null;
        }
        if (!t.getTriggers().isEmpty()) {
            if (columnNames != null) {
                if ((oldValues != null && oldValues.length != columnNames.length)
                        || (newValues != null && newValues.length != columnNames.length)) {
                    throw new IllegalArgumentException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40162));
                }
            } else {
               if ((oldValues != null && oldValues.length != t.getColumns().size())
                        || (newValues != null && newValues.length != t.getColumns().size())) {
                   throw new IllegalArgumentException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40163));
                }
            }

            //create command
            SourceEventCommand sec = new SourceEventCommand(t, oldValues, newValues, columnNames);
            try {
                return DQPCore.executeQuery(sec, vdb, "admin", "event-distributor", -1, getDQPCore(), new DQPCore.ResultsListener() { //$NON-NLS-1$ //$NON-NLS-2$
                    @Override
                    public void onResults(List<String> columns, List<? extends List<?>> results) throws Exception {
                        //no result
                    }
                });
            } catch (Throwable throwable) {
                throw new TeiidRuntimeException(throwable);
            }
        }
        return null;
    }

    @Override
    public void register(EventListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void unregister(EventListener listener) {
        this.listeners.remove(listener);
    }
}
