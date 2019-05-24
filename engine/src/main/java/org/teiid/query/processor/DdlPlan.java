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

package org.teiid.query.processor;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Arrays;
import java.util.List;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.events.EventDistributor;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;

public class DdlPlan extends ProcessorPlan {

    public static final boolean ALLOW_ALTER = PropertiesUtils.getHierarchicalProperty("org.teiid.allowAlter", true, Boolean.class); //$NON-NLS-1$

    class AlterProcessor extends LanguageVisitor {
        DQPWorkContext workContext = getContext().getDQPWorkContext();
        VDBMetaData vdb = getContext().getVdb();
        TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);

        private MetadataRepository getMetadataRepository(VDBMetaData vdb, String schemaName) {
            ModelMetaData model = vdb.getModel(schemaName);
            return model.getAttachment(MetadataRepository.class);
        }

        @Override
        public void visit(AlterView obj) {
            Table t = (Table)obj.getTarget().getMetadataID();
            String sql = obj.getDefinition().toString();

            MetadataRepository metadataRepository = getMetadataRepository(vdb, t.getParent().getName());
            if (metadataRepository != null) {
                metadataRepository.setViewDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, sql);
            }
            alterView(vdb, t, sql, false);
            if (pdm.getEventDistributor() != null) {
                pdm.getEventDistributor().setViewDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t.getParent().getName(), t.getName(), sql);
            }
        }

        @Override
        public void visit(AlterProcedure obj) {
            Procedure p = (Procedure)obj.getTarget().getMetadataID();
            String sql = obj.getDefinition().toString();

            MetadataRepository metadataRepository = getMetadataRepository(vdb, p.getParent().getName());
            if (metadataRepository != null) {
                metadataRepository.setProcedureDefinition(workContext.getVdbName(), workContext.getVdbVersion(), p, sql);
            }
            alterProcedureDefinition(vdb, p, sql, false);
            if (pdm.getEventDistributor() != null) {
                pdm.getEventDistributor().setProcedureDefinition(workContext.getVdbName(), workContext.getVdbVersion(), p.getParent().getName(), p.getName(), sql);
            }
        }

        @Override
        public void visit(AlterTrigger obj) {
            Table t = (Table)obj.getTarget().getMetadataID();
            String sql = null;
            TriggerEvent event = obj.getEvent();
            if (obj.getEnabled() == null) {
                if (obj.isCreate()) {
                    if (getPlanForEvent(t, event) != null) {
                         throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30156, t.getName(), obj.getEvent())));
                    }
                } else if (getPlanForEvent(t, event) == null) {
                     throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30158, t.getName(), obj.getEvent())));
                }
                sql = obj.getDefinition().toString();
            } else if (getPlanForEvent(t, event) == null) {
                 throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30158, t.getName(), obj.getEvent())));
            }
            MetadataRepository metadataRepository = getMetadataRepository(vdb, t.getParent().getName());
            if (metadataRepository != null) {
                if (sql != null) {
                    metadataRepository.setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), sql);
                } else {
                    metadataRepository.setInsteadOfTriggerEnabled(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), obj.getEnabled());
                }
            }
            alterInsteadOfTrigger(vdb, t, sql, obj.getEnabled(), event, false);
            if (pdm.getEventDistributor() != null) {
                pdm.getEventDistributor().setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t.getParent().getName(), t.getName(), obj.getEvent(), sql, obj.getEnabled());
            }
        }
    }

    public static void alterView(final VDBMetaData vdb, final Table t, final String sql, boolean updateStore) {
        TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
        DatabaseStore store = vdb.getAttachment(DatabaseStore.class);

        try {
            Command command = QueryParser.getQueryParser().parseCommand(t.getSelectTransformation());
            QueryResolver.resolveCommand(command, metadata);
            MetadataValidator.determineDependencies(t, command);
        } catch (TeiidException e) {
            //should have been caught in validation, but this logic
            //is also not mature so since there is no lock on the vdb
            //it is possible that the plan is no longer valid at this point due
            //to a concurrent execution
        }
        t.setSelectTransformation(sql);
        t.setLastModified(System.currentTimeMillis());
        metadata.addToMetadataCache(t, "transformation/"+SQLConstants.Reserved.SELECT, null); //$NON-NLS-1$
    }

    public static class SetPropertyProcessor {
        private MetadataRepository metadataRepository;
        private EventDistributor eventDistributor;

        public SetPropertyProcessor(MetadataRepository metadataRepository,
                EventDistributor eventDistributor) {
            this.metadataRepository = metadataRepository;
            this.eventDistributor = eventDistributor;
        }

        public String setProperty(final VDBMetaData vdb, final AbstractMetadataRecord record, final String key, final String value) throws TeiidProcessingException {
            if (!ALLOW_ALTER) {
                throw new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31296));
            }
            if (metadataRepository != null) {
                metadataRepository.setProperty(vdb.getName(), vdb.getVersion(), record, key, value);
            }
            String result = DdlPlan.setProperty(vdb, record, key, value);
            if (eventDistributor != null) {
                eventDistributor.setProperty(vdb.getName(), vdb.getVersion(), record.getUUID(), key, value);
            }
            return result;
        }

    }

    public static String setProperty(final VDBMetaData vdb, final AbstractMetadataRecord record, final String key, final String value) {
       TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
       String result = record.setProperty(key, value);
       metadata.addToMetadataCache(record, "transformation/matview", null); //$NON-NLS-1$
       if (record instanceof Table) {
           ((Table)record).setLastModified(System.currentTimeMillis());
       } else if (record instanceof Procedure) {
           ((Procedure)record).setLastModified(System.currentTimeMillis());
       }
       return result;
    }

    public static void setColumnStats(final VDBMetaData vdb, Column column, final ColumnStats columnStats) {
        column.setColumnStats(columnStats);
        if (column.getParent() instanceof Table) {
            ((Table)column.getParent()).setLastModified(System.currentTimeMillis());
        }
    }

    public static void setTableStats(final VDBMetaData vdb, final Table table, final TableStats tableStats) {
        table.setTableStats(tableStats);
        table.setLastModified(System.currentTimeMillis());
    }

    public static void alterProcedureDefinition(final VDBMetaData vdb, final Procedure p, final String sql, boolean updateStore) {
        TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
        DatabaseStore store = vdb.getAttachment(DatabaseStore.class);

        try {
            Command command = QueryParser.getQueryParser().parseProcedure(p.getQueryPlan(), false);
            QueryResolver.resolveCommand(command, new GroupSymbol(p.getFullName()), Command.TYPE_STORED_PROCEDURE, metadata, false);
            MetadataValidator.determineDependencies(p, command);
        } catch (TeiidException e) {
            //should have been caught in validation, but this logic
            //is also not mature so since there is no lock on the vdb
            //it is possible that the plan is no longer valid at this point due
            //to a concurrent execution
        }
        p.setQueryPlan(sql);
        p.setLastModified(System.currentTimeMillis());
        metadata.addToMetadataCache(p, "transformation/"+StoredProcedure.class.getSimpleName().toUpperCase(), null); //$NON-NLS-1$
    }

    public static void alterInsteadOfTrigger(final VDBMetaData vdb, final Table t,
            final String sql, final Boolean enabled, final TriggerEvent event, boolean updateStore) {
        switch (event) {
        case DELETE:
            if (sql != null) {
                t.setDeletePlan(sql);
            } else {
                t.setDeletePlanEnabled(enabled);
            }
            break;
        case INSERT:
            if (sql != null) {
                t.setInsertPlan(sql);
            } else {
                t.setInsertPlanEnabled(enabled);
            }
            break;
        case UPDATE:
            if (sql != null) {
                t.setUpdatePlan(sql);
            } else {
                t.setUpdatePlanEnabled(enabled);
            }
            break;
        }
        TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
        indexMetadata.addToMetadataCache(t, "transformation/"+event, null); //$NON-NLS-1$
        t.setLastModified(System.currentTimeMillis());
    }

    private static String getPlanForEvent(Table t, TriggerEvent event) {
        switch (event) {
        case DELETE:
            return t.getDeletePlan();
        case INSERT:
            return t.getInsertPlan();
        case UPDATE:
            return t.getUpdatePlan();
        }
        throw new AssertionError();
    }

    private Command command;
    private ProcessorDataManager pdm;

    public DdlPlan(Command command) {
        this.command = command;
    }

    @Override
    public ProcessorPlan clone() {
        return new DdlPlan(command);
    }

    @Override
    public void close() throws TeiidComponentException {
    }

    @Override
    public List getOutputElements() {
        return command.getProjectedSymbols();
    }

    @Override
    public void initialize(CommandContext context,
            ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        this.setContext(context);
        this.pdm = dataMgr;
    }

    @Override
    public TupleBatch nextBatch() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        TupleBatch tupleBatch = new TupleBatch(1, new List[] {Arrays.asList(0)});
        tupleBatch.setTerminationFlag(true);
        return tupleBatch;
    }

    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
        if (!ALLOW_ALTER) {
            throw new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31296));
        }
        AlterProcessor ap = new AlterProcessor();
        try {
            command.acceptVisitor(ap);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof TeiidProcessingException) {
                throw (TeiidProcessingException)e.getCause();
            }
            throw e;
        }
    }

    @Override
    public PlanNode getDescriptionProperties() {
        PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_SQL, this.command.toString());
        return props;
    }

    @Override
    public String toString() {
        return command.toString();
    }

}
