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

package org.teiid.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.metadata.Trigger;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.ForEachRowPlan;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;

/**
 * Handles the planning of triggers from source events
 */
public final class SourceTriggerActionPlanner implements CommandPlanner {

    /**
     * TODO: elevate the transaction handling?
     */
    public static class CompositeProcessorPlan extends ProcessorPlan {

        private List<ProcessorPlan> plans;
        private int planIndex;
        private boolean open;
        private List<String> names;
        private Table table;

        public CompositeProcessorPlan(List<ProcessorPlan> plans, List<String> names, Table t) {
            this.plans = plans;
            this.names = names;
            this.table = t;
        }

        @Override
        public List getOutputElements() {
            return Command.getUpdateCommandSymbol();
        }

        @Override
        public void open() throws TeiidComponentException,
                TeiidProcessingException {
        }

        @Override
        public void initialize(CommandContext context,
                ProcessorDataManager dataMgr, BufferManager bufferMgr) {
            super.initialize(context, dataMgr, bufferMgr);
            for (ProcessorPlan plan : plans) {
                plan.initialize(context, dataMgr, bufferMgr);
            }
        }

        @Override
        public TupleBatch nextBatch() throws BlockedException,
                TeiidComponentException, TeiidProcessingException {
            while (planIndex < plans.size()) {
                try {
                    if (!open) {
                        plans.get(planIndex).open();
                    }
                    plans.get(planIndex).nextBatch();
                    plans.get(planIndex).close();
                } catch (TeiidProcessingException e) {
                    LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31214, names.get(planIndex), table));
                } catch (BlockedException e) {
                    throw e;
                }
                //allow other exception types to bubble up
                open = false;
                planIndex++;
            }
            TupleBatch batch = new TupleBatch(1, new List<?>[0]);
            batch.setTerminationFlag(true);
            return batch;
        }

        @Override
        public void close() throws TeiidComponentException {

        }

        @Override
        public ProcessorPlan clone() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Represents a source event as a Command - is localized here
     * as it's not directly callable by a user
     */
    public static class SourceEventCommand extends Command {

        private Table table;
        private Object[] oldValues;
        private Object[] newValues;
        private String[] columnNames;

        public SourceEventCommand(Table t, Object[] old, Object[] newValues,
                String[] columnNames) {
            this.table = t;
            this.oldValues = old;
            this.newValues = newValues;
            this.columnNames = columnNames;
        }

        @Override
        public void acceptVisitor(LanguageVisitor visitor) {
        }

        @Override
        public int getType() {
            return TYPE_SOURCE_EVENT;
        }

        @Override
        public Object clone() {
            return this;
        }

        @Override
        public List<Expression> getProjectedSymbols() {
            return Command.getUpdateCommandSymbol();
        }

        @Override
        public boolean areResultsCachable() {
            return false;
        }

        public Table getTable() {
            return table;
        }

        public Object[] getOldValues() {
            return oldValues;
        }

        public Object[] getNewValues() {
            return newValues;
        }

        public String[] getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return "AFTER EVENT ON " + table; //$NON-NLS-1$
        }

    }

    @Override
    public ProcessorPlan optimize(Command command, IDGenerator idGenerator,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        SourceEventCommand sec = (SourceEventCommand)command;

        Map<Expression, Integer> lookup = new HashMap<Expression, Integer>();
        Map<ElementSymbol, Expression> params = new HashMap<ElementSymbol, Expression>();
        List<Object> tuple = new ArrayList<Object>();

        Map<String, Integer> map = null;

        if (sec.getColumnNames() != null) {
            map = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
            for (String name : sec.getColumnNames()) {
                map.put(name, map.size());
            }
        }

        GroupSymbol changingGroup = new GroupSymbol(ProcedureReservedWords.CHANGING);
        if (sec.newValues != null) {
            GroupSymbol newGroup = new GroupSymbol(SQLConstants.Reserved.NEW);
            newGroup.setMetadataID(sec.table);
            for (int i = 0; i < sec.getTable().getColumns().size(); i++) {
                Column c = sec.getTable().getColumns().get(i);
                Integer index = null;
                if (map != null) {
                    index = map.get(c.getName());
                } else {
                    index = i;
                }
                ElementSymbol newElement = new ElementSymbol(c.getName(), newGroup);
                newElement.setMetadataID(c);
                ElementSymbol changingElement = new ElementSymbol(c.getName(), changingGroup);
                lookup.put(newElement, tuple.size());
                lookup.put(changingElement, tuple.size() + 1);
                params.put(newElement, newElement);
                params.put(changingElement, changingElement);
                if (index == null) {
                    //not changing
                    tuple.add(new Constant(null));
                    tuple.add(new Constant(Boolean.FALSE));
                } else {
                    //changing
                    tuple.add(new Constant(DataTypeManager.convertToRuntimeType(sec.newValues[index], true)));
                    tuple.add(new Constant(Boolean.TRUE));
                }
            }
        }
        if (sec.oldValues != null) {
            GroupSymbol oldGroup = new GroupSymbol(SQLConstants.Reserved.OLD);
            oldGroup.setMetadataID(sec.table);
            for (int i = 0; i < sec.getTable().getColumns().size(); i++) {
                Column c = sec.getTable().getColumns().get(i);
                Integer index = null;
                if (map != null) {
                    index = map.get(c.getName());
                } else {
                    index = i;
                }
                ElementSymbol oldElement = new ElementSymbol(c.getName(), oldGroup);
                oldElement.setMetadataID(c);
                lookup.put(oldElement, tuple.size());
                params.put(oldElement, oldElement);
                if (index != null) {
                    tuple.add(new Constant(DataTypeManager.convertToRuntimeType(sec.oldValues[index], true)));
                }
            }
        }

        List<ProcessorPlan> plans = new ArrayList<ProcessorPlan>();
        List<String> names = new ArrayList<String>();

        for (Trigger tr : sec.getTable().getTriggers().values()) {
            int updateType = Command.TYPE_UPDATE;
            switch (tr.getEvent()) {
            case DELETE:
                updateType = Command.TYPE_DELETE;
                if (sec.newValues != null) {
                    continue;
                }
                break;
            case INSERT:
                updateType = Command.TYPE_INSERT;
                if (sec.oldValues != null) {
                    continue;
                }
                break;
            case UPDATE:
                if (sec.oldValues == null || sec.newValues == null) {
                    continue;
                }
                break;
            }
            //create plan
            ForEachRowPlan result = new ForEachRowPlan();
            result.setSingleRow(true);
            result.setParams(params);
            TriggerAction parseProcedure;
            GroupSymbol gs = new GroupSymbol(sec.table.getFullName());
            try {
                parseProcedure = (TriggerAction)QueryParser.getQueryParser().parseProcedure(tr.getPlan(), true);
                QueryResolver.resolveCommand(parseProcedure, gs, updateType, metadata.getDesignTimeMetadata(), false);
            } catch (QueryParserException e) {
                //should have been validated
                throw new TeiidComponentException(e);
            } catch (QueryResolverException e) {
                //should have been validated
                throw new TeiidComponentException(e);
            }
            CreateProcedureCommand cpc = new CreateProcedureCommand(parseProcedure.getBlock());
            gs.setMetadataID(sec.table);
            cpc.setVirtualGroup(gs);
            cpc.setUpdateType(updateType);
            ProcedurePlan rowProcedure = (ProcedurePlan)QueryOptimizer.optimizePlan(cpc, metadata, idGenerator, capFinder, analysisRecord, context);
            rowProcedure.setRunInContext(false);
            result.setRowProcedure(rowProcedure);
            result.setLookupMap(lookup);
            result.setTupleSource(new CollectionTupleSource(Arrays.asList(tuple).iterator()));
            plans.add(result);
            names.add(tr.getName());
        }

        return new CompositeProcessorPlan(plans, names, sec.table);
    }

}
