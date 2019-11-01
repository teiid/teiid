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

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.id.IDGenerator;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.Procedure;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempCapabilitiesFinder;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;


/**
 * <p>This Class produces a ProcessorPlan object (a plan for query execution) from a
 * user's command and a source of metadata.
 */
public class QueryOptimizer {

    private static final CommandPlanner PROCEDURE_PLANNER = new ProcedurePlanner();
    private static final CommandPlanner BATCHED_UPDATE_PLANNER = new BatchedUpdatePlanner();
    private static final CommandPlanner DDL_PLANNER = new DdlPlanner();
    private static final CommandPlanner SOURCE_EVENT_PLANNER = new SourceTriggerActionPlanner();

    // Can't construct
    private QueryOptimizer() {}

    public static ProcessorPlan optimizePlan(Command command, QueryMetadataInterface metadata, IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryMetadataException, TeiidComponentException, QueryPlannerException {

        if (analysisRecord == null) {
            analysisRecord = new AnalysisRecord(false, false);
        }

        if (context == null) {
            context = new CommandContext();
        }

        if (!(capFinder instanceof TempCapabilitiesFinder)) {
            capFinder = new TempCapabilitiesFinder(capFinder);
        }

        boolean debug = analysisRecord.recordDebug();

        if (!(metadata instanceof TempMetadataAdapter)) {
            metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
        }

        if (context.getMetadata() == null) {
            context.setMetadata(metadata);
        }

        // Create an ID generator that can be used for all plans to generate unique data node IDs
        if(idGenerator == null) {
            idGenerator = new IDGenerator();
        }

        if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZE: \n" + command); //$NON-NLS-1$
        }

        command = command.getActualCommand();

        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            if (insert.isUpsert()) {
                //if not supported or there are trigger actions, then rewrite as a procedure
                //we do this here since we're changing the command type.
                //TODO: we could push this back into the rewrite, but it will need to be capabilities aware
                GroupSymbol group = insert.getGroup();
                Object modelId = metadata.getModelID(group.getMetadataID());
                boolean supportsUpsert = CapabilitiesUtil.supports(Capability.UPSERT, modelId, metadata, capFinder);
                if (!supportsUpsert) {
                    try {
                        command = QueryRewriter.rewriteAsUpsertProcedure(insert, metadata, context);
                    } catch (TeiidProcessingException e) {
                        throw new QueryPlannerException(e);
                    }
                    if(debug) {
                        analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
                        analysisRecord.println("OPTIMIZE UPSERT PROCEDURE: \n" + command); //$NON-NLS-1$
                    }
                }
            }
        }

        ProcessorPlan result = null;

        switch (command.getType()) {
        case Command.TYPE_UPDATE_PROCEDURE:
            CreateProcedureCommand cupc = (CreateProcedureCommand)command;
            if (cupc.getUpdateType() != Command.TYPE_UNKNOWN || cupc.getVirtualGroup() == null) {
                //row update procedure or anon block
                result = planProcedure(command, metadata, idGenerator, capFinder, analysisRecord, context);
            } else {
                Object pid = cupc.getVirtualGroup().getMetadataID();
                if (pid instanceof TempMetadataID) {
                    TempMetadataID tid = (TempMetadataID)pid;
                    if (tid.getOriginalMetadataID() != null) {
                        pid = tid.getOriginalMetadataID();
                    }
                }
                String fullName = metadata.getFullName(pid);
                fullName = "procedure cache:" + fullName; //$NON-NLS-1$
                PreparedPlan pp = context.getPlan(fullName);
                if (pp == null) {
                    Determinism determinismLevel = context.resetDeterminismLevel();
                    try {
                        CommandContext clone = context.clone();
                        ProcessorPlan plan = planProcedure(command, metadata, idGenerator, capFinder, analysisRecord, clone);
                        //note that this is not a full prepared plan.  It is not usable by user queries.
                        if (pid instanceof Procedure) {
                            clone.accessedPlanningObject(pid);
                        }
                        pp = new PreparedPlan();
                        pp.setPlan(plan, clone);
                        context.putPlan(fullName, pp, context.getDeterminismLevel());
                    } finally {
                        context.setDeterminismLevel(determinismLevel);
                    }
                }
                result = pp.getPlan().clone();
                for (Object id : pp.getAccessInfo().getObjectsAccessed()) {
                    context.accessedPlanningObject(id);
                }
            }
            break;
        case Command.TYPE_BATCHED_UPDATE:
            result = BATCHED_UPDATE_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
            break;
        case Command.TYPE_ALTER_PROC:
        case Command.TYPE_ALTER_TRIGGER:
        case Command.TYPE_ALTER_VIEW:
            result = DDL_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
            break;
        case Command.TYPE_SOURCE_EVENT:
            result = SOURCE_EVENT_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
            break;
        default:
            try {
                RelationalPlanner planner = new RelationalPlanner();
                planner.initialize(command, idGenerator, metadata, capFinder, analysisRecord, context);
                result = planner.optimize(command);
            } catch (QueryResolverException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30245, e);
            }
        }

        if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZATION COMPLETE:"); //$NON-NLS-1$
            analysisRecord.println("PROCESSOR PLAN:\n" + result); //$NON-NLS-1$
            analysisRecord.println("============================================================================");         //$NON-NLS-1$
        }

        return result;
    }

    private static ProcessorPlan planProcedure(Command command,
            QueryMetadataInterface metadata, IDGenerator idGenerator,
            CapabilitiesFinder capFinder, AnalysisRecord analysisRecord,
            CommandContext context) throws TeiidComponentException,
            QueryPlannerException, QueryMetadataException {
        ProcessorPlan result;
        try {
            command = QueryRewriter.rewrite(command, metadata, context);
        } catch (TeiidProcessingException e) {
             throw new QueryPlannerException(e);
        }
        result = PROCEDURE_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
        return result;
    }

}
