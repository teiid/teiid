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
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.id.IDGenerator;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.processor.BatchedUpdatePlan;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.BatchedUpdateNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.util.CommandContext;



/**
 * Planner for BatchedUpdateCommands
 * @since 4.2
 */
public class BatchedUpdatePlanner implements CommandPlanner {

    /**
     * Optimizes batched updates by batching all contiguous commands that relate to the same physical model.
     * For example, for the following batch of commands:
     * <br>
     * <ol>
     *      <li>1.  INSERT INTO physicalModel.myPhysical ...</li>
     *      <li>2.  UPDATE physicalModel.myPhysical ... </li>
     *      <li>3.  DELETE FROM virtualmodel.myVirtual ... </li>
     *      <li>4.  UPDATE virtualmodel.myVirtual ... </li>
     *      <li>5.  UPDATE physicalModel.myOtherPhysical ...</li>
     *      <li>6.  INSERT INTO physicalModel.myOtherPhysical ... <li>
     *      <li>7.  DELETE FROM physicalModel.myOtherPhysical ...</li>
     *      <li>8.  INSERT INTO physicalModel.myPhysical ... </li>
     *      <li>9.  INSERT INTO physicalModel.myPhysical ... </li>
     *      <li>10. INSERT INTO physicalModel.myPhysical ... </li>
     *      <li>11. INSERT INTO physicalModel.myPhysical ... </li>
     *      <li>12. INSERT INTO physicalModel.myPhysical ... </li>
     * </ol>
     * <br> this implementation will batch as follows: (1,2), (5, 6, 7), (8 thru 12).
     * The remaining commands/plans will be executed individually.
     * @see org.teiid.query.optimizer.CommandPlanner#optimize(Command, org.teiid.core.id.IDGenerator, org.teiid.query.metadata.QueryMetadataInterface, org.teiid.query.optimizer.capabilities.CapabilitiesFinder, org.teiid.query.analysis.AnalysisRecord, CommandContext)
     * @since 4.2
     */
    public ProcessorPlan optimize(Command command,
                                  IDGenerator idGenerator,
                                  QueryMetadataInterface metadata,
                                  CapabilitiesFinder capFinder,
                                  AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        BatchedUpdateCommand batchedUpdateCommand = (BatchedUpdateCommand)command;
        List<ProcessorPlan> childPlans = new ArrayList<ProcessorPlan>(batchedUpdateCommand.getUpdateCommands().size());
        List<Command> updateCommands = batchedUpdateCommand.getUpdateCommands();
        int numCommands = updateCommands.size();
        List<VariableContext> allContexts = batchedUpdateCommand.getVariableContexts();
        List<VariableContext> planContexts = null;
        if (allContexts != null) {
            planContexts = new ArrayList<VariableContext>(allContexts.size());
        }
        //pre-plan the commands to determine pushdown/batching eligibility
        for (int commandIndex = 0; commandIndex < numCommands; commandIndex++) {
            Command cmd = batchedUpdateCommand.getUpdateCommands().get(commandIndex);
            ProcessorPlan plan = cmd.getProcessorPlan();
            if (plan == null) {
                plan = QueryOptimizer.optimizePlan(cmd, metadata, idGenerator, capFinder, analysisRecord, context);
                cmd.setProcessorPlan(plan);
            }
        }
        for (int commandIndex = 0; commandIndex < numCommands; commandIndex++) {
            // Potentially the first command of a batch
            Command updateCommand = updateCommands.get(commandIndex);
            boolean commandWasBatched = false;
            // If this command can be placed in a batch
            if (isEligibleForBatching(updateCommand, metadata)) {
                // Get the model ID. Subsequent and contiguous commands that update a group in this model are candidates for this batch
                Object batchModelID = metadata.getModelID(getUpdatedGroup(updateCommand).getMetadataID());
                String modelName = metadata.getFullName(batchModelID);
                SourceCapabilities caps = capFinder.findCapabilities(modelName);
                // Only attempt batching if the source supports batching
                if (caps.supportsCapability(Capability.BATCHED_UPDATES)) {
                    // Start a new batch
                    List<Command> batch = new ArrayList<Command>();
                    List<VariableContext> contexts = new ArrayList<VariableContext>();
                    List<Boolean> shouldEvaluate = new ArrayList<Boolean>();
                    // This is the first command in a potential batch, so add it to the batch
                    batch.add(updateCommand);
                    if (allContexts != null) {
                        contexts.add(allContexts.get(commandIndex));
                        shouldEvaluate.add(Boolean.TRUE);
                    } else {
                        shouldEvaluate.add(EvaluatableVisitor.needsProcessingEvaluation(updateCommand));
                    }
                    // Find out if there are other commands called on the same physical model
                    // immediately and contiguously after this one
                    batchLoop: for (int batchIndex = commandIndex+1; batchIndex < numCommands; batchIndex++) {
                        Command batchingCandidate = updateCommands.get(batchIndex);
                        // If this command updates the same model, and is eligible for batching, add it to the batch
                        if (canBeAddedToBatch(batchingCandidate, batchModelID, metadata, capFinder)) {
                            batch.add(batchingCandidate);
                            if (allContexts != null) {
                                contexts.add(allContexts.get(batchIndex));
                                shouldEvaluate.add(Boolean.TRUE);
                            } else {
                                shouldEvaluate.add(EvaluatableVisitor.needsProcessingEvaluation(batchingCandidate));
                            }
                        } else { // Otherwise, stop batching at this point. The next command may well be the start of a new batch
                            break batchLoop;
                        }
                    }
                    // If two or more contiguous commands made on the same model were found, then batch them
                    if (batch.size() > 1) {
                        ProjectNode projectNode = new ProjectNode(idGenerator.nextInt());
                        // Create a BatchedUpdateNode that creates a batched request for the connector
                        BatchedUpdateNode batchNode = new BatchedUpdateNode(idGenerator.nextInt(),
                                                                            batch, contexts, shouldEvaluate,
                                                                            modelName);
                        List symbols = batchedUpdateCommand.getProjectedSymbols();
                        projectNode.setSelectSymbols(symbols);
                        projectNode.setElements(symbols);

                        batchNode.setElements(symbols);

                        projectNode.addChild(batchNode);
                        // Add a new RelationalPlan that represents the plan for this batch.
                        childPlans.add(new RelationalPlan(projectNode));
                        if (planContexts != null) {
                            planContexts.add(new VariableContext());
                        }
                        // Skip those commands that were added to this batch
                        commandIndex += batch.size() - 1;
                        commandWasBatched = true;
                    }
                }
            }
            if (!commandWasBatched) { // If the command wasn't batched, just add the plan for this command to the list of plans
                Command cmd = batchedUpdateCommand.getUpdateCommands().get(commandIndex);
                ProcessorPlan plan = cmd.getProcessorPlan();
                childPlans.add(plan);
                if (allContexts != null) {
                    planContexts.add(allContexts.get(commandIndex));
                }
            }
        }
        return new BatchedUpdatePlan(childPlans, batchedUpdateCommand.getUpdateCommands().size(), planContexts, batchedUpdateCommand.isSingleResult());
    }

    /**
     * Get the group being updated by the update command
     * @param command an INSERT, UPDATE, DELETE or SELECT INTO command
     * @return the group being updated
     * @since 4.2
     */
    public static GroupSymbol getUpdatedGroup(Command command) {
        int type = command.getType();
        if (type == Command.TYPE_INSERT) {
            return ((Insert)command).getGroup();
        } else if (type == Command.TYPE_UPDATE) {
            return ((Update)command).getGroup();
        } else if (type == Command.TYPE_DELETE) {
            return ((Delete)command).getGroup();
        }
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30244, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30244, command));
    }

    /**
     * Returns whether a command can be placed in a connector batch
     * @param command an update command
     * @param metadata
     * @return true if this command can be added to a batch; false otherwise
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @since 4.2
     */
    public static boolean isEligibleForBatching(Command command, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        ProcessorPlan plan = command.getProcessorPlan();
        AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(plan);
        // If the command updates a physical group, it's eligible
        return aNode != null && !metadata.isVirtualGroup(getUpdatedGroup(command).getMetadataID());
    }

    /**
     * Returns whether a command can be placed in a given batch
     * @param command an update command
     * @param batchModelID the model ID for the batch concerned
     * @param metadata
     * @param capFinder
     * @return true if this command can be place in a batch associated with the model ID; false otherwise
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @since 4.2
     */
    private static boolean canBeAddedToBatch(Command command, Object batchModelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {
        // If it's eligible ...
        if (isEligibleForBatching(command, metadata)) {
            Object modelID = metadata.getModelID(getUpdatedGroup(command).getMetadataID());

            return CapabilitiesUtil.isSameConnector(modelID, batchModelID, metadata, capFinder);
        }
        return false;
    }

}
