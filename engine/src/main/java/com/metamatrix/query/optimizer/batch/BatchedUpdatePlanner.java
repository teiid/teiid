/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.optimizer.batch;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerID;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.RelationalPlanner;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.batch.BatchedUpdatePlan;
import com.metamatrix.query.processor.relational.BatchedUpdateNode;
import com.metamatrix.query.processor.relational.ProjectNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.sql.visitor.EvaluatableVisitor;
import com.metamatrix.query.util.CommandContext;


/** 
 * Planner for BatchedUpdateCommands
 * @since 4.2
 */
public class BatchedUpdatePlanner implements CommandPlanner {

    /** 
     * @see com.metamatrix.query.optimizer.CommandPlanner#generateCanonical(com.metamatrix.query.optimizer.CommandTreeNode, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.analysis.AnalysisRecord, CommandContext)
     * @since 4.2
     */
    public void generateCanonical(CommandTreeNode rootNode,
                                  QueryMetadataInterface metadata,
                                  AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        // do nothing. the planner framework takes care of generating the canonical plan for each of the child commands
    }

    /** 
     * Optimizes batched updates by batching all contiguous commands that relate to the same physical model.
     * For example, for the following batch of commands:
     * <br/>
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
     * <br/> this implementation will batch as follows: (1,2), (5, 6, 7), (8 thru 12).
     * The remaining commands/plans will be executed individually.
     * @see com.metamatrix.query.optimizer.CommandPlanner#optimize(com.metamatrix.query.optimizer.CommandTreeNode, com.metamatrix.core.id.IDGenerator, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.analysis.AnalysisRecord, CommandContext)
     * @since 4.2
     */
    public ProcessorPlan optimize(CommandTreeNode node,
                                  IDGenerator idGenerator,
                                  QueryMetadataInterface metadata,
                                  CapabilitiesFinder capFinder,
                                  AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        List children = node.getChildren();
        List childPlans = new ArrayList(children.size());
        BatchedUpdateCommand batchedUpdateCommand = (BatchedUpdateCommand)node.getCommand();
        List updateCommands = batchedUpdateCommand.getUpdateCommands();
        int numCommands = updateCommands.size();
        List<VariableContext> allContexts = (List<VariableContext>)node.getProperty(RelationalPlanner.VARIABLE_CONTEXTS);
        for (int commandIndex = 0; commandIndex < numCommands; commandIndex++) {
            // Potentially the first command of a batch
            Command updateCommand = (Command)updateCommands.get(commandIndex);
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
                        Command batchingCandidate = (Command)updateCommands.get(batchIndex);
                        // If this command updates the same model, and is eligible for batching, add it to the batch
                        if (canBeAddedToBatch(batchingCandidate, batchModelID, metadata)) {
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
                        ProjectNode projectNode = new ProjectNode(((IntegerID)idGenerator.create()).getValue());
                        // Create a BatchedUpdateNode that creates a batched request for the connector
                        BatchedUpdateNode batchNode = new BatchedUpdateNode(((IntegerID)idGenerator.create()).getValue(),
                                                                            batch, contexts, shouldEvaluate,
                                                                            modelName);
                        List symbols = batchedUpdateCommand.getProjectedSymbols();
                        projectNode.setSelectSymbols(symbols);
                        projectNode.setElements(symbols);
                        
                        batchNode.setElements(symbols);
                        
                        projectNode.addChild(batchNode);
                        // Add a new RelationalPlan that represents the plan for this batch.
                        childPlans.add(new RelationalPlan(projectNode));
                        // Skip those commands that were added to this batch
                        commandIndex += batch.size() - 1;
                        commandWasBatched = true;
                    }
                }
            }
            if (!commandWasBatched) { // If the command wasn't batched, just add the plan for this command to the list of plans
                childPlans.add(((CommandTreeNode)children.get(commandIndex)).getProcessorPlan()); // Assumes the command index is the same as the plan node index
            }
        }
        return new BatchedUpdatePlan(childPlans, children.size());
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
        } else if (type == Command.TYPE_QUERY) { 
            return ((Query)command).getInto().getGroup();
        }
        throw new MetaMatrixRuntimeException(QueryExecPlugin.Util.getString("BatchedUpdatePlanner.unrecognized_command", command)); //$NON-NLS-1$
    }
    
    /**
     * Returns whether a command can be placed in a connector batch 
     * @param command an update command
     * @param metadata
     * @return true if this command can be added to a batch; false otherwise
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     * @since 4.2
     */
    public static boolean isEligibleForBatching(Command command, QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException {
        // If it's a SELECT INTO, it shouldn't be part of a connector batch.
        if (command.getType() == Command.TYPE_QUERY) {
            return false;
        }
        // If the command updates a physical group, it's eligible
        return !metadata.isVirtualGroup(getUpdatedGroup(command).getMetadataID());
    }
    
    /**
     * Returns whether a command can be placed in a given batch 
     * @param command an update command
     * @param batchModelID the model ID for the batch concerned
     * @param metadata
     * @return true if this command can be place in a batch associated with the model ID; false otherwise
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     * @since 4.2
     */
    private static boolean canBeAddedToBatch(Command command, Object batchModelID, QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException {
        // If it's eligible ...
        if (isEligibleForBatching(command, metadata)) {
            Object modelID = metadata.getModelID(getUpdatedGroup(command).getMetadataID());
            // ... and it updates a group in the same model ...
            if (modelID.equals(batchModelID)) {
                // ... then it can be added to the batch.
                return true;
            }
        }
        return false;
    }

}
