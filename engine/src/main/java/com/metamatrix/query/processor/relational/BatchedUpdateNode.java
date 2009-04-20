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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;


/** 
 * Node that batches commands sent to the DataManager.
 * @since 4.2
 */
public class BatchedUpdateNode extends RelationalNode {
    
    private static final List<Integer> ZERO_COUNT_TUPLE = Arrays.asList(Integer.valueOf(0));

    /** The commands in this batch. */
    private List<Command> updateCommands;
    private List<VariableContext> contexts;
    private List<Boolean> shouldEvaluate;
    
    /** The model name within the scope of which these commands are being executed. */
    private String modelName;
    /** The tuple source containing the update counts after the batch has been executed. */
    private TupleSource tupleSource;
    
    /** Set containing the indexes of commands that weren't executed. */
    private Set<Integer> unexecutedCommands;
    /** Flag indicating that at least one command was sent to the DataManager. */
    private boolean commandsWereExecuted = true;
    
    /**
     *  
     * @param nodeID
     * @param commands The Commands in this batch
     * @param modelName The name of the model. All the commands in this batch must update groups only within this model.
     * @since 4.2
     */
    public BatchedUpdateNode(int nodeID, List<Command> commands, List<VariableContext> contexts, List<Boolean> shouldEvaluate, String modelName) {
        super(nodeID);
        this.shouldEvaluate = shouldEvaluate;
        this.contexts = contexts;
        this.updateCommands = commands;
        this.modelName = modelName;
    }

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#open()
     * @since 4.2
     */
    public void open() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        super.open();
        unexecutedCommands = new HashSet<Integer>();
        List<Command> commandsToExecute = new ArrayList<Command>(updateCommands.size());
        // Find the commands to be executed
        for (int i = 0; i < updateCommands.size(); i++) {
            Command updateCommand = (Command)updateCommands.get(i).clone();
            CommandContext context = this.getContext();
            if (this.contexts != null) {
            	context = (CommandContext)context.clone();
            	context.setVariableContext(this.contexts.get(i));
            }
            boolean needProcessing = false;
            if(shouldEvaluate != null && shouldEvaluate.get(i)) {
                updateCommand = (Command) updateCommand.clone();
                needProcessing = AccessNode.prepareCommand(updateCommand, this, context);
            } else {
                needProcessing = RelationalNodeUtil.shouldExecute(updateCommand, true);
            }
            if (needProcessing) {
                commandsToExecute.add(updateCommand);
            } else {
                unexecutedCommands.add(Integer.valueOf(i));
            }
        }
        if (commandsToExecute.isEmpty()) {
            commandsWereExecuted = false;
        } else {
            BatchedUpdateCommand command = new BatchedUpdateCommand(commandsToExecute);
            tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), command, modelName, null, getID());
        }
    }
    
    /** 
     * @throws MetaMatrixProcessingException 
     * @see com.metamatrix.query.processor.relational.RelationalNode#nextBatchDirect()
     * @since 4.2
     */
    public TupleBatch nextBatchDirect() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        int numExpectedCounts = updateCommands.size();
        // If we don't have the tuple source yet, then we're blocked
        if (commandsWereExecuted) {
            // Otherwise, this tuple source has all the data we need
            List tuple;
            for ( int i = 0; i < numExpectedCounts; i++) {
                // If the command at this index was not executed
                if (unexecutedCommands.contains(Integer.valueOf(i))) {
                    addBatchRow(ZERO_COUNT_TUPLE);
                } else { // Otherwise, get the next count in the batch
                    tuple = tupleSource.nextTuple();
                    if (tuple != null) {
                        // Assumption: the number of returned tuples exactly equals the number of commands submitted
                        addBatchRow(Arrays.asList(new Object[] {tuple.get(0)}));
                    } else {
                        // Should never happen since the number of expected results is known
                        throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("BatchedUpdateNode.unexpected_end_of_batch")); //$NON-NLS-1$
                    }
                }
            }
        } else { // No commands were executed, so the counts are all 0.
            for (int i = 0; i < numExpectedCounts; i++) {
                addBatchRow(ZERO_COUNT_TUPLE);
            }
        }
        // This is the only tuple batch we need.
        terminateBatches();
        return pullBatch();
    }

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#close()
     * @since 4.2
     */
    public void close() throws MetaMatrixComponentException {
        if (!isClosed()) {
            super.close();

            if (tupleSource != null) {
            	tupleSource.closeSource();
            	tupleSource = null;
            }
        }
    }
    
    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#reset()
     * @since 4.2
     */
    public void reset() {
        super.reset();
        tupleSource = null;
        unexecutedCommands = null;
        commandsWereExecuted = true;
    }
    
    /** 
     * @see java.lang.Object#clone()
     * @since 4.2
     */
    public Object clone() {
        BatchedUpdateNode clonedNode = new BatchedUpdateNode(getID(), updateCommands, contexts, shouldEvaluate, modelName);
        super.copy(this, clonedNode);
        return clonedNode;
    }

}
