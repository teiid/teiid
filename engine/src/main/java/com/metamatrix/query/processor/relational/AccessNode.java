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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.util.CommandContext;

public class AccessNode extends RelationalNode {

    // Initialization state
    private Command command;
    private String modelName;
    private String connectorBindingId;
    private boolean shouldEvaluate = false;

    // Processing state
	private TupleSource tupleSource;
	private boolean isUpdate = false;
    private boolean returnedRows = false;
    
	public AccessNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        tupleSource = null;
		isUpdate = false;
        returnedRows = false;
    }

	public void setCommand(Command command) {
		this.command = command;
	}

    public Command getCommand() {
        return this.command;
    }

	public void setModelName(String name) {
		this.modelName = name;
	}

	public String getModelName() {
		return this.modelName;
	}

    public void setShouldEvaluateExpressions(boolean shouldEvaluate) {
        this.shouldEvaluate = shouldEvaluate;
    }

	public void open()
		throws MetaMatrixComponentException, MetaMatrixProcessingException {

        // Copy command and resolve references if necessary
        Command atomicCommand = command;
        boolean needProcessing = true;
        if(shouldEvaluate) {
            atomicCommand = (Command) command.clone();
            needProcessing = prepareNextCommand(atomicCommand);
        } else {
            needProcessing = RelationalNodeUtil.shouldExecute(atomicCommand, true);
        }
        // else command will not be changed, so no reason to all this work.
        // Removing this if block and always evaluating has a significant cost that will
        // show up in performance tests for many simple tests that do not require it.
        
        isUpdate = RelationalNodeUtil.isUpdate(atomicCommand);
        
		if(needProcessing) {
            this.tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), atomicCommand, modelName, connectorBindingId, getID());
		}
	}

    protected boolean prepareNextCommand(Command atomicCommand) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	return prepareCommand(atomicCommand, this, this.getContext());
    }

	static boolean prepareCommand(Command atomicCommand, RelationalNode node, CommandContext context)
			throws ExpressionEvaluationException, MetaMatrixComponentException,
			MetaMatrixProcessingException, CriteriaEvaluationException {
		// evaluate all references and any functions on constant values
        EvaluateExpressionVisitor.replaceExpressions(atomicCommand, true, node.getDataManager(), context);                            
        
        try {
            // Defect 16059 - Rewrite the command once the references have been replaced with values.
            QueryRewriter.rewrite(atomicCommand, null, null, context);
        } catch (QueryValidatorException e) {
            throw new MetaMatrixProcessingException(e, QueryExecPlugin.Util.getString("AccessNode.rewrite_failed", atomicCommand)); //$NON-NLS-1$
        }
        
        return RelationalNodeUtil.shouldExecute(atomicCommand, true);
	}
    
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        while (tupleSource != null || hasNextCommand()) {
        	//drain the tuple source
        	while (tupleSource != null) {
                List<?> tuple = tupleSource.nextTuple();
    
                if(tuple == null) {
                    closeSources();
                    break;
                } 
                
                returnedRows = true;
                
                addBatchRow(tuple);
                
                if (isBatchFull()) {
                	return pullBatch();
                }
        	}
        	
        	//execute another command
            while (hasNextCommand()) {
            	if (processCommandsIndividually() && hasPendingRows()) {
            		return pullBatch();
            	}
                Command atomicCommand = (Command)command.clone();
                if (prepareNextCommand(atomicCommand)) {
                    tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), atomicCommand, modelName, null, getID());
                    break;
                }
            }            
        }
        
        if(isUpdate && !returnedRows) {
			List<Integer> tuple = new ArrayList<Integer>(1);
			tuple.add(Integer.valueOf(0));
            // Add tuple to current batch
            addBatchRow(tuple);
        }
        terminateBatches();
        return pullBatch();
	}
	
	protected boolean processCommandsIndividually() {
		return false;
	}
    
    protected boolean hasNextCommand() {
        return false;
    }
    
	public void close() throws MetaMatrixComponentException {
	    if (!isClosed()) {
            super.close();
            
            closeSources();            
        }
	}

    private void closeSources() throws MetaMatrixComponentException {
        if(this.tupleSource != null) {
    		this.tupleSource.closeSource();
            tupleSource = null;
        }
	}

	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(command);
	}

	public Object clone(){
		AccessNode clonedNode = new AccessNode(super.getID());
		this.copy(this, clonedNode);
		return clonedNode;
	}

	protected void copy(AccessNode source, AccessNode target){
		super.copy(source, target);
		target.modelName = source.modelName;
		target.connectorBindingId = source.connectorBindingId;
		target.shouldEvaluate = source.shouldEvaluate;
		target.command = source.command;
	}

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Access"); //$NON-NLS-1$
        props.put(PROP_SQL, this.command.toString());
        props.put(PROP_MODEL_NAME, this.modelName);
        return props;
    }

	public String getConnectorBindingId() {
		return connectorBindingId;
	}

	public void setConnectorBindingId(String connectorBindingId) {
		this.connectorBindingId = connectorBindingId;
	}
    
}