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

package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;


public class AccessNode extends SubqueryAwareRelationalNode {

    // Initialization state
    private Command command;
    private String modelName;
    private String connectorBindingId;
    private boolean shouldEvaluate = false;

    // Processing state
	private TupleSource tupleSource;
	private boolean isUpdate = false;
    private boolean returnedRows = false;
    private Command nextCommand;
    
    protected AccessNode() {
		super();
	}
    
	public AccessNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        tupleSource = null;
		isUpdate = false;
        returnedRows = false;
        nextCommand = null;
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
		throws TeiidComponentException, TeiidProcessingException {

        // Copy command and resolve references if necessary
        Command atomicCommand = command;
        boolean needProcessing = true;
        if(shouldEvaluate) {
            atomicCommand = nextCommand();
            needProcessing = prepareNextCommand(atomicCommand);
            nextCommand = null;
        } else {
            needProcessing = RelationalNodeUtil.shouldExecute(atomicCommand, true);
        }
        // else command will not be changed, so no reason to all this work.
        // Removing this if block and always evaluating has a significant cost that will
        // show up in performance tests for many simple tests that do not require it.
        
        isUpdate = RelationalNodeUtil.isUpdate(atomicCommand);
        
		if(needProcessing) {
			registerRequest(atomicCommand);
		}
	}

	private Command nextCommand() {
		//it's important to save the next command
		//to ensure that the subquery ids remain stable
		if (nextCommand == null) {
			nextCommand = (Command) command.clone(); 
		}
		return nextCommand; 
	}

    protected boolean prepareNextCommand(Command atomicCommand) throws TeiidComponentException, TeiidProcessingException {
    	return prepareCommand(atomicCommand, getEvaluator(Collections.emptyMap()), this.getContext(), this.getContext().getMetadata());
    }

	static boolean prepareCommand(Command atomicCommand, Evaluator eval, CommandContext context, QueryMetadataInterface metadata)
			throws ExpressionEvaluationException, TeiidComponentException,
			TeiidProcessingException {
        try {
            // Defect 16059 - Rewrite the command once the references have been replaced with values.
            QueryRewriter.evaluateAndRewrite(atomicCommand, eval, context, metadata);
        } catch (QueryValidatorException e) {
            throw new TeiidProcessingException(e, QueryPlugin.Util.getString("AccessNode.rewrite_failed", atomicCommand)); //$NON-NLS-1$
        }
        
        return RelationalNodeUtil.shouldExecute(atomicCommand, true);
	}
    
	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {
        
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
                Command atomicCommand = nextCommand();
                if (prepareNextCommand(atomicCommand)) {
                	nextCommand = null;
                    registerRequest(atomicCommand);
                    break;
                }
                nextCommand = null;
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

	private void registerRequest(Command atomicCommand)
			throws TeiidComponentException, TeiidProcessingException {
		int limit = -1;
		if (getParent() instanceof LimitNode) {
			LimitNode parent = (LimitNode)getParent();
			limit = parent.getLimit() + parent.getOffset();
			if (limit < parent.getLimit()) {
				limit = -1; //guard against overflow
			}
		}
		tupleSource = getDataManager().registerRequest(getContext(), atomicCommand, modelName, connectorBindingId, getID(), limit);
	}
	
	protected boolean processCommandsIndividually() {
		return false;
	}
    
    protected boolean hasNextCommand() {
        return false;
    }
    
	public void closeDirect() {
		super.closeDirect();
        closeSources();            
	}

    private void closeSources() {
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
		AccessNode clonedNode = new AccessNode();
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

    public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_SQL, this.command.toString());
        props.addProperty(PROP_MODEL_NAME, this.modelName);
        return props;
    }

	public String getConnectorBindingId() {
		return connectorBindingId;
	}

	public void setConnectorBindingId(String connectorBindingId) {
		this.connectorBindingId = connectorBindingId;
	}
    
}