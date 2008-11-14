/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;

public class AccessNode extends RelationalNode {

    // Initialization state
    private Command command;
    private String modelName;
    private boolean shouldEvaluate = false;

    // Processing state
	private TupleSource tupleSource;
	private boolean needProcessing = true;
	private boolean isUpdate = false;
    private boolean returnedRows = false;
    
	public AccessNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        tupleSource = null;
		needProcessing = true;
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
        needProcessing = true;
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
            this.tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), atomicCommand, modelName, getID());
		}
	}

    protected boolean prepareNextCommand(Command atomicCommand) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	// evaluate all references and any functions on constant values
        EvaluateExpressionVisitor.replaceExpressions(atomicCommand, true, getDataManager(), getContext());                            
        
        try {
            // Defect 16059 - Rewrite the command once the references have been replaced with values.
            QueryRewriter.rewrite(atomicCommand, null, null, getContext());
        } catch (QueryValidatorException e) {
            throw new MetaMatrixProcessingException(e, QueryExecPlugin.Util.getString("AccessNode.rewrite_failed", atomicCommand)); //$NON-NLS-1$
        }
        
        return RelationalNodeUtil.shouldExecute(atomicCommand, true);
    }
    
	public TupleBatch nextBatchDirect()
		throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        boolean batchDone = false;

        while (!batchDone) {
            while (!needProcessing && hasNextCommand()) {
                Command atomicCommand = (Command)command.clone();
                needProcessing = prepareNextCommand(atomicCommand);
                if (needProcessing) {
                	closeSources();
                    tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), atomicCommand, modelName, getID());
                }
            }
            
    		if(!needProcessing && !hasNextCommand()) {
                if(isUpdate && !returnedRows) {
        			List tuple = new ArrayList(1);
        			tuple.add(new Integer(0));
                    // Add tuple to current batch
                    addBatchRow(tuple);
                }
                terminateBatches();
                return pullBatch();
    		}
    
            //needProcessing must be true after this point
            
            // Pull a batch worth of tuples
            while(!batchDone) {
        		// Read a tuple
                List tuple = tupleSource.nextTuple();
    
                // Check for termination tuple
                if(tuple == null) {
                    closeSources();
                    needProcessing = false;
                    break;
                } 
                
                returnedRows = true;
                
                // Add tuple to current batch
                addBatchRow(tuple);
                // Check for full batch
                batchDone = isBatchFull();
            }
        }

        return pullBatch();
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
		target.shouldEvaluate = source.shouldEvaluate;
		target.command = (Command)source.command.clone();
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
    
}