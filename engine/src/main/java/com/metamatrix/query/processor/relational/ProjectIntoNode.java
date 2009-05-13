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

/*
 */
package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;

public class ProjectIntoNode extends RelationalNode {

    private static int REQUEST_CREATION = 1;
    private static int RESPONSE_PROCESSING = 2;
    
    // Initialization state
    private GroupSymbol intoGroup;
    private List intoElements;
    private String modelName;
    private boolean doBatching = false;
    private boolean doBulkInsert = false;
    
    // Processing state
    private int batchRow = 1;
    private int insertCount = 0;
    private int phase = REQUEST_CREATION;    
    private int requestsRegistered = 0;
    private int tupleSourcesProcessed = 0;
    
    private TupleBatch currentBatch;
        	
    private TupleSource tupleSource;
    
    public ProjectIntoNode(int nodeID) {
        super(nodeID);
    }
    
    public void reset() {
        super.reset();     
        this.phase = REQUEST_CREATION;
        this.batchRow = 1;
        this.insertCount = 0; 
        this.tupleSourcesProcessed = 0;
        this.requestsRegistered = 0;
        this.currentBatch=null;
    }

    public void setIntoGroup(GroupSymbol group) { 
        this.intoGroup = group;
    }

    public void setIntoElements(List intoElements) {
        this.intoElements = intoElements;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    /**
     * Get batch from child node
     * Walk through each row of child batch
     *    Bind values to insertCommand
     *    Execute insertCommand
     *    Update insertCount
     * When no more data is available, output batch with single row containing insertCount 
     */     
    public TupleBatch nextBatchDirect() 
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        while(phase == REQUEST_CREATION) {
            
            checkExitConditions();
            
            /* If we don't have a batch to work with yet, or
             * if this is not the last batch and we've reached the end of this one,
             * then get the next batch.
             */
            
            if (currentBatch == null || (!currentBatch.getTerminationFlag() && this.batchRow > currentBatch.getEndRow())) {           
                currentBatch = getChildren()[0].nextBatch(); // can throw BlockedException
                this.batchRow = currentBatch.getBeginRow();
                
                if(currentBatch.getRowCount() == 0 && !this.intoGroup.isImplicitTempGroupSymbol()) {
                    continue;
                }
            } else if (currentBatch.getTerminationFlag() && this.batchRow > currentBatch.getEndRow()) {
                phase = RESPONSE_PROCESSING;
                break;
            }
            
            int batchSize = currentBatch.getRowCount();
            
            if (doBulkInsert) {
            	//convert to multivalued parameter
                List<Constant> parameters = new ArrayList<Constant>(intoElements.size());
                for (int i = 0; i < intoElements.size(); i++) {
					Constant value = new Constant(null, ((ElementSymbol)intoElements.get(i)).getType());
					value.setMultiValued(new ArrayList<Object>(currentBatch.getAllTuples().length));
                	parameters.add(value);
				}
                for (List row : currentBatch.getAllTuples()) {
                	for (int i = 0; i < row.size(); i++) {
                		((List<Object>)parameters.get(i).getValue()).add(row.get(i));
                	}
				}
                // Create a bulk insert command batching all rows in the current batch.
                Insert insert = new Insert(intoGroup, intoElements, parameters);
                // Register insert command against source 
                registerRequest(insert);
            } else if (doBatching) { 
                // Register batched update command against source
                int endRow = currentBatch.getEndRow();
                List rows = new ArrayList(endRow-batchRow);
                for(int rowNum = batchRow; rowNum <= endRow; rowNum++) {

                    Insert insert = new Insert( intoGroup, 
                                                 intoElements, 
                                                 convertValuesToConstants(currentBatch.getTuple(rowNum), intoElements));
                    rows.add( insert );
                }
                registerRequest(new BatchedUpdateCommand( rows ));
            } else {
                batchSize = 1;
                // Register insert command against source 
                // Defect 16036 - submit a new INSERT command to the DataManager.
                registerRequest(new Insert(intoGroup, intoElements, convertValuesToConstants(currentBatch.getTuple(batchRow), intoElements)));
            }
            this.batchRow += batchSize;
            this.requestsRegistered++;
        }
        
        checkExitConditions();
        
        // End this node's work
        List outputRow = new ArrayList(1);
        outputRow.add(new Integer(this.insertCount));
        addBatchRow(outputRow);
        terminateBatches();
        return pullBatch();                                                           
    }

    private void checkExitConditions()  throws MetaMatrixComponentException, BlockedException, MetaMatrixProcessingException {
    	if (tupleSource != null) {
	    	Integer count = (Integer)tupleSource.nextTuple().get(0);
	        insertCount += count.intValue();
	        closeRequest();
	        // Mark as processed
	        tupleSourcesProcessed++; // This should set tupleSourcesProcessed to be the same as requestsRegistered
    	}
        // RESPONSE_PROCESSING: process tuple sources
        if (tupleSourcesProcessed < requestsRegistered) {
            throw BlockedException.INSTANCE;
        }
                
    }

    private void registerRequest(Command command) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	tupleSource = getDataManager().registerRequest(this.getContext().getProcessorID(), command, this.modelName, null, getID());        
    }
    
    private void closeRequest() throws MetaMatrixComponentException {

        if (this.tupleSource != null) {
            tupleSource.closeSource();
            this.tupleSource = null;
        }
    }
    
    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(intoGroup);
    }
    
    public Object clone(){
        ProjectIntoNode clonedNode = new ProjectIntoNode(super.getID());
        super.copy(this, clonedNode);

        clonedNode.intoGroup = intoGroup;
        clonedNode.intoElements = intoElements;
        clonedNode.modelName = this.modelName;
        clonedNode.doBatching = this.doBatching;
        clonedNode.doBulkInsert = this.doBulkInsert;
        
        return clonedNode;
    }

    public Map getDescriptionProperties() {
        // Default implementation - should be overridden
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Project Into"); //$NON-NLS-1$
        props.put(PROP_INTO_GROUP, intoGroup.toString());
        List selectCols = new ArrayList(intoElements.size());
        for(int i=0; i<this.intoElements.size(); i++) {
            selectCols.add(this.intoElements.get(i).toString());
        }
        props.put(PROP_SELECT_COLS, selectCols);

        return props;
    }
    
    private List convertValuesToConstants(List values, List elements) {
        ArrayList constants = new ArrayList(values.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol es = (ElementSymbol)elements.get(i);
            Class type = es.getType();
            constants.add(new Constant(values.get(i),type));
        }
        return constants;
    }    
            
    public void setDoBatching(boolean doBatching) {
        this.doBatching = doBatching;
    }
    
    public void setDoBulkInsert(boolean doBulkInsert) {
        this.doBulkInsert = doBulkInsert;
    }

    public boolean isTempGroupInsert() {
		return intoGroup.isTempGroupSymbol();
	}

    public void close() throws MetaMatrixComponentException {
        closeRequest();
        super.close();
	}
}
