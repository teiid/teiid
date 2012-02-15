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
package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


public class ProjectIntoNode extends RelationalNode {

	public enum Mode {
		BATCH, BULK, ITERATOR, SINGLE
	}
	
    private static int REQUEST_CREATION = 1;
    private static int RESPONSE_PROCESSING = 2;
    
    // Initialization state
    private GroupSymbol intoGroup;
    private List intoElements;
    private String modelName;
    private Mode mode;
    
    // Processing state
    private int batchRow = 1;
    private int insertCount = 0;
    private int phase = REQUEST_CREATION;    
    private int requestsRegistered = 0;
    private int tupleSourcesProcessed = 0;
    private boolean sourceDone;
    
    private TupleBuffer buffer;
    private TupleBatch currentBatch;
        	
    private TupleSource tupleSource;

    protected ProjectIntoNode() {
        super();
    }
    
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
        this.sourceDone=false;
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
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        
        while(phase == REQUEST_CREATION) {
            
            checkExitConditions();
            
            /* If we don't have a batch to work, get the next
             */
            if (currentBatch == null) {
            	if (sourceDone) {
	                phase = RESPONSE_PROCESSING;
	                break;
            	}
                currentBatch = getChildren()[0].nextBatch(); // can throw BlockedException
                sourceDone = currentBatch.getTerminationFlag();
                this.batchRow = currentBatch.getBeginRow();
                
                //normally we would want to skip a 0 sized batch, but it typically represents the terminal batch
                //and for implicit temp tables we need to issue an empty insert
                if(currentBatch.getRowCount() == 0
                		&& (!currentBatch.getTerminationFlag() || mode != Mode.ITERATOR)) {
            		currentBatch = null;
            		continue;
                }
            } 
            
            int batchSize = currentBatch.getRowCount();
            int requests = 1;
            switch (mode) {
            case ITERATOR:
            	if (buffer == null) {
            		buffer = getBufferManager().createTupleBuffer(intoElements, getConnectionID(), TupleSourceType.PROCESSOR);
            	}
            	buffer.addTupleBatch(currentBatch, true);
            	if (currentBatch.getTerminationFlag() && (buffer.getRowCount() != 0 || intoGroup.isImplicitTempGroupSymbol())) {
            		Insert insert = new Insert(intoGroup, intoElements, null);
            		buffer.close();
            		insert.setTupleSource(buffer.createIndexedTupleSource(true));
                    // Register insert command against source 
                    registerRequest(insert);
            	} else {
            		requests = 0;
            	}
            	break;
            case BULK:
            	//convert to multivalued parameter
                List<Constant> parameters = new ArrayList<Constant>(intoElements.size());
                for (int i = 0; i < intoElements.size(); i++) {
					Constant value = new Constant(null, ((ElementSymbol)intoElements.get(i)).getType());
					value.setMultiValued(new ArrayList<Object>(currentBatch.getTuples().size()));
                	parameters.add(value);
				}
                for (List row : currentBatch.getTuples()) {
                	for (int i = 0; i < row.size(); i++) {
                		((List<Object>)parameters.get(i).getValue()).add(row.get(i));
                	}
				}
                // Create a bulk insert command batching all rows in the current batch.
                Insert insert = new Insert(intoGroup, intoElements, parameters);
                // Register insert command against source 
                registerRequest(insert);
                break;
            case BATCH:
                // Register batched update command against source
                int endRow = currentBatch.getEndRow();
                List rows = new ArrayList(endRow-batchRow);
                for(int rowNum = batchRow; rowNum <= endRow; rowNum++) {

                    insert = new Insert( intoGroup, 
                                                 intoElements, 
                                                 convertValuesToConstants(currentBatch.getTuple(rowNum), intoElements));
                    rows.add( insert );
                }
                registerRequest(new BatchedUpdateCommand( rows ));
                break;
            case SINGLE:
                batchSize = 1;
                // Register insert command against source 
                // Defect 16036 - submit a new INSERT command to the DataManager.
                registerRequest(new Insert(intoGroup, intoElements, convertValuesToConstants(currentBatch.getTuple(batchRow), intoElements)));
            }
            
            this.batchRow += batchSize;
            if (batchRow > currentBatch.getEndRow()) {
            	currentBatch = null;
            }
            this.requestsRegistered+=requests;
        }
        
        checkExitConditions();
        
        // End this node's work
        List outputRow = new ArrayList(1);
        outputRow.add(new Integer(this.insertCount));
        addBatchRow(outputRow);
        terminateBatches();
        return pullBatch();                                                           
    }

    private void checkExitConditions()  throws TeiidComponentException, BlockedException, TeiidProcessingException {
    	if (tupleSource != null) {
	    	Integer count = (Integer)tupleSource.nextTuple().get(0);
	        insertCount += count.intValue();
	        closeRequest();
	        // Mark as processed
	        tupleSourcesProcessed++; // This should set tupleSourcesProcessed to be the same as requestsRegistered
    	}
        // RESPONSE_PROCESSING: process tuple sources
        if (tupleSourcesProcessed < requestsRegistered) {
        	throw BlockedException.block(getContext().getRequestId(), "Blocking on insert update count"); //$NON-NLS-1$
        }
                
    }

    private void registerRequest(Command command) throws TeiidComponentException, TeiidProcessingException {
    	tupleSource = getDataManager().registerRequest(getContext(), command, this.modelName, null, getID(), -1);        
    }
    
    private void closeRequest() {
    	if (this.buffer != null) {
    		this.buffer.remove();
    		this.buffer = null;
    	}
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
        ProjectIntoNode clonedNode = new ProjectIntoNode();
        super.copy(this, clonedNode);

        clonedNode.intoGroup = intoGroup;
        clonedNode.intoElements = intoElements;
        clonedNode.modelName = this.modelName;
        clonedNode.mode = this.mode;
        
        return clonedNode;
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_INTO_GROUP, intoGroup.toString());
        List<String> selectCols = new ArrayList<String>(intoElements.size());
        for(int i=0; i<this.intoElements.size(); i++) {
            selectCols.add(this.intoElements.get(i).toString());
        }
        props.addProperty(PROP_SELECT_COLS, selectCols);

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
    
    public Mode getMode() {
		return mode;
	}
            
    public void setMode(Mode mode) {
		this.mode = mode;
	}

    public boolean isTempGroupInsert() {
		return intoGroup.isTempGroupSymbol();
	}

    public void closeDirect() {
        closeRequest();
	}
    
    public String getModelName() {
		return modelName;
	}
    
}
