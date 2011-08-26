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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.BatchCollector.BatchProducer;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;


public abstract class RelationalNode implements Cloneable, BatchProducer {
	
	static class NodeData {
		int nodeID;
		List elements;
		Number estimateNodeCardinality;
		Number setSizeEstimate;
		Number depAccessEstimate;
		Number estimateDepJoinCost;
		Number estimateJoinCost;
	}

	static class ProcessingState {
		CommandContext context;
		BufferManager bufferManager;
		ProcessorDataManager dataMgr;
		int batchSize;
		RelationalNodeStatistics nodeStatistics;
		int beginBatch = 1;
		List batchRows;
		boolean lastBatch;
		boolean closed;
		
		void reset() {
			this.beginBatch = 1;
			this.batchRows = null;
			this.lastBatch = false;
			this.closed = false;
		}
	}

    private ProcessingState processingState;
	private NodeData data;
	/** The parent of this node, null if root. */
    private RelationalNode parent;

	/** Child nodes, usually just 1 or 2 */
	private RelationalNode[] children = new RelationalNode[2];
	
	protected RelationalNode() {
		
	}
	
	public RelationalNode(int nodeID) {
		this.data = new NodeData();
		this.data.nodeID = nodeID;
	}
	
	public boolean isLastBatch() {
		return getProcessingState().lastBatch;
	}
	
	public void setContext(CommandContext context) {
		this.getProcessingState().context = context;
	}

    public void initialize(CommandContext context, BufferManager bufferManager, ProcessorDataManager dataMgr) {
        this.getProcessingState().context = context;
        this.getProcessingState().bufferManager = bufferManager;
        this.getProcessingState().dataMgr = dataMgr;
        
        if(context.getCollectNodeStatistics()) {
            this.getProcessingState().nodeStatistics = new RelationalNodeStatistics();
        }

        this.getProcessingState().batchSize = bufferManager.getProcessorBatchSize();
    }

    public CommandContext getContext() {
        return this.getProcessingState().context;
    }

	public int getID() {
		return this.data.nodeID;
	}
    
    public void setID(int nodeID) {
    	NodeData newData = new NodeData();
    	newData.nodeID = nodeID;
    	newData.elements = this.data.elements;
        this.data = newData;
    }

    protected BufferManager getBufferManager() {
        return this.getProcessingState().bufferManager;
    }

    protected ProcessorDataManager getDataManager() {
        return this.getProcessingState().dataMgr;
    }

    protected String getConnectionID() {
        return this.getProcessingState().context.getConnectionID();
    }

    protected int getBatchSize() {
        return this.getProcessingState().batchSize;
    }

    public void reset() {
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].reset();
            } else {
                break;
            }
        }
        if (this.getProcessingState() != null) {
        	this.getProcessingState().reset();
        }
    }

	public void setElements(List elements) {
		this.data.elements = elements;
	}
	
	@Override
	public List getOutputElements() {
		return getElements();
	}

	public List getElements() {
		return this.data.elements;
	}
    	
    public RelationalNode getParent() {
		return parent;
	}

	public void setParent(RelationalNode parent) {
		this.parent = parent;
	}

	public RelationalNode[] getChildren() {
		return this.children;
	}

    public void addChild(RelationalNode child) {
        // Set parent of child to match
        child.setParent(this);

        for(int i=0; i<children.length; i++) {
            if(children[i] == null) {
                children[i] = child;
                return;
            }
        }
        
        // No room to add - double size of the array and copy
        RelationalNode[] newChildren = new RelationalNode[children.length * 2];
        System.arraycopy(this.children, 0, newChildren, 0, this.children.length);
        newChildren[this.children.length] = child;
        this.children = newChildren;        
    }

    protected void addBatchRow(List row) {
        if(this.getProcessingState().batchRows == null) {
            this.getProcessingState().batchRows = new ArrayList(this.getProcessingState().batchSize / 4);
        }
        this.getProcessingState().batchRows.add(row);
    }

    protected void terminateBatches() {
        this.getProcessingState().lastBatch = true;
    }

    protected boolean isBatchFull() {
        return (this.getProcessingState().batchRows != null) && (this.getProcessingState().batchRows.size() >= this.getProcessingState().batchSize);
    }
    
    protected boolean hasPendingRows() {
    	return this.getProcessingState().batchRows != null;
    }

    protected TupleBatch pullBatch() {
        TupleBatch batch = null;
        if(this.getProcessingState().batchRows != null) {
            batch = new TupleBatch(this.getProcessingState().beginBatch, this.getProcessingState().batchRows);
            getProcessingState().beginBatch += this.getProcessingState().batchRows.size();
        } else {
            batch = new TupleBatch(this.getProcessingState().beginBatch, Collections.EMPTY_LIST);
        }

        batch.setTerminationFlag(this.getProcessingState().lastBatch);

        // Reset batch state
        this.getProcessingState().batchRows = null;
        this.getProcessingState().lastBatch = false;

        // Return batch
        return batch;
    }

	public void open()
		throws TeiidComponentException, TeiidProcessingException {

        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].open();
            } else {
                break;
            }
        }
    }
    
    /**
     * Wrapper for nextBatchDirect that does performance timing - callers
     * should always call this rather than nextBatchDirect(). 
     * @return
     * @throws BlockedException
     * @throws TeiidComponentException
     * @since 4.2
     */
    public final TupleBatch nextBatch() throws BlockedException,  TeiidComponentException, TeiidProcessingException {
        boolean recordStats = this.getProcessingState().context != null && this.getProcessingState().context.getCollectNodeStatistics();
        
        try {
            while (true) {
            	//start timer for this batch
                if(recordStats && this.getProcessingState().context.getCollectNodeStatistics()) {
                    this.getProcessingState().nodeStatistics.startBatchTimer();
                }
                TupleBatch batch = nextBatchDirect();
                if (recordStats) {
                    if(this.getProcessingState().context.getCollectNodeStatistics()) {
                        // stop timer for this batch (normal)
                        this.getProcessingState().nodeStatistics.stopBatchTimer();
                        this.getProcessingState().nodeStatistics.collectCumulativeNodeStats(batch, RelationalNodeStatistics.BATCHCOMPLETE_STOP);
                        if (batch.getTerminationFlag()) {
                            this.getProcessingState().nodeStatistics.collectNodeStats(this.getChildren(), this.getClassName());
                            //this.nodeStatistics.dumpProperties(this.getClassName());
                        }
                    }
                    this.recordBatch(batch);
                }
                //24663: only return non-zero batches. 
                //there have been several instances in the code that have not correctly accounted for non-terminal zero length batches
                //this processing style however against the spirit of batch processing (but was already utilized by Sort and Grouping nodes)
                if (batch.getRowCount() != 0 || batch.getTerminationFlag()) {
                    if (batch.getTerminationFlag()) {
                        close();
                    }
                    return batch;
                }
            }
        } catch (BlockedException e) {
            if(recordStats && this.getProcessingState().context.getCollectNodeStatistics()) {
                // stop timer for this batch (BlockedException)
                this.getProcessingState().nodeStatistics.stopBatchTimer();
                this.getProcessingState().nodeStatistics.collectCumulativeNodeStats(null, RelationalNodeStatistics.BLOCKEDEXCEPTION_STOP);
            }
            throw e;
        } catch (QueryProcessor.ExpiredTimeSliceException e) {
        	if(recordStats && this.getProcessingState().context.getCollectNodeStatistics()) {
                this.getProcessingState().nodeStatistics.stopBatchTimer();
            }
            throw e;
        } catch (TeiidComponentException e) {
            // stop timer for this batch (MetaMatrixComponentException)
            if(recordStats &&  this.getProcessingState().context.getCollectNodeStatistics()) {
                this.getProcessingState().nodeStatistics.stopBatchTimer();
            }
            throw e;
        }
    }

    /**
     * Template method for subclasses to implement. 
     * @return
     * @throws BlockedException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException if exception related to user input occured
     * @since 4.2
     */
	protected abstract TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException;

	public final void close()
		throws TeiidComponentException {

        if (!this.getProcessingState().closed) {
        	closeDirect();
            for(int i=0; i<children.length; i++) {
                if(children[i] != null) {
                    children[i].close();
                } else {
                    break;
                }
            }
            this.getProcessingState().closed = true;
        }
    }
	
	public void closeDirect() {
		
	}

    /**
     * Check if the node has been already closed
     * @return
     */
    public boolean isClosed() {
        return this.getProcessingState().closed;
    }
    
	/**
	 * Helper method for all the node that will filter the elements needed for the next node.
	 */
	public static int[] getProjectionIndexes(Map<? extends Expression, Integer> tupleElements, List<? extends Expression> projectElements) {
		int[] result = new int[projectElements.size()];

		int i = 0;
		for (Expression symbol : projectElements) {
			Integer index = tupleElements.get(symbol);
			Assertion.isNotNull(index);
			result[i++] = index;
		}

		return result;
	}
	
	public static <T> List<T> projectTuple(int[] indexes, List<T> tupleValues) {
	
		List<T> projectedTuple = new ArrayList<T>(indexes.length);
	
		for (int index : indexes) {
			projectedTuple.add(tupleValues.get(index));
		}
	
		return projectedTuple;
	}

    /**
     * Useful function to build an element lookup map from an element list.
     * @param elements List of elements
     * @return Map of element to Integer, which is the index
     */
    public static Map createLookupMap(List elements) {
        Map lookupMap = new HashMap();
        for(int i=0; i<elements.size(); i++) {
            Object element = elements.get(i);
            lookupMap.put(element, new Integer(i));
            if (element instanceof AliasSymbol) {
                lookupMap.put(((AliasSymbol)element).getSymbol(), new Integer(i));
            }
        }
        return lookupMap;
    }

    /**
     * For debugging purposes - all intermediate batches should go through this
     * method so we can easily trace data flow through the plan.
     * @param batch Batch being sent
     */
    private void recordBatch(TupleBatch batch) {
        if (!LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_DQP, MessageLevel.TRACE)) {
        	return;
        }
    	// Print summary
        StringBuffer str = new StringBuffer();
        str.append(getClassName());
        str.append("("); //$NON-NLS-1$
        str.append(getID());
        str.append(") sending "); //$NON-NLS-1$
        str.append(batch);
        str.append("\n"); //$NON-NLS-1$

        // Print batch contents
        for (int row = batch.getBeginRow(); row <= batch.getEndRow(); row++) {
        	str.append("\t").append(row).append(": ").append(batch.getTuple(row)).append("\n"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        }
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP, str.toString());
    }

    // =========================================================================
    //            O V E R R I D D E N    O B J E C T     M E T H O D S
    // =========================================================================

	/**
	 * Print plantree structure starting at this node
	 * @return String representing this node and all children under this node
	 */
	public String toString() {
		StringBuffer str = new StringBuffer();
		getRecursiveString(str, 0);
		return str.toString();
	}

	/**
	 * Just print single node to string instead of node+recursive plan.
	 * @return String representing just this node
	 */
	public String nodeToString() {
		StringBuffer str = new StringBuffer();
		getNodeString(str);
		return str.toString();
	}

	// Define a single tab
	private static final String TAB = "  "; //$NON-NLS-1$

	private void setTab(StringBuffer str, int tabStop) {
		for(int i=0; i<tabStop; i++) {
			str.append(TAB);
		}
	}

	private void getRecursiveString(StringBuffer str, int tabLevel) {
		setTab(str, tabLevel);
		getNodeString(str);
		str.append("\n"); //$NON-NLS-1$

		// Recursively add children at one greater tab level
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                children[i].getRecursiveString(str, tabLevel+1);
            } else {
                break;
            }
        }
	}

	protected void getNodeString(StringBuffer str) {
		str.append(getClassName());
		str.append("("); //$NON-NLS-1$
		str.append(getID());
		str.append(") output="); //$NON-NLS-1$
		str.append(getElements());
		str.append(" "); //$NON-NLS-1$
	}

	/**
	 * Helper for the toString to get the class name from the full class name.
	 * @param fullClassName Fully qualified class name
	 * @return Just the last part which is the class name
	 */
	protected String getClassName() {
		return this.getClass().getSimpleName();
	}

	/**
	 * All the implementation of Cloneable interface need to implement clone() method.
	 * The plan is only clonable in the pre-execution stage, not the execution state
	 * (things like program state, result sets, etc). It's only safe to call that method in between query processings,
	 * in other words, it's only safe to call clone() on a plan after nextTuple() returns null,
	 * meaning the plan has finished processing.
	 */
	public abstract Object clone();

	protected void copy(RelationalNode source, RelationalNode target){
		target.data = source.data;
        
        target.children = new RelationalNode[source.children.length];
        for(int i=0; i<source.children.length; i++) {
            if(source.children[i] != null) {
                target.children[i] = (RelationalNode)source.children[i].clone();
                target.children[i].setParent(target);
            } else {
                break;
            }
        }
	}
    
    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden
        PlanNode result = new PlanNode(getClassName());
        result.addProperty(PROP_OUTPUT_COLS, AnalysisRecord.getOutputColumnProperties(this.data.elements));
        if(this.getProcessingState().context != null && this.getProcessingState().context.getCollectNodeStatistics()) {
            result.addProperty(PROP_NODE_STATS_LIST, this.getProcessingState().nodeStatistics.getStatisticsList());
        }
        List<String> costEstimates = this.getCostEstimates();
        if(costEstimates != null) {
        	result.addProperty(PROP_NODE_COST_ESTIMATES, costEstimates);
        }
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                result.addProperty("Child " + i, this.children[i].getDescriptionProperties()); //$NON-NLS-1$
            }
        }
        return result;
    }

    /** 
     * @return Returns the nodeStatistics.
     * @since 4.2
     */
    public RelationalNodeStatistics getNodeStatistics() {
        return this.getProcessingState().nodeStatistics;
    }
    
    public void setEstimateNodeCardinality(Number estimateNodeCardinality) {
        this.data.estimateNodeCardinality = estimateNodeCardinality;
    }
    
    public void setEstimateNodeSetSize(Number setSizeEstimate) {
        this.data.setSizeEstimate = setSizeEstimate;
    }
    
    public void setEstimateDepAccessCardinality(Number depAccessEstimate) {
        this.data.depAccessEstimate = depAccessEstimate;
    }
    
    public void setEstimateDepJoinCost(Number estimateDepJoinCost){
        this.data.estimateDepJoinCost = estimateDepJoinCost;
    }
    
    public void setEstimateJoinCost(Number estimateJoinCost){
        this.data.estimateJoinCost = estimateJoinCost;
    }
    
    private List<String> getCostEstimates() {
        List<String> costEstimates = new ArrayList<String>();
        if(this.data.estimateNodeCardinality != null) {
            costEstimates.add("Estimated Node Cardinality: "+ this.data.estimateNodeCardinality); //$NON-NLS-1$
        }
        if(this.data.setSizeEstimate != null) {
            costEstimates.add("Estimated Independent Node Produced Set Size: "+ this.data.setSizeEstimate); //$NON-NLS-1$
        }
        if(this.data.depAccessEstimate != null) {
            costEstimates.add("Estimated Dependent Access Cardinality: "+ this.data.depAccessEstimate); //$NON-NLS-1$
        }
        if(this.data.estimateDepJoinCost != null) {
            costEstimates.add("Estimated Dependent Join Cost: "+ this.data.estimateDepJoinCost); //$NON-NLS-1$
        }
        if(this.data.estimateJoinCost != null) {
            costEstimates.add("Estimated Join Cost: "+ this.data.estimateJoinCost); //$NON-NLS-1$
        }
        if(costEstimates.size() <= 0) {
            return null;
        }
        return costEstimates;
    }
    
    /** 
     * @return Returns the estimateNodeCardinality.
     */
    public Number getEstimateNodeCardinality() {
        return this.data.estimateNodeCardinality;
    }

	private ProcessingState getProcessingState() {
		//construct lazily since not all tests call initialize
		if (this.processingState == null) {
			this.processingState = new ProcessingState();
		}
		return processingState;
	}
	
	public boolean hasFinalBuffer() {
		return false;
	}
	
	/**
     * return the final tuple buffer or null if not available
     * @return
	 * @throws TeiidProcessingException 
	 * @throws TeiidComponentException 
	 * @throws BlockedException 
     */
	public TupleBuffer getFinalBuffer() throws BlockedException, TeiidComponentException, TeiidProcessingException {
		return null;
	}
	
}