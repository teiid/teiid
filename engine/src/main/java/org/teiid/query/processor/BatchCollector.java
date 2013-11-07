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

package org.teiid.query.processor;

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.query.util.CommandContext;


public class BatchCollector {
	
	public interface BatchProducer {
	    /**
	     * Get a batch of results or possibly an Exception.
	     * @return Batch of results
	     * @throws BlockedException indicating next batch is not available yet
	     * @throws TeiidComponentException for non-business rule exception
	     * @throws TeiidProcessingException for business rule exception, related
	     * to user input or modeling
	     */
	    TupleBatch nextBatch() throws BlockedException, TeiidComponentException, TeiidProcessingException;
	    
	    /**
	     * Get list of resolved elements describing output columns for this plan.
	     * @return List of SingleElementSymbol
	     */
	    List getOutputElements();
	    
	    /**
	     * return the final tuple buffer or null if not available
	     * @param maxRows
	     * @return
	     * @throws TeiidProcessingException 
	     * @throws TeiidComponentException 
	     * @throws BlockedException 
	     */
	    TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException;
	    
	    boolean hasBuffer(boolean requireFinal);
	    
	    void close() throws TeiidComponentException;
	}
	
	public static class BatchProducerTupleSource implements TupleSource {
		private final BatchProducer sourceNode;
		private TupleBatch sourceBatch;           // Current batch loaded from the source, if blocked
		private int sourceRow = 1;

		public BatchProducerTupleSource(BatchProducer sourceNode) {
			this.sourceNode = sourceNode;
		}

		@Override
		public List<Object> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			while (true) {
				if(sourceBatch == null) {
		            // Read next batch
		            sourceBatch = sourceNode.nextBatch();
		        }
		        
		        if(sourceBatch.getRowCount() > 0 && sourceRow <= sourceBatch.getEndRow()) {
		            // Evaluate expressions needed for grouping
		            List tuple = sourceBatch.getTuple(sourceRow);
		            tuple = updateTuple(tuple);
		            sourceRow++;
		            return tuple;
		        }
		        
		        // Check for termination condition
		        if(sourceBatch.getTerminationFlag()) {
		        	sourceBatch = null;			            
		            return null;
		        } 
		        sourceBatch = null;
			}
		}
		
		@SuppressWarnings("unused")
		protected List<?> updateTuple(List<?> tuple) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
			return tuple;
		}

		@Override
		public void closeSource() {
			
		}
	}
	
    private BatchProducer sourceNode;

    private boolean done = false;
    private TupleBuffer buffer;
    private boolean forwardOnly;
    private int rowLimit = -1; //-1 means no_limit
    private boolean hasFinalBuffer;

	private boolean saveLastRow;
    
    public BatchCollector(BatchProducer sourceNode, BufferManager bm, CommandContext context, boolean forwardOnly) throws TeiidComponentException {
        this.sourceNode = sourceNode;
        this.forwardOnly = forwardOnly;
        this.hasFinalBuffer = this.sourceNode.hasBuffer(false);
        if (!this.hasFinalBuffer) {
            this.buffer = bm.createTupleBuffer(sourceNode.getOutputElements(), context.getConnectionId(), TupleSourceType.PROCESSOR);
            this.buffer.setForwardOnly(forwardOnly);
        }
    }

    public TupleBuffer collectTuples() throws TeiidComponentException, TeiidProcessingException {
    	return collectTuples(false);
    }
    
    public TupleBuffer collectTuples(boolean singleBatch) throws TeiidComponentException, TeiidProcessingException {
        TupleBatch batch = null;
    	while(!done) {
    		if (this.hasFinalBuffer) {
	    		if (this.buffer == null) {
	    			TupleBuffer finalBuffer = this.sourceNode.getBuffer(rowLimit);
	    			Assertion.isNotNull(finalBuffer);
					this.buffer = finalBuffer;
	    		}
	    		if (this.buffer.isFinal()) {
					this.buffer.setForwardOnly(forwardOnly);
					done = true;
					break;
				}
    		}
    		batch = sourceNode.nextBatch();
    		
    		if (rowLimit > 0 && rowLimit <= batch.getEndRow()) {
    	    	if (!done) {
    	    		this.sourceNode.close();
    	    	}
    	    	List<?> lastTuple = null;
    	    	if (saveLastRow) {
    	    		if (batch.getTerminationFlag()) {
    	    			lastTuple = batch.getTuples().get(batch.getTuples().size() - 1);
    	    		} else if (rowLimit < batch.getBeginRow()) {
    	    			continue; //skip until end
    	    		}
    	    	}
    	    	boolean modified = false;
    	    	if (rowLimit < batch.getEndRow()) {
    	    		int firstRow = Math.min(rowLimit + 1, batch.getBeginRow());
    	    		List<List<?>> tuples = batch.getTuples().subList(0, rowLimit - firstRow + 1);
    	    		batch = new TupleBatch(firstRow, tuples);
    	    		modified = true;
    	    	}
    	    	if (lastTuple != null) {
    	    		if (!modified) {
    	    			batch = new TupleBatch(batch.getBeginRow(), batch.getTuples());
    	    		}
    	    		batch.getTuples().add(lastTuple);
    	    	}
    	    	batch.setTerminationFlag(true);
    	    }
            
            flushBatch(batch);

            // Check for termination condition
            if(batch.getTerminationFlag()) {
            	done = true;
            	if (!this.sourceNode.hasBuffer(false)) {
            		buffer.close();
            	}
                break;
            }
            
            if (singleBatch) {
            	return null;
            }
        }
        return buffer;
    }
    
    public TupleBuffer getTupleBuffer() {
		return buffer;
	}
    
    /**
     * Flush the batch by giving it to the buffer manager.
     */
    private void flushBatch(TupleBatch batch) throws TeiidComponentException, TeiidProcessingException {
    	if (batch.getRowCount() == 0 && batch.getTermination() == TupleBatch.NOT_TERMINATED) {
    		return;
    	}
    	flushBatchDirect(batch, true);
    }
    
    @SuppressWarnings("unused")
	protected void flushBatchDirect(TupleBatch batch, boolean add) throws TeiidComponentException, TeiidProcessingException {
    	if (!this.hasFinalBuffer) {
    		buffer.addTupleBatch(batch, add);
    	}
    }
    
    public int getRowCount() {
    	if (buffer == null) {
    		return 0;
    	}
        return buffer.getRowCount();
    }
    
    public void setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
	}

	public void setSaveLastRow(boolean saveLastRow) {
		this.saveLastRow = saveLastRow;
	}
	
	public boolean isSaveLastRow() {
		return saveLastRow;
	}
    
}
