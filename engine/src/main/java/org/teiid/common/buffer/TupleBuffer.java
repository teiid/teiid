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

package org.teiid.common.buffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.DQPPlugin;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.symbol.Expression;


public class TupleBuffer {
	
	class TupleSourceImpl implements IndexedTupleSource {
	    private int currentRow = 1;
	    private int mark = 1;
		private List<?> currentTuple;
		private TupleBatch batch;

	    @Override
	    public int getCurrentIndex() {
	    	return this.currentRow;
	    }

	    @Override
	    public List getSchema(){
	        return schema;
	    }

	    @Override
	    public List<?> nextTuple()
	    throws TeiidComponentException{
	    	List<?> result = null;
	    	if (currentTuple != null){
				result = currentTuple;
				currentTuple = null;
	    	} else {
	    		result = getCurrentTuple();
	    	} 
	    	if (result != null) {
	    		currentRow++;
	    	}
	        return result;
	    }

		private List<?> getCurrentTuple() throws TeiidComponentException,
				BlockedException {
			if (currentRow <= rowCount) {
				//if (forwardOnly) {
					if (batch == null || !batch.containsRow(currentRow)) {
						batch = getBatch(currentRow);
					}
					return batch.getTuple(currentRow);
				//} 
				//TODO: determine if we should directly hold a soft reference here
				//return getRow(currentRow);
			}
			batch = null;
			if(isFinal) {
	            return null;
	        } 
	        throw BlockedException.INSTANCE;
		}

	    @Override
	    public void closeSource() {
	    	batch = null;
	        mark = 1;
	        reset();
	    }
	    
	    @Override
		public boolean hasNext() throws TeiidComponentException {
	        if (this.currentTuple != null) {
	            return true;
	        }
	        
	        this.currentTuple = getCurrentTuple();
			return this.currentTuple != null;
		}

		@Override
		public void reset() {
			this.setPosition(mark);
			this.mark = 1;
		}

	    @Override
	    public void mark() {
	        this.mark = currentRow;
	    }

	    @Override
	    public void setPosition(int position) {
	        if (this.currentRow != position) {
		        this.currentRow = position;
		        this.currentTuple = null;
	        }
	    }
	    
	    @Override
	    public int available() {
	    	return rowCount - currentRow + 1;
	    }
	}
	
    /**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    public static String[] getTypeNames(List expressions) {
    	if (expressions == null) {
    		return null;
    	}
        String[] types = new String[expressions.size()];
        for (ListIterator i = expressions.listIterator(); i.hasNext();) {
            Expression expr = (Expression)i.next();
            types[i.previousIndex()] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
    }

	private static final AtomicLong LOB_ID = new AtomicLong();
	
	//construction state
	private BatchManager manager;
	private String tupleSourceID;
	private List<?> schema;
	private String[] types;
	private int batchSize;
	
	private int rowCount;
	private boolean isFinal;
    private TreeMap<Integer, BatchManager.ManagedBatch> batches = new TreeMap<Integer, BatchManager.ManagedBatch>();
	private ArrayList<List<?>> batchBuffer;
	private boolean removed;
	private boolean forwardOnly;

    //lob management
    private Map<String, Streamable<?>> lobReferences; //references to contained lobs
    private boolean lobs = true;
	
	public TupleBuffer(BatchManager manager, String id, List<?> schema, int batchSize) {
		this.manager = manager;
		this.tupleSourceID = id;
		this.schema = schema;
		this.types = getTypeNames(schema);
		this.batchSize = batchSize;
		if (types != null) {
			int i = 0;
		    for (i = 0; i < types.length; i++) {
		        if (DataTypeManager.isLOB(types[i]) || types[i] == DataTypeManager.DefaultDataTypes.OBJECT) {
		        	break;
		        }
		    }
		    if (i == types.length) {
		    	lobs = false;
		    }
        }
	}
	
	public boolean isLobs() {
		return lobs;
	}
	
	public void addTuple(List<?> tuple) throws TeiidComponentException {
		if (lobs) {
			correctLobReferences(new List[] {tuple});
		}
		this.rowCount++;
		if (batchBuffer == null) {
			batchBuffer = new ArrayList<List<?>>(batchSize/4);
		}
		batchBuffer.add(tuple);
		if (batchBuffer.size() == batchSize) {
			saveBatch(false, false);
		}
	}
	
	/**
	 * Adds the given batch preserving row offsets.
	 * @param batch
	 * @throws TeiidComponentException
	 */
	public void addTupleBatch(TupleBatch batch, boolean save) throws TeiidComponentException {
		setRowCount(batch.getBeginRow() - 1); 
		if (save) {
			for (List<?> tuple : batch.getAllTuples()) {
				addTuple(tuple);
			}
		} else {
			//add the lob references only, since they may still be referenced later
			correctLobReferences(batch.getAllTuples()); 
		}
	}

	public void setRowCount(int rowCount)
			throws TeiidComponentException {
		assert this.rowCount <= rowCount;
		if (this.rowCount != rowCount) {
			saveBatch(false, true);
			this.rowCount = rowCount;
		}
	}
	
	public void purge() {
		if (this.batchBuffer != null) {
			this.batchBuffer.clear();
		}
		for (BatchManager.ManagedBatch batch : this.batches.values()) {
			batch.remove();
		}
		this.batches.clear();
	}
	
	/**
	 * Force the persistence of any rows held in memory.
	 * @throws TeiidComponentException
	 */
	public void saveBatch() throws TeiidComponentException {
		this.saveBatch(false, false);
	}

	void saveBatch(boolean finalBatch, boolean force) throws TeiidComponentException {
		Assertion.assertTrue(!this.isRemoved());
		if (batchBuffer == null || batchBuffer.isEmpty() || (!force && batchBuffer.size() < Math.max(1, batchSize / 32))) {
			return;
		}
        TupleBatch writeBatch = new TupleBatch(rowCount - batchBuffer.size() + 1, batchBuffer);
        if (finalBatch) {
        	writeBatch.setTerminationFlag(true);
        }
        writeBatch.setDataTypes(types);
		BatchManager.ManagedBatch mbatch = manager.createManagedBatch(writeBatch);
		this.batches.put(writeBatch.getBeginRow(), mbatch);
        batchBuffer = null;
	}
	
	public void close() throws TeiidComponentException {
		saveBatch(true, false);
		this.isFinal = true;
	}
	
	/**
	 * Get the batch containing the given row.
	 * NOTE: the returned batch may be empty or may begin with a row other
	 * than the one specified.
	 * @param row
	 * @return
	 * @throws TeiidComponentException
	 */
	public TupleBatch getBatch(int row) throws TeiidComponentException {
		TupleBatch result = null;
		if (row > rowCount) {
			result = new TupleBatch(rowCount + 1, new List[] {});
		} else if (this.batchBuffer != null && row > rowCount - this.batchBuffer.size()) {
			result = new TupleBatch(rowCount - this.batchBuffer.size() + 1, batchBuffer);
			if (forwardOnly) {
				this.batchBuffer = null;
			}
		} else {
			if (this.batchBuffer != null && !this.batchBuffer.isEmpty()) {
				//this is just a sanity check to ensure we're not holding too many
				//hard references to batches.
				saveBatch(isFinal, false);
			}
			Map.Entry<Integer, BatchManager.ManagedBatch> entry = batches.floorEntry(row);
			Assertion.isNotNull(entry);
			BatchManager.ManagedBatch batch = entry.getValue();
	    	result = batch.getBatch(!forwardOnly, types);
	    	if (lobs && result.getDataTypes() == null) {
		        correctLobReferences(result.getAllTuples());
	    	}
	    	result.setDataTypes(types);
	    	if (forwardOnly) {
				batches.remove(entry.getKey());
			}
		}
		if (isFinal && result.getEndRow() == rowCount) {
			result.setTerminationFlag(true);
		}
		return result;
	}
	
	public void remove() {
		if (!removed) {
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
	            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Removing TupleBuffer:", this.tupleSourceID); //$NON-NLS-1$
	        }
			this.batchBuffer = null;
			purge();
			this.manager.remove();
			removed = true;
		}
	}
	
	public int getRowCount() {
		return rowCount;
	}
	
	public boolean isFinal() {
		return isFinal;
	}
	
	public void setFinal(boolean isFinal) {
		this.isFinal = isFinal;
	}
	
	public List<?> getSchema() {
		return schema;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	    
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	Streamable<?> lob = null;
    	if (this.lobReferences != null) {
    		lob = this.lobReferences.get(id);
    	}
    	if (lob == null) {
    		throw new TeiidComponentException(DQPPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob;
    }
    
    /**
     * If a tuple batch is being added with Lobs, then references to
     * the lobs will be held on the {@link TupleSourceInfo} 
     * @param batch
     * @throws TeiidComponentException 
     */
    @SuppressWarnings("unchecked")
	private void correctLobReferences(List[] rows) throws TeiidComponentException {
        int columns = schema.size();
        // walk through the results and find all the lobs
        for (int row = 0; row < rows.length; row++) {
            for (int col = 0; col < columns; col++) {                                                
                Object anObj = rows[row].get(col);
                
                if (!(anObj instanceof Streamable<?>)) {
                	continue;
                }
                Streamable lob = (Streamable)anObj;                  
                String id = lob.getReferenceStreamId();
            	if (id == null) {
            		id = String.valueOf(LOB_ID.getAndIncrement());
            		lob.setReferenceStreamId(id);
            	}
            	if (this.lobReferences == null) {
            		this.lobReferences = Collections.synchronizedMap(new HashMap<String, Streamable<?>>());
            	}
            	this.lobReferences.put(id, lob);
                if (lob.getReference() == null) {
                	lob.setReference(getLobReference(lob.getReferenceStreamId()).getReference());
                }
            }
        }
    }
    
    public void setForwardOnly(boolean forwardOnly) {
		this.forwardOnly = forwardOnly;
	}
    
	/**
	 * Create a new iterator for this buffer
	 * @return
	 */
	public IndexedTupleSource createIndexedTupleSource() {
		return new TupleSourceImpl();
	}
	
	@Override
	public String toString() {
		return this.tupleSourceID;
	}
	
	public boolean isRemoved() {
		return removed;
	}
	
	public boolean isForwardOnly() {
		return forwardOnly;
	}
	
}
