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

package com.metamatrix.common.buffer;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;

public class TupleBuffer {
	
	class TupleSourceImpl implements IndexedTupleSource {
	    private SoftReference<TupleBatch> currentBatch;
	    private int currentRow = 1;
	    private int mark = 1;
		private List<?> currentTuple;

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
	    throws MetaMatrixComponentException{
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

		private List<?> getCurrentTuple() throws MetaMatrixComponentException,
				BlockedException {
			TupleBatch batch = getBatch();
	        if(batch.getRowCount() == 0) {
	            // Check if last
                if(isFinal) {
                	currentBatch = null;
                    return null;
                } 
                throw BlockedException.INSTANCE;
	        }

	        return batch.getTuple(currentRow);
		}

	    @Override
	    public void closeSource()
	    throws MetaMatrixComponentException{
	    	currentBatch = null;
	        mark = 1;
	        reset();
	    }
	    
	    // Retrieves the necessary batch based on the currentRow
	    TupleBatch getBatch()
	    throws MetaMatrixComponentException{
	    	TupleBatch batch = null;
	    	if (currentBatch != null) {
	            batch = currentBatch.get();
	        }
	        if (batch != null) {
	            if (currentRow <= batch.getEndRow() && currentRow >= batch.getBeginRow()) {
	                return batch;
	            }
	            currentBatch = null;
	        } 
	        
            batch = TupleBuffer.this.getBatch(currentRow);
            if (batch != null) {
            	currentBatch = new SoftReference<TupleBatch>(batch);
            }
	        return batch;
	    }
	    
	    @Override
		public boolean hasNext() throws MetaMatrixComponentException {
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
	}

	private static class LobReference extends PhantomReference<Streamable<?>> {
		
		String persistentStreamId;
		
		public LobReference(Streamable<?> lob) {
			super(lob, LOB_QUEUE);
			this.persistentStreamId = lob.getPersistenceStreamId();
		}		
	}
	
	private static final AtomicLong LOB_ID = new AtomicLong();
	private static final ReferenceQueue<Streamable<?>> LOB_QUEUE = new ReferenceQueue<Streamable<?>>();
	
	//construction state
	private StorageManager manager;
	private String groupName;
	private TupleSourceID tupleSourceID;
	private List<?> schema;
	private String[] types;
	private int batchSize;

	private int rowCount;
	private boolean isFinal;
    private TreeMap<Integer, ManagedBatch> batches = new TreeMap<Integer, ManagedBatch>();
	private ArrayList<List<?>> batchBuffer;

    //lob management
    private Map<String, Streamable<?>> lobReferences; //references to contained lobs
    private boolean lobs = true;
	private LobReference containingLobReference; //reference to containing lob
	
	public TupleBuffer(StorageManager manager, String groupName, TupleSourceID id, List<?> schema, String[] types, int batchSize) {
		this.manager = manager;
		this.tupleSourceID = id;
		this.schema = schema;
		this.types = types;
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
		this.groupName = groupName;
	}
	
	public String getGroupName() {
		return groupName;
	}

	public boolean isBatchBufferEmpty() {
		return this.batchBuffer == null || this.batchBuffer.isEmpty();
	}
	
	public void addTuple(List<?> tuple) throws MetaMatrixComponentException {
		this.rowCount++;
		if (batchBuffer == null) {
			batchBuffer = new ArrayList<List<?>>(batchSize/4);
		}
		batchBuffer.add(tuple);
		if (batchBuffer.size() == batchSize) {
			saveBatch(false);
		}
	}

	void saveBatch(boolean finalBatch) throws MetaMatrixComponentException {
		if ((batchBuffer == null || batchBuffer.isEmpty()) && !finalBatch) {
			return;
		}
		List rows = batchBuffer==null?Collections.emptyList():batchBuffer;
        TupleBatch writeBatch = new TupleBatch(rowCount - rows.size() + 1, rows);
        if (finalBatch) {
        	writeBatch.setTerminationFlag(true);
        }
        if (writeBatch.getTerminationFlag()) {
			this.isFinal = true;
		}
		correctLobReferences(writeBatch);
		ManagedBatch mbatch = new ManagedBatch(writeBatch);
		this.batches.put(mbatch.getBeginRow(), mbatch);
		manager.addBatch(tupleSourceID, writeBatch, types);
        batchBuffer = null;
	}
	
	public void close() throws MetaMatrixComponentException {
		saveBatch(true);
	}

	public TupleBatch getBatch(int row) throws MetaMatrixComponentException {
		if (row > rowCount) {
			TupleBatch batch = new TupleBatch(rowCount + 1, new List[] {});
			if (isFinal) {
				batch.setTerminationFlag(true);
			}
			return batch;
		}
		if (this.batchBuffer != null && row > rowCount - this.batchBuffer.size()) {
			return new TupleBatch(rowCount - this.batchBuffer.size() + 1, batchBuffer);
		}
		Map.Entry<Integer, ManagedBatch> entry = batches.floorEntry(row);
		ManagedBatch batch = null;
        if (entry != null && entry.getValue().getEndRow() >= row) {
        	batch = entry.getValue();
        	TupleBatch result = batch.getBatch();
        	if (result != null) {
        		return result;
        	}
        	row = batch.getBeginRow();
        }
		try {
			TupleBatch result = manager.getBatch(tupleSourceID, row, types);
			correctLobReferences(result);
			if (batch != null) {
				batch.setBatchReference(result);
			}
			return result;
		} catch (TupleSourceNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		}
	}
	
	public void remove() {
		manager.removeBatches(tupleSourceID);
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
	
    public void setContainingLobReference(Streamable<?> s) {
    	cleanLobTupleSource();
    	s.setPersistenceStreamId(tupleSourceID.getStringID());
    	Assertion.isNull(this.containingLobReference);
		this.containingLobReference = new LobReference(s);
	}
    
    /**
     * Attempt to clean up lingering tuplebuffers to lobs that are no longer referenced
     */
	private void cleanLobTupleSource() {
		for (int i = 0; i < 10; i++) {
			String tupleSourceId = null;
			LobReference ref = (LobReference)LOB_QUEUE.poll();
	    	if (ref != null) {
	    		tupleSourceId = ref.persistentStreamId;
	    	}
			if (tupleSourceId == null) {
				break;
			}
			this.manager.removeBatches(new TupleSourceID(tupleSourceId));
		}
	}
    
    public Streamable<?> getLobReference(String id) throws MetaMatrixComponentException {
    	Streamable<?> lob = null;
    	if (this.lobReferences != null) {
    		lob = this.lobReferences.get(id);
    	}
    	if (lob == null) {
    		throw new MetaMatrixComponentException(DQPPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob;
    }
    
    /**
     * If a tuple batch is being added with Lobs, then references to
     * the lobs will be held on the {@link TupleSourceInfo} 
     * @param batch
     * @throws MetaMatrixComponentException 
     */
    @SuppressWarnings("unchecked")
	private void correctLobReferences(TupleBatch batch) throws MetaMatrixComponentException {
    	if (!lobs) {
    		return;
    	}
        List[] rows = batch.getAllTuples();
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
    
	/**
	 * Create a new iterator for this buffer
	 * @return
	 */
	public IndexedTupleSource createIndexedTupleSource() {
		return new TupleSourceImpl();
	}
	
	@Override
	public String toString() {
		return this.tupleSourceID.toString();
	}
	
}
