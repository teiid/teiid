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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.common.buffer.BatchManager.ManagedBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.Expression;


public class TupleBuffer {
	
	/**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    public static String[] getTypeNames(List<? extends Expression> expressions) {
    	if (expressions == null) {
    		return null;
    	}
        String[] types = new String[expressions.size()];
        for (ListIterator<? extends Expression> i = expressions.listIterator(); i.hasNext();) {
            Expression expr = i.next();
            types[i.previousIndex()] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
    }

	//construction state
	private BatchManager manager;
	private String tupleSourceID;
	private List<? extends Expression> schema;
	private String[] types;
	private int batchSize;
	
	private int rowCount;
	private boolean isFinal;
    private TreeMap<Integer, BatchManager.ManagedBatch> batches = new TreeMap<Integer, BatchManager.ManagedBatch>();
	private ArrayList<List<?>> batchBuffer;
	private boolean removed;
	private boolean forwardOnly;
	private boolean prefersMemory;

	private LobManager lobManager;
	private int[] lobIndexes;
	private String uuid;
	private FileStore lobStore;
	
	public TupleBuffer(BatchManager manager, String id, List<? extends Expression> schema, int[] lobIndexes, int batchSize) {
		this.manager = manager;
		this.tupleSourceID = id;
		this.schema = schema;
		this.types = getTypeNames(schema);
		this.lobIndexes = lobIndexes;
		if (this.lobIndexes != null) {
			this.lobManager = new LobManager();
			this.lobStore = this.manager.createStorage("_lobs"); //$NON-NLS-1$
			this.lobStore.setCleanupReference(this);
		}
		this.batchSize = batchSize;		
	}
	
	public String getId() {
		if (this.uuid == null) {
			this.uuid = java.util.UUID.randomUUID().toString();
		}
		return this.uuid;
	}	
	
	public boolean isLobs() {
		return lobIndexes != null;
	}
	
	public void addTuple(List<?> tuple) throws TeiidComponentException {
		if (isLobs()) {
			lobManager.updateReferences(lobIndexes, tuple);
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
			for (List<?> tuple : batch.getTuples()) {
				addTuple(tuple);
			}
		} else {
			//add the lob references only, since they may still be referenced later
			if (isLobs()) {
				for (List<?> tuple : batch.getTuples()) {
					lobManager.updateReferences(lobIndexes, tuple);
				}
			}
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
	
	public void persistLobs() throws TeiidComponentException {
		if (this.lobManager != null) {
			this.lobManager.persist(this.lobStore);
		}
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
		BatchManager.ManagedBatch mbatch = manager.createManagedBatch(writeBatch, prefersMemory);
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
	    	if (forwardOnly) {
				batches.remove(entry.getKey());
			}
		}
		result.setDataTypes(types);
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
			if (this.lobStore != null) {
				this.lobStore.remove();
			}
			this.batchBuffer = null;
			purge();
			this.manager.remove();
			removed = true;
		}
	}
	
	/**
	 * Returns the total number of rows contained in managed batches
	 * @return
	 */
	public int getManagedRowCount() {
		if (!this.batches.isEmpty()) {
			int start = this.batches.firstKey();
			return rowCount - start + 1;
		} else if (this.batchBuffer != null) {
			return this.batchBuffer.size();
		} 
		return 0;
	}
	
	/**
	 * Returns the last row number
	 * @return
	 */
	public int getRowCount() {
		return rowCount;
	}
	
	public boolean isFinal() {
		return isFinal;
	}
	
	public void setFinal(boolean isFinal) {
		this.isFinal = isFinal;
	}
	
	public List<? extends Expression> getSchema() {
		return schema;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	    
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	if (lobManager == null) {
    		throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lobManager.getLobReference(id);
    }
    
    public void setForwardOnly(boolean forwardOnly) {
		this.forwardOnly = forwardOnly;
	}
    
	public IndexedTupleSource createIndexedTupleSource() {
		return createIndexedTupleSource(false);
	}
    
	/**
	 * Create a new iterator for this buffer
	 * @return
	 */
	public IndexedTupleSource createIndexedTupleSource(final boolean singleUse) {
		if (singleUse) {
			setForwardOnly(true);
		}
		return new AbstractTupleSource() {
			
			@Override
			protected List<?> finalRow() throws BlockedException {
				if(isFinal) {
		            return null;
		        } 
		        throw BlockedException.block("Blocking on non-final TupleBuffer", tupleSourceID); //$NON-NLS-1$
			}
			
			@Override
			public int available() {
				return rowCount - getCurrentIndex() + 1;
			}
			
			@Override
			protected TupleBatch getBatch(int row) throws TeiidComponentException {
				return TupleBuffer.this.getBatch(row);
			}
			
			@Override
			public void closeSource() {
				super.closeSource();
				if (singleUse) {
					remove();
				}
			}
		};
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
	
	public void setPrefersMemory(boolean prefersMemory) {
		this.prefersMemory = prefersMemory;
		for (ManagedBatch batch : this.batches.values()) {
			batch.setPrefersMemory(prefersMemory);
		}
	}
	
	public boolean isPrefersMemory() {
		return prefersMemory;
	}
	
	public String[] getTypes() {
		return types;
	}
	
}
