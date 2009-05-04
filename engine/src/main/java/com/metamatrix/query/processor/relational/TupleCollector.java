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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;

public class TupleCollector {
	
	private TupleSourceID tsid;
	private BufferManager bm;
	
	private int batchSize;
	private ArrayList<List<?>> batch;
	private int index;

	public TupleCollector(TupleSourceID tsid, BufferManager bm) throws TupleSourceNotFoundException, MetaMatrixComponentException {
		this.tsid = tsid;
		this.batchSize = bm.getProcessorBatchSize();
		this.bm = bm;
		this.index = bm.getRowCount(tsid) + 1;
	}
	
	public TupleCollector(BufferManager bm) {
		this.batchSize = bm.getProcessorBatchSize();
		this.bm = bm;
		this.index = 1;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public void setTupleSourceID(TupleSourceID tsid) {
		this.tsid = tsid;
	}
	
	public boolean isEmpty() {
		return this.batch == null || this.batch.isEmpty();
	}
	
	public TupleSourceID getTupleSourceID() {
		return tsid;
	}
	
	public ArrayList<List<?>> getBatch() {
		if (batch == null) {
			batch = new ArrayList<List<?>>(batchSize/4);
		}
		return batch;
	}
	
	public void addTuple(List<?> tuple) throws TupleSourceNotFoundException, MetaMatrixComponentException {
		if (batch == null) {
			batch = new ArrayList<List<?>>(batchSize/4);
		}
		batch.add(tuple);
		if (batch.size() == batchSize) {
			saveBatch();
		}
	}

	public void saveBatch() throws TupleSourceNotFoundException,
			MetaMatrixComponentException {
		if (batch == null || batch.isEmpty()) {
			return;
		}
		int batchIndex = 0;
		while(batchIndex < batch.size()) {
            int writeEnd = Math.min(batch.size(), batchIndex + batchSize);

            TupleBatch writeBatch = new TupleBatch(index, batch.subList(batchIndex, writeEnd));
            bm.addTupleBatch(tsid, writeBatch);
            index += writeBatch.getRowCount();
            batchIndex += writeBatch.getRowCount();
        }
        batch = null;
	}
	
	public void close() throws TupleSourceNotFoundException, MetaMatrixComponentException {
		saveBatch();
		this.bm.setStatus(this.tsid, TupleSourceStatus.FULL);
	}
	
}
