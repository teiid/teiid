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
	
	private int batchSize;
	private ArrayList<List<?>> batch;
	private int index;
	private BufferManager bm;

	public TupleCollector(TupleSourceID tsid, BufferManager bm) throws TupleSourceNotFoundException, MetaMatrixComponentException {
		this.tsid = tsid;
		this.batchSize = bm.getProcessorBatchSize();
		this.bm = bm;
		this.index = bm.getRowCount(tsid) + 1;
	}
	
	public TupleSourceID getTupleSourceID() {
		return tsid;
	}
	
	public void addTuple(List<?> tuple) throws TupleSourceNotFoundException, MetaMatrixComponentException {
		if (batch == null) {
			batch = new ArrayList<List<?>>(batchSize/4);
		}
		batch.add(tuple);
		if (batch.size() == batchSize) {
			saveBatch(false);
		}
	}

	public void saveBatch(boolean isLast) throws TupleSourceNotFoundException,
			MetaMatrixComponentException {
		ArrayList<List<?>> toSave = batch;
		if (toSave == null || toSave.isEmpty()) {
			if (!isLast) {
				return;
			}
			toSave = new ArrayList<List<?>>(0);
		}
		TupleBatch tb = new TupleBatch(index, toSave);
		tb.setTerminationFlag(isLast);
		this.bm.addTupleBatch(tsid, tb);
		this.index += toSave.size();
		batch = null;
	}
	
	public void close() throws TupleSourceNotFoundException, MetaMatrixComponentException {
		saveBatch(true);
		this.bm.setStatus(this.tsid, TupleSourceStatus.FULL);
	}
	
}
