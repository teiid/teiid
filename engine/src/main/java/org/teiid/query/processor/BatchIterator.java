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

import org.teiid.common.buffer.AbstractTupleSource;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchCollector.BatchProducer;


/**
 * A BatchIterator provides an iterator interface to a {@link BatchProducer}.
 * By setting {@link #setBuffer(TupleBuffer)}, 
 * the iterator can copy on read into a {@link TupleBuffer} for repeated reading.
 * 
 * Note that the saveOnMark buffering only lasts until the next mark is set.
 */
public class BatchIterator extends AbstractTupleSource {

    private final BatchProducer source;
    private boolean saveOnMark;
    private TupleBuffer buffer;
    private boolean done;
    private boolean mark;

    public BatchIterator(BatchProducer source) {
        this.source = source;
    }
    
	@Override
	protected TupleBatch getBatch(long row) throws TeiidComponentException, TeiidProcessingException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected List<?> finalRow() throws TeiidComponentException, TeiidProcessingException {
		if (this.buffer != null && this.getCurrentIndex() <= this.buffer.getRowCount()) {
			batch = this.buffer.getBatch(this.getCurrentIndex());
    	}
		while (available() < 1) {
			if (done) {
				return null;
			}
			batch = source.nextBatch();
			done = batch.getTerminationFlag();
			if (buffer != null && (!saveOnMark || mark) && !buffer.isForwardOnly()) {
            	buffer.addTupleBatch(batch, true);
            }
			if (done && buffer != null) {
				this.buffer.close();
			}
		}
		return getCurrentTuple();
	}
	
	@Override
	protected List<?> getCurrentTuple() throws TeiidComponentException,
			BlockedException, TeiidProcessingException {
		List<?> tuple = super.getCurrentTuple();
		saveTuple(tuple);
		return tuple;
	}

	private void saveTuple(List<?> tuple) throws TeiidComponentException {
		if (tuple != null && mark && saveOnMark && this.getCurrentIndex() > this.buffer.getRowCount()) {
        	this.buffer.setRowCount(this.getCurrentIndex() - 1);
        	this.buffer.addTuple(tuple);
        }
	}
	
	public long available() {
		if (batch != null && batch.containsRow(getCurrentIndex())) {
			return batch.getEndRow() - getCurrentIndex() + 1;
		}
		return 0;
	}

    public void setBuffer(TupleBuffer buffer, boolean saveOnMark) {
		this.buffer = buffer;
		this.saveOnMark = saveOnMark;
	}   
    
    @Override
    public void closeSource() {
    	if (this.buffer != null) {
    		this.buffer.remove();
    		this.buffer = null;
    	}
    }
    
    @Override
    public void reset() {
    	super.reset();
    	if (this.buffer != null) {
    		mark = false;
    		return;
    	}
    }

    @Override
    public void mark() throws TeiidComponentException {
    	super.mark();
    	if (this.buffer != null && saveOnMark && this.getCurrentIndex() > this.buffer.getRowCount()) {
			this.buffer.purge();
    	}
    	mark = true;
		saveTuple(this.currentTuple);
    }
    
    @Override
    public void setPosition(long position) {
    	if (this.buffer == null && position < getCurrentIndex() && position < (this.batch != null ? batch.getBeginRow() : Long.MAX_VALUE)) {
			throw new UnsupportedOperationException("Backwards positioning is not allowed"); //$NON-NLS-1$
    	}
    	super.setPosition(position);
    }

    /**
     * non-destructive method to set the mark
     * @return true if the mark was set
     */
	public boolean ensureSave() {
		if (!saveOnMark || mark) {
			return false;
		}
		mark = true;
		return true;
	}
	
	public void disableSave() {
		if (buffer != null) {
			this.saveOnMark = true;
			this.mark = false;
			if (batch != null && batch.getEndRow() <= this.buffer.getRowCount()) {
				this.batch = null;
			}
		}
	}
	
	public void readAhead(long limit) throws TeiidComponentException, TeiidProcessingException {
		if (buffer == null || done) {
			return;
		}
		if (this.buffer.getManagedRowCount() >= limit) {
			return;
		}
		if (this.batch != null && this.buffer.getRowCount() < this.batch.getEndRow() && !this.buffer.isForwardOnly()) {
			//haven't saved already
			this.buffer.addTupleBatch(this.batch, true);
		}
		TupleBatch tb = source.nextBatch();
		done = tb.getTerminationFlag();
		this.buffer.addTupleBatch(tb, true);
		if (done) {
			this.buffer.close();
		}
	}
	
	public TupleBuffer getBuffer() {
		return buffer;
	}
    
}
