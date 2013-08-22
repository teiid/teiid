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

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

public abstract class AbstractTupleSource implements IndexedTupleSource {
    private int currentRow = 1;
    private int mark = 1;
	protected List<?> currentTuple;
	protected TupleBatch batch;

    @Override
    public int getCurrentIndex() {
    	return this.currentRow;
    }

    @Override
    public List<?> nextTuple()
    throws TeiidComponentException, TeiidProcessingException{
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

	protected List<?> getCurrentTuple() throws TeiidComponentException,
			BlockedException, TeiidProcessingException {
		if (available() > 0) {
			//if (forwardOnly) {
				int row = getCurrentIndex();
				if (batch == null || !batch.containsRow(row)) {
					batch = getBatch(row);
				}
				return batch.getTuple(row);
			//} 
			//TODO: determine if we should directly hold a soft reference here
			//return getRow(currentRow);
		}
		batch = null;
		return finalRow();
	}

	protected abstract List<?> finalRow() throws BlockedException, TeiidComponentException, TeiidProcessingException;
	
	protected abstract TupleBatch getBatch(int row) throws TeiidComponentException, TeiidProcessingException;
	
	protected abstract int available();
	
    @Override
    public void closeSource() {
    	batch = null;
        mark = 1;
        reset();
    }
    
    @Override
	public boolean hasNext() throws TeiidComponentException, TeiidProcessingException {
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
    public void mark() throws TeiidComponentException {
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