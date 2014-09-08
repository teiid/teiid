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

import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

public class DupRemoveNode extends RelationalNode {

	private STree stree = null;
	private TupleBatch batch;
	private int counter;
	
	public DupRemoveNode(int nodeID) {
		super(nodeID);
	}

    public void reset() {
        super.reset();
        stree = null;
        counter = 0;
        batch = null;
    }
    
    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
    	super.open();
    	
    	stree = getBufferManager().createSTree(this.getElements(), this.getConnectionID(), this.getElements().size());
    }

	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {
		while (true) {
			if (batch == null) {
				batch = this.getChildren()[0].nextBatch();
			}
			
			List<List<?>> tuples = batch.getTuples();
			for (;counter < tuples.size(); counter++) {
				List<?> tuple = tuples.get(counter);
				List<?> existing = stree.insert(tuple, InsertMode.NEW, -1);
				if (existing != null) {
					continue;
				}
				this.addBatchRow(tuple);
				if (this.isBatchFull()) {
					return pullBatch();
				}
			}
			if (batch.getTerminationFlag()) {
				terminateBatches();
				return pullBatch();
			}
			batch = null;
			counter = 0;
		}
    }

    public void closeDirect() {
    	if (stree != null) {
    		stree.remove();
    	}
    }

	public Object clone(){
		DupRemoveNode clonedNode = new DupRemoveNode(super.getID());
		copyTo(clonedNode);
		return clonedNode;
	}
    
}
