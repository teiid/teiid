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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 */
public class FakeRelationalNode extends RelationalNode {

    // For raw data mode
    private List[] data;
    
    // For tuple source mode
    private TupleSource source;
    private int batchSize;
    
    // State    
    private int currentRow;

    /**
     * Constructor for FakeRelationalNode.
     * @param nodeID
     */
    public FakeRelationalNode(int nodeID, List[] data) {
        super(nodeID);
        this.data = data;
        this.currentRow = 0;
    }
    
    @Override
    public void reset() {
    	super.reset();
    	this.currentRow = 0;
    }

    public FakeRelationalNode(int nodeID, List[] data, int batchSize) {
        super(nodeID);
        this.data = data;
        this.currentRow = 0;
        this.batchSize = batchSize;
    }

    public FakeRelationalNode(int nodeID, TupleSource source, int batchSize) {
        super(nodeID);
        this.source = source;
        this.batchSize = batchSize;
    }

    /**
     * @throws TeiidProcessingException 
     * @see com.metamatrix.query.processor.relational.x.RelationalNode#nextBatch()
     */
    public TupleBatch nextBatchDirect() throws BlockedException, TeiidComponentException, TeiidProcessingException {
        if(data != null) {
            if(currentRow < data.length) {
                int endRow = Math.min(data.length, currentRow+getBatchSize());            
                List batchRows = new ArrayList();
                for(int i=currentRow; i<endRow; i++) {
                    batchRows.add(data[i]);    
                }
                
                TupleBatch batch = new TupleBatch(currentRow+1, batchRows);
                currentRow += batch.getRowCount();
                
                if(currentRow >= data.length) {
                    batch.setTerminationFlag(true);
                }
                return batch;
    
            }
            TupleBatch batch = new TupleBatch(currentRow+1, Collections.EMPTY_LIST);
            batch.setTerminationFlag(true);
            return batch;    
        }
        boolean last = false; 
        List rows = new ArrayList(batchSize);
        for(int i=0; i<batchSize; i++) { 
            List tuple = source.nextTuple();    
            if(tuple == null) { 
                last = true;
                break;
            }
            rows.add(tuple);    
        }
        
        TupleBatch batch = new TupleBatch(currentRow+1, rows);
        if(last) {
            batch.setTerminationFlag(true);
        } else {
            currentRow += rows.size();
        }                        
        
        return batch;
    }        

    
    
    /** 
     * @see org.teiid.query.processor.relational.RelationalNode#getBatchSize()
     * @since 4.2
     */
    protected int getBatchSize() {
        if(this.batchSize != 0) {
            return this.batchSize;
        }
        return super.getBatchSize();
    }
    
	public Object clone(){
		throw new UnsupportedOperationException();
	}
}
