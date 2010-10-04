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
import java.util.List;

import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


public abstract class JoinStrategy {
            
    protected JoinNode joinNode;
    protected SourceState leftSource;
    protected SourceState rightSource;
    private int reserved;

    public void close() {
    	if (joinNode == null) {
    		return;
    	}
    	joinNode.getBufferManager().releaseBuffers(reserved);
		reserved = 0;
        try {
            if (leftSource != null) {
                leftSource.close();
            }
        } finally {
            try {
                if (rightSource != null) {
                    rightSource.close();
                }
            } finally {
                leftSource = null;
                rightSource = null;
            }
        }
    }
        
    public void initialize(JoinNode joinNode) {
        this.joinNode = joinNode;
        this.leftSource = new SourceState(joinNode.getChildren()[0], joinNode.getLeftExpressions());
        this.leftSource.markDistinct(this.joinNode.isLeftDistinct());
        this.rightSource = new SourceState(joinNode.getChildren()[1], joinNode.getRightExpressions());
        this.rightSource.markDistinct(this.joinNode.isRightDistinct());
    }
            
    protected void loadLeft() throws TeiidComponentException, TeiidProcessingException {
    }
    
    protected void loadRight() throws TeiidComponentException, TeiidProcessingException {
    }
    
    /**
     * Output a combined, projected tuple based on tuple parts from the left and right. 
     * @param leftTuple Left tuple part
     * @param rightTuple Right tuple part
     * @throws TeiidComponentException
     */
    protected List outputTuple(List leftTuple, List rightTuple) {
        List combinedRow = new ArrayList(this.joinNode.getCombinedElementMap().size());
        combinedRow.addAll(leftTuple);
        combinedRow.addAll(rightTuple);
        return combinedRow; 
    }
    
    protected abstract void process() throws TeiidComponentException, TeiidProcessingException;
    
    public abstract JoinStrategy clone();
    
    protected void openLeft() throws TeiidComponentException, TeiidProcessingException {
        if (!this.leftSource.open) {
            leftSource.getSource().open();
            this.leftSource.open = true;
        }
    }
    
    protected void openRight() throws TeiidComponentException, TeiidProcessingException {
    	if (!this.rightSource.open) {
			if (reserved == 0) {
				reserved = joinNode.getBufferManager().reserveBuffers(joinNode.getBufferManager().getSchemaSize(joinNode.getOutputElements()), BufferReserveMode.FORCE);
			}
			rightSource.getSource().open();
			this.rightSource.open = true;
		}
    }
        
}
