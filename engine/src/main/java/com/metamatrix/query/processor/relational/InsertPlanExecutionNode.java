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

import java.util.Arrays;
import java.util.List;

import org.teiid.dqp.internal.process.PreparedStatementRequest;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.symbol.Reference;

public class InsertPlanExecutionNode extends PlanExecutionNode {

	private List<Reference> references;
	
    private int batchRow = 1;
    private int insertCount = 0;
    private TupleBatch currentBatch;
    private QueryMetadataInterface metadata;
	
	public InsertPlanExecutionNode(int nodeID, QueryMetadataInterface metadata) {
		super(nodeID);
		this.metadata = metadata;
	}
	
	public void setReferences(List<Reference> references) {
		this.references = references;
	}
	
	@Override
	protected void addBatchRow(List row) {
		this.insertCount += ((Integer)row.get(0)).intValue();
	}
	
	@Override
	protected TupleBatch pullBatch() {
		if (isLastBatch()) {
			super.addBatchRow(Arrays.asList(insertCount));
		}
		return super.pullBatch();
	}
	
	@Override
	protected boolean hasNextCommand() {
		return !this.currentBatch.getTerminationFlag() || batchRow <= this.currentBatch.getEndRow();
	}
	
	@Override
	protected boolean openPlanImmediately() {
		return false;
	}
	
	@Override
	protected boolean prepareNextCommand() throws BlockedException,
			MetaMatrixComponentException, MetaMatrixProcessingException {
		if (this.currentBatch == null) {
			this.currentBatch = this.getChildren()[0].nextBatch();
		}
		if (!hasNextCommand()) {
			return false;
		}
		//assign the reference values.
		PreparedStatementRequest.resolveParameterValues(this.references, this.currentBatch.getTuple(this.batchRow), getProcessorPlan().getContext(), this.metadata);
		this.batchRow++;
		return true;
	}
	
	public Object clone(){
		InsertPlanExecutionNode clonedNode = new InsertPlanExecutionNode(super.getID(), this.metadata);
		copy(this, clonedNode);
        return clonedNode;
	}
	
	protected void copy(InsertPlanExecutionNode source, InsertPlanExecutionNode target) {
		target.references = source.references;
		super.copy(source, target);
	}
	
	@Override
	public void closeDirect() {
		this.currentBatch = null;
	}
	
	@Override
	public void reset() {
		super.reset();
		this.currentBatch = null;
		this.batchRow = 1;
		this.insertCount = 0;
	}

}
