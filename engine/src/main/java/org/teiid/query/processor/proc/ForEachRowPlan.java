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

package org.teiid.query.processor.proc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.BatchCollector;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.util.CommandContext;

public class ForEachRowPlan extends ProcessorPlan {
	
	private ProcessorPlan queryPlan;
	private ProcedurePlan rowProcedure;
	private Map<ElementSymbol, Expression> params;
	private Map lookupMap;
	
	private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
    
    private QueryProcessor queryProcessor;
    private TupleSource tupleSource;
    private QueryProcessor rowProcessor;
    private List<?> currentTuple;
    private int updateCount;

	@Override
	public ProcessorPlan clone() {
		ForEachRowPlan clone = new ForEachRowPlan();
		clone.setQueryPlan(queryPlan.clone());
		clone.setRowProcedure((ProcedurePlan) rowProcedure.clone());
		clone.setParams(params);
		clone.setLookupMap(lookupMap);
		return clone;
	}

	@Override
	public void close() throws TeiidComponentException {
		if (this.queryProcessor != null) {
			this.queryProcessor.closeProcessing();
			if (this.rowProcessor != null) {
				this.rowProcessor.closeProcessing();
			}
		}
	}

	@Override
	public List<SingleElementSymbol> getOutputElements() {
		return Command.getUpdateCommandSymbol();
	}

	@Override
	public void initialize(CommandContext context,
			ProcessorDataManager dataMgr, BufferManager bufferMgr) {
		setContext(context);
		this.dataMgr = dataMgr;
		this.bufferMgr = bufferMgr;
	}

	@Override
	public TupleBatch nextBatch() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		while (true) {
			if (currentTuple == null) {
				currentTuple = tupleSource.nextTuple();
				if (currentTuple == null) {
					TupleBatch result = new TupleBatch(1, new List[] {Arrays.asList(updateCount)});
					result.setTerminationFlag(true);
					return result;
				}
			}
			if (this.rowProcessor == null) {
				rowProcedure.reset();
				this.rowProcessor = new QueryProcessor(rowProcedure, getContext(), this.bufferMgr, this.dataMgr);
				for (Map.Entry<ElementSymbol, Expression> entry : this.params.entrySet()) {
					Integer index = (Integer)this.lookupMap.get(entry.getValue());
					if (index != null) {
						rowProcedure.getCurrentVariableContext().setValue(entry.getKey(), this.currentTuple.get(index));
					} else {
						rowProcedure.getCurrentVariableContext().setValue(entry.getKey(), entry.getValue());
					}
				}
			}
			//just getting the next batch is enough
			this.rowProcessor.nextBatch();
			this.rowProcessor.closeProcessing();
			this.rowProcessor = null;
			this.currentTuple = null;
			this.updateCount++;
		}		
	}

	@Override
	public void open() throws TeiidComponentException, TeiidProcessingException {
		queryProcessor = new QueryProcessor(queryPlan, getContext(), this.bufferMgr, this.dataMgr);
		tupleSource = new BatchCollector.BatchProducerTupleSource(queryProcessor);
	}
	
	public void setQueryPlan(ProcessorPlan queryPlan) {
		this.queryPlan = queryPlan;
	}
	
	public void setRowProcedure(ProcedurePlan rowProcedure) {
		this.rowProcedure = rowProcedure;
	}
	
	public void setParams(Map<ElementSymbol, Expression> params) {
		this.params = params;
	}
	
	public void setLookupMap(Map symbolMap) {
		this.lookupMap = symbolMap;
	}
	
	@Override
	public void reset() {
		super.reset();
		this.queryPlan.reset();
		this.updateCount = 0;
		this.currentTuple = null;
		this.rowProcessor = null;
		this.queryProcessor = null;
		this.tupleSource = null;
	}
	
	@Override
	public boolean requiresTransaction(boolean transactionalReads) {
		return true;
	}
	
	@Override
	public void getAccessedGroups(List<GroupSymbol> groups) {
		this.queryPlan.getAccessedGroups(groups);
		this.rowProcedure.getAccessedGroups(groups);
	}

}
