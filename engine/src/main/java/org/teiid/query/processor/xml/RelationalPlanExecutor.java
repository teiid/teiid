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

package org.teiid.query.processor.xml;

import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.tempdata.AlterTempTable;
import org.teiid.query.util.CommandContext;

/** 
 * This is a helper class which can execute a supplied relational plan and supply
 * resulting query results to the caller.
 */
class RelationalPlanExecutor implements PlanExecutor {
    
    private final class TempLoadTupleSource implements TupleSource {
		@Override
		public List<?> nextTuple() throws TeiidComponentException,
				TeiidProcessingException {
			try {
				List<?> tuple = tupleSource.nextTuple();
				if (tuple == null) {
					doneLoading = true;
				}
				return tuple;
			} catch (BlockedException e) {
				return null;
			}
		}

		@Override
		public void closeSource() {
			tupleSource.closeSource();
		}
	}

	QueryProcessor internalProcessor;

    // information about the result set.
    ResultSetInfo resultInfo;
    // buffer store
    BufferManager bufferMgr;
    // flag to denote the end of rows
    boolean endOfRows = false;
    // results after the execution bucket.
    TupleSource tupleSource;
    // cached current row of results.
    List<?> currentRow;
    int currentRowNumber = 0;
    private ProcessorDataManager dataManager;
    private boolean executed;
    private boolean doneLoading;
    
    public RelationalPlanExecutor (ResultSetInfo resultInfo, CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) 
        throws TeiidComponentException{
        
        this.resultInfo = resultInfo;
        this.bufferMgr = bufferMgr;
        this.dataManager = dataMgr;
        
        ProcessorPlan plan = resultInfo.getPlan();
        CommandContext subContext = context.clone();
        subContext.pushVariableContext(new VariableContext());
        this.internalProcessor = new QueryProcessor(plan, subContext, bufferMgr, dataMgr);
    }
    
    /** 
     * @see org.teiid.query.processor.xml.PlanExecutor#getOutputElements()
     */
    public List getOutputElements() throws TeiidComponentException {
        ProcessorPlan plan = resultInfo.getPlan();
        return plan.getOutputElements();
    }      

    /**
     * @throws TeiidProcessingException 
     * @see org.teiid.query.processor.xml.PlanExecutor#execute(java.util.Map, boolean)
     */
    public void execute(Map referenceValues, boolean openOnly) throws TeiidComponentException, BlockedException, TeiidProcessingException {        
        if (this.tupleSource == null) {
        	setReferenceValues(referenceValues);
            this.tupleSource = new BatchIterator(internalProcessor);
            if (openOnly) {
            	internalProcessor.init();
            }
        }
        if (!openOnly && !executed) {
	        String tempTable = this.resultInfo.getTempTable();
			if (tempTable != null && !doneLoading && !this.resultInfo.isAutoStaged()) {
	        	LogManager.logDetail(LogConstants.CTX_XML_PLAN, "Loading result set temp table", tempTable); //$NON-NLS-1$

	        	Insert insert = this.resultInfo.getTempInsert();
	        	insert.setTupleSource(new TempLoadTupleSource());
	        	this.dataManager.registerRequest(this.internalProcessor.getContext(), insert, TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
	        	if (!doneLoading) {
	        		throw BlockedException.block(resultInfo.getResultSetName(), "Blocking on result set load"); //$NON-NLS-1$
	        	}
        		internalProcessor.closeProcessing();
				AlterTempTable att = new AlterTempTable(tempTable);
				//mark the temp table as non-updatable
				this.dataManager.registerRequest(this.internalProcessor.getContext(), att, TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
        		this.tupleSource = this.dataManager.registerRequest(this.internalProcessor.getContext(), this.resultInfo.getTempSelect(), TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
	        }
			//force execution
        	currentRow();
        	
			if (this.resultInfo.isAutoStaged() && tempTable != null) {
				AlterTempTable att = new AlterTempTable(tempTable);
				//TODO: if the parent is small, then this is not necessary
				att.setIndexColumns(this.resultInfo.getFkColumns());
				this.dataManager.registerRequest(this.internalProcessor.getContext(), att, TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
			}

			this.currentRowNumber = 0;
			this.executed = true;
        }
    }    
    
    void setReferenceValues(Map<ElementSymbol, Object> referencesValues) {
        if (referencesValues == null || referencesValues.isEmpty()) {
        	return;
        }
        for (Map.Entry<ElementSymbol, Object> entry : referencesValues.entrySet()) {
            this.internalProcessor.getContext().getVariableContext().setValue(entry.getKey(), entry.getValue());
		}
    }    
    
    /**
     * Get the next row from the result set
     * @return
     * @throws TeiidComponentException
     */
    public List nextRow() throws TeiidComponentException, TeiidProcessingException {
        // if we not already closed the tuple source; look for more results.
        if (!endOfRows) {

            // get the next row
        	if (this.currentRow == null || this.currentRowNumber > 0) {
        		this.currentRow = this.tupleSource.nextTuple();     
        	}
            this.currentRowNumber++;

            // check if we walked over the row limit
            if (this.currentRow != null && this.resultInfo.getUserRowLimit() > 0 && this.currentRowNumber > this.resultInfo.getUserRowLimit()) {
                if (this.resultInfo.exceptionOnRowlimit()) {
                    throw new TeiidProcessingException(QueryPlugin.Util.getString("row_limit_passed", new Object[] { new Integer(this.resultInfo.getUserRowLimit()), this.resultInfo.getResultSetName()})); //$NON-NLS-1$                
                }
                // well, we did not throw a exception, that means we need to limit it to current row
                this.currentRow = null;
            }
        }
        
        if (this.currentRow == null) {
            this.endOfRows = true;
        }
        return this.currentRow;
    }
    
    /**
     * Get the current row. 
     * @return
     */
    public List currentRow() throws TeiidComponentException, TeiidProcessingException {
        // automatically forward to the very first row.
        if (this.currentRow == null && !endOfRows) {
            return nextRow();
        }

        return this.currentRow;
    }
    
    /**
     * Close the executor and release all the resources.
     */
    public void close() throws TeiidComponentException {
		this.internalProcessor.closeProcessing();
		if (this.tupleSource != null) {
			this.tupleSource.closeSource();
		}
		String rsTempTable = this.resultInfo.getTempTable();
		if (rsTempTable != null) {
        	LogManager.logDetail(LogConstants.CTX_XML_PLAN, "Unloading result set temp table", rsTempTable); //$NON-NLS-1$
        	internalProcessor.closeProcessing();
        	try {
				this.tupleSource = this.dataManager.registerRequest(this.internalProcessor.getContext(), this.resultInfo.getTempDrop(), TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
			} catch (TeiidProcessingException e) {
		        LogManager.logDetail(org.teiid.logging.LogConstants.CTX_XML_PLAN, e, "Error dropping result set temp table", rsTempTable); //$NON-NLS-1$
			}
        }
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"closed executor", resultInfo.getResultSetName()}); //$NON-NLS-1$
    }
  
}
