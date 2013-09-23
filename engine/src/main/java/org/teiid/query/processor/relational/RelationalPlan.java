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

import java.util.HashMap;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.language.SQLConstants;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TableProcessor;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.util.CommandContext;

/**
 */
public class RelationalPlan extends ProcessorPlan {

	// Initialize state - don't reset
	private RelationalNode root;
	private List<? extends Expression> outputCols;
	private List<WithQueryCommand> with;
	
	private TempTableStore tempTableStore;
	private SourceHint sourceHint;

    /**
     * Constructor for RelationalPlan.
     */
    public RelationalPlan(RelationalNode node) {
        this.root = node;
    }

    public RelationalNode getRootNode() {
        return this.root;
    }
    
    public void setRootNode(RelationalNode root) {
        this.root = root;
    }
    
    public void setWith(List<WithQueryCommand> with) {
		this.with = with;
	}
    
    public void setSourceHint(SourceHint sourceHint) {
		this.sourceHint = sourceHint;
	}
    
    public SourceHint getSourceHint() {
		return sourceHint;
	}
    
    /**
     * @see ProcessorPlan#connectDataManager(ProcessorDataManager)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
    	if (sourceHint != null && context.getSourceHint() == null) {
    		context.setSourceHint(sourceHint);
    	}
    	if (this.with != null) {
    		context = context.clone();
    		tempTableStore = new TempTableStore(context.getConnectionId(), TransactionMode.NONE);
            tempTableStore.setParentTempTableStore(context.getTempTableStore());
            context.setTempTableStore(tempTableStore);
        } 
    	setContext(context);
        connectExternal(this.root, context, dataMgr, bufferMgr);	
    }        

	private void connectExternal(RelationalNode node, CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {		
                                    
        node.initialize(context, bufferMgr, dataMgr);

        RelationalNode[] children = node.getChildren();
        int childCount = node.getChildCount();
        for(int i=0; i<childCount; i++) {
            if(children[i] != null) {
                connectExternal(children[i], context, dataMgr, bufferMgr);                
            } else {
                break;
            }
        }
    }
    
    /**
     * Get list of resolved elements describing output columns for this plan.
     * @return List of SingleElementSymbol
     */
    public List<? extends Expression> getOutputElements() {
        return this.outputCols;
    }

    @Override
    public void open()
        throws TeiidComponentException, TeiidProcessingException {
    	if (with != null && tempTableStore.getProcessors() == null) {
	    	HashMap<String, TableProcessor> processors = new HashMap<String, TableProcessor>();
	        tempTableStore.setProcessors(processors);
			for (WithQueryCommand withCommand : this.with) {
				ProcessorPlan plan = withCommand.getCommand().getProcessorPlan();
				QueryProcessor withProcessor = new QueryProcessor(plan, getContext(), root.getBufferManager(), root.getDataManager()); 
				processors.put(withCommand.getGroupSymbol().getName(), new TableProcessor(withProcessor, withCommand.getColumns()));
			}
    	}
        this.root.open();
    }

    /**
     * @see ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        return this.root.nextBatch();
    }

    public void close()
        throws TeiidComponentException {
		if (this.tempTableStore != null) {
			this.tempTableStore.removeTempTables();
			if (this.tempTableStore.getProcessors() != null) {
    			for (TableProcessor proc : this.tempTableStore.getProcessors().values()) {
    				proc.getQueryProcessor().closeProcessing();
    			}
    			this.tempTableStore.setProcessors(null);
			}
		}
        this.root.close();
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#reset()
     */
    public void reset() {
        super.reset();
        
        this.root.reset();
        if (this.with != null) {
        	for (WithQueryCommand withCommand : this.with) {
				withCommand.getCommand().getProcessorPlan().reset();
			}
        }
    }

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (this.with != null) {
			sb.append(SQLConstants.Reserved.WITH);
			for (WithQueryCommand withCommand : this.with) {
				sb.append("\n"); //$NON-NLS-1$
				sb.append(withCommand.getCommand().getProcessorPlan());
			}
		}
		sb.append(this.root.toString());
		return sb.toString();
	}
    
	public RelationalPlan clone(){
		RelationalPlan plan = new RelationalPlan((RelationalNode)root.clone());
		plan.sourceHint = this.sourceHint;
		plan.setOutputElements(outputCols);
		if (with != null) {
			List<WithQueryCommand> newWith = LanguageObject.Util.deepClone(this.with, WithQueryCommand.class);
			for (WithQueryCommand withQueryCommand : newWith) {
				withQueryCommand.getCommand().setProcessorPlan(withQueryCommand.getCommand().getProcessorPlan().clone());
			}
			plan.setWith(newWith);
		}
		return plan;
	}
	
    public PlanNode getDescriptionProperties() {
    	PlanNode node = this.root.getDescriptionProperties();
    	if (this.with != null) {
    		AnalysisRecord.addLanaguageObjects(node, AnalysisRecord.PROP_WITH, this.with);
        }
    	return node;
    }
    
    /** 
     * @param outputCols The outputCols to set.
     */
    public void setOutputElements(List<? extends Expression> outputCols) {
        this.outputCols = outputCols;
    }
    
    @Override
    public boolean requiresTransaction(boolean transactionalReads) {
    	if (this.with != null) {
    		if (transactionalReads) {
    			return true;
    		}
    		for (WithQueryCommand withCommand : this.with) {
				if (withCommand.getCommand().getProcessorPlan().requiresTransaction(transactionalReads)) {
					return true;
				}
			}
    	}
    	return Boolean.TRUE.equals(requiresTransaction(transactionalReads, root));
    }
    
    static Boolean requiresTransaction(boolean transactionalReads, RelationalNode node) {
    	Boolean requiresTxn = node.requiresTransaction(transactionalReads);
    	if (Boolean.TRUE.equals(requiresTxn)) {
    		return true;
    	}
		for (RelationalNode child : node.getChildren()) {
			if (child == null) {
				continue;
			}
			Boolean childRequires = requiresTransaction(transactionalReads, child);
			if (Boolean.TRUE.equals(childRequires)) {
				return true;
			}
			if (transactionalReads) {
				if (childRequires == null) {
					if (requiresTxn == null) {
						return true;
					}
					requiresTxn = null;
				}
			}
		}
		return requiresTxn;
    }
    
    @Override
    public TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
    	return root.getBuffer(maxRows);
    }
    
    @Override
    public boolean hasBuffer(boolean requireFinal) {
    	return root.hasBuffer(requireFinal);
    }
	
}
