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

import java.util.LinkedList;
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
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.relational.ProjectIntoNode.Mode;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.util.CommandContext;

/**
 */
public class RelationalPlan extends ProcessorPlan {

	// Initialize state - don't reset
	private RelationalNode root;
	private List outputCols;
	private List<WithQueryCommand> with;
	
	private List<WithQueryCommand> withToProcess;
	private QueryProcessor withProcessor;
	private TempTableStore tempTableStore;
	private boolean multisourceUpdate;
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
    		tempTableStore = new TempTableStore(context.getConnectionID(), TransactionMode.NONE);
            tempTableStore.setParentTempTableStore(context.getTempTableStore());
            context.setTempTableStore(tempTableStore);
    		for (WithQueryCommand withCommand : this.with) {
    			withCommand.getCommand().getProcessorPlan().initialize(context, dataMgr, bufferMgr);
			}
        } 
    	setContext(context);
        connectExternal(this.root, context, dataMgr, bufferMgr);	
    }        

	private void connectExternal(RelationalNode node, CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {		
                                    
        node.initialize(context, bufferMgr, dataMgr);

        RelationalNode[] children = node.getChildren();  
        for(int i=0; i<children.length; i++) {
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
    public List getOutputElements() {
        return this.outputCols;
    }

    public void open()
        throws TeiidComponentException, TeiidProcessingException {
    	if (this.with != null) {
    		if (withToProcess == null) {
    			withToProcess = new LinkedList<WithQueryCommand>(with);
    		}
    		while (!withToProcess.isEmpty()) {
    			WithQueryCommand withCommand = withToProcess.get(0); 
    			if (withProcessor == null) {
	        		ProcessorPlan plan = withCommand.getCommand().getProcessorPlan();
					withProcessor = new QueryProcessor(plan, getContext(), this.root.getBufferManager(), this.root.getDataManager());
					Create create = new Create();
					create.setElementSymbolsAsColumns(withCommand.getColumns());
					create.setTable(withCommand.getGroupSymbol());
					this.root.getDataManager().registerRequest(getContext(), create, TempMetadataAdapter.TEMP_MODEL.getID(), null, 0, -1);
    			}
    			while (true) {
    				TupleBatch batch = withProcessor.nextBatch();
    				Insert insert = new Insert(withCommand.getGroupSymbol(), withCommand.getColumns(), null);
            		insert.setTupleSource(new CollectionTupleSource(batch.getTuples().iterator()));
            		this.root.getDataManager().registerRequest(getContext(), insert, TempMetadataAdapter.TEMP_MODEL.getID(), null, 0, -1);
    				if (batch.getTerminationFlag()) {
    					break;
    				}
    			}
        		this.tempTableStore.setUpdatable(withCommand.getGroupSymbol().getCanonicalName(), false);
        		withToProcess.remove(0);
        		withProcessor = null;
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
    	if (this.with != null) {
    		for (WithQueryCommand withCommand : this.with) {
    			withCommand.getCommand().getProcessorPlan().close();
			}
    		if (this.tempTableStore != null) {
    			this.tempTableStore.removeTempTables();
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
        	withToProcess = null;
        	withProcessor = null;
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
    public void setOutputElements(List outputCols) {
        this.outputCols = outputCols;
    }
    
    public void setMultisourceUpdate(boolean multisourceUpdate) {
		this.multisourceUpdate = multisourceUpdate;
	}
    
    @Override
    public boolean requiresTransaction(boolean transactionalReads) {
    	if (multisourceUpdate) {
    		return true;
    	}
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
    	return requiresTransaction(transactionalReads, root);
    }
    
    /**
     * Currently does not detect procedures in non-inline view subqueries
     */
    boolean requiresTransaction(boolean transactionalReads, RelationalNode node) {
    	if (node instanceof DependentAccessNode) {
			if (transactionalReads || !(((DependentAccessNode)node).getCommand() instanceof QueryCommand)) {
				return true;
			}
			return false;
		}
    	if (node instanceof ProjectIntoNode) {
    		if (((ProjectIntoNode)node).getMode() == Mode.ITERATOR) {
    			return transactionalReads;
    		}
    		return true;
    	} else if (node instanceof AccessNode) {
			return false;
		}
		if (transactionalReads) {
			return true;
		}
		if (node instanceof PlanExecutionNode) {
			ProcessorPlan plan = ((PlanExecutionNode)node).getProcessorPlan();
			return plan.requiresTransaction(transactionalReads);
		}
		for (RelationalNode child : node.getChildren()) {
			if (child != null && requiresTransaction(transactionalReads, child)) {
				return true;
			}
		}
		return false;
    }
    
    @Override
    public TupleBuffer getFinalBuffer() throws BlockedException, TeiidComponentException, TeiidProcessingException {
    	return root.getFinalBuffer();
    }
    
    @Override
    public boolean hasFinalBuffer() {
    	return root.hasFinalBuffer();
    }
	
}
