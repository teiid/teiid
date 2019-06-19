/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.RecursiveTableProcessor;
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

    @Override
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        if (this.with != null) {
            context = context.clone();
            tempTableStore = new TempTableStore(context.getConnectionId(), TransactionMode.NONE);
            tempTableStore.setParentTempTableStore(context.getTempTableStore());
            context.setTempTableStore(tempTableStore);
        }
        setContext(context);
        connectExternal(this.root, context, dataMgr, bufferMgr);
    }

    static void connectExternal(RelationalNode node, CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {

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
                if (withCommand.isRecursive()) {
                    SetQuery setQuery = (SetQuery)withCommand.getCommand();
                    ProcessorPlan initial = setQuery.getLeftQuery().getProcessorPlan();
                    QueryProcessor withProcessor = new QueryProcessor(initial, getContext().clone(), root.getBufferManager(), root.getDataManager());
                    processors.put(withCommand.getGroupSymbol().getName(), new RecursiveTableProcessor(withProcessor, withCommand.getColumns(), setQuery.getRightQuery().getProcessorPlan(), setQuery.isAll()));
                    continue;
                }
                ProcessorPlan plan = withCommand.getCommand().getProcessorPlan();
                QueryProcessor withProcessor = new QueryProcessor(plan, getContext().clone(), root.getBufferManager(), root.getDataManager());
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
                    proc.close();
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
                if (withCommand.isRecursive()) {
                    SetQuery setQuery = (SetQuery)withCommand.getCommand();
                    setQuery.getLeftQuery().getProcessorPlan().reset();
                    setQuery.getLeftQuery().getProcessorPlan().reset();
                } else {
                    withCommand.getCommand().getProcessorPlan().reset();
                }
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.with != null) {
            sb.append(SQLConstants.Reserved.WITH);
            for (WithQueryCommand withCommand : this.with) {
                sb.append("\n"); //$NON-NLS-1$
                sb.append(withCommand.getGroupSymbol());
                if (withCommand.isRecursive()) {
                    sb.append(" anchor\n").append(((SetQuery)withCommand.getCommand()).getLeftQuery().getProcessorPlan()); //$NON-NLS-1$
                    sb.append("recursive\n").append(((SetQuery)withCommand.getCommand()).getRightQuery().getProcessorPlan()); //$NON-NLS-1$
                } else {
                    sb.append("\n"); //$NON-NLS-1$
                    sb.append(withCommand.getCommand().getProcessorPlan());
                }
            }
            sb.append("body\n"); //$NON-NLS-1$
        }
        sb.append(this.root.toString());
        return sb.toString();
    }

    public RelationalPlan clone(){
        RelationalPlan plan = new RelationalPlan((RelationalNode)root.clone());
        plan.setOutputElements(outputCols);
        if (with != null) {
            List<WithQueryCommand> newWith = LanguageObject.Util.deepClone(this.with, WithQueryCommand.class);
            for (WithQueryCommand withQueryCommand : newWith) {
                if (withQueryCommand.isRecursive()) {
                    SetQuery setQuery = (SetQuery)withQueryCommand.getCommand();
                    setQuery.getLeftQuery().setProcessorPlan(setQuery.getLeftQuery().getProcessorPlan().clone());
                    setQuery.getRightQuery().setProcessorPlan(setQuery.getRightQuery().getProcessorPlan().clone());
                } else {
                    withQueryCommand.getCommand().setProcessorPlan(withQueryCommand.getCommand().getProcessorPlan().clone());
                }
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
    public Boolean requiresTransaction(boolean transactionalReads) {
        if (this.with != null) {
            for (WithQueryCommand withCommand : this.with) {
                if (withCommand.isRecursive()) {
                    SetQuery setQuery = (SetQuery)withCommand.getCommand();
                    Boolean leftRequires = setQuery.getLeftQuery().getProcessorPlan().requiresTransaction(transactionalReads);
                    Boolean rightRequires = setQuery.getLeftQuery().getProcessorPlan().requiresTransaction(transactionalReads);
                    if (!Boolean.FALSE.equals(leftRequires) || !Boolean.FALSE.equals(rightRequires)) {
                        return true;
                    }
                } else {
                    Boolean requires = withCommand.getCommand().getProcessorPlan().requiresTransaction(transactionalReads);
                    if (!Boolean.FALSE.equals(requires)) {
                        return true;
                    }
                }
            }
        }
        return requiresTransaction(transactionalReads, root);
    }

    static Boolean requiresTransaction(boolean transactionalReads, RelationalNode node) {
        Boolean requiresTxn = node.requiresTransaction(transactionalReads);
        if (Boolean.TRUE.equals(requiresTxn)) {
            return true;
        }
        boolean last = true;
        for (RelationalNode child : node.getChildren()) {
            if (child == null) {
                continue;
            }
            Boolean childRequires = requiresTransaction(transactionalReads, child);
            if (Boolean.TRUE.equals(childRequires)) {
                return true;
            }
            if (childRequires == null) {
                if (requiresTxn == null) {
                    return true;
                }
                requiresTxn = null;
                last = true;
            } else {
                last = false;
            }
        }
        if (requiresTxn == null && !last) {
            return true;
        }
        return requiresTxn;
    }

    @Override
    public TupleBuffer getBuffer(int maxRows) throws BlockedException, TeiidComponentException, TeiidProcessingException {
        return root.getBuffer(maxRows);
    }

    @Override
    public boolean hasBuffer() {
        return root.hasBuffer();
    }

}
