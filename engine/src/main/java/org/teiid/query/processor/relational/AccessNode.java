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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.*;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.multisource.MultiSourceElementReplacementVisitor;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.RowBasedSecurityHelper;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.relational.SubqueryAwareEvaluator.SubqueryState;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryContainer.Evaluatable;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.TransactionSupport;


public class AccessNode extends SubqueryAwareRelationalNode {

    private static final Object[] NO_PROJECTION = new Object[0];
    private static final int MAX_CONCURRENT = 10; //TODO: this could be settable via a property
    // Initialization state
    private Command command;
    private String modelName;
    private String connectorBindingId;
    private Expression connectorBindingExpression;
    private boolean shouldEvaluate = false;
    private boolean multiSource;
    private Object modelId;
    private Set<Object> conformedTo;
    private TransactionSupport transactionSupport;

    // Processing state
    private ArrayList<TupleSource> tupleSources = new ArrayList<TupleSource>();
    private boolean isUpdate = false;
    private boolean returnedRows = false;
    protected Command nextCommand;
    private int reserved;
    private int schemaSize;
    private Command processingCommand;
    private boolean shouldExecute = true;
    private boolean open;

    private Object[] projection;
    private List<Expression> originalSelect;

    private List<String> sourceNames;

    public RegisterRequestParameter.SharedAccessInfo info;
    private Map<GroupSymbol, RelationalPlan> subPlans;
    private Map<GroupSymbol, SubqueryState> evaluatedPlans;

    protected AccessNode() {
        super();
    }

    public AccessNode(int nodeID) {
        super(nodeID);
    }

    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
            ProcessorDataManager dataMgr) {
        super.initialize(context, bufferManager, dataMgr);
        this.schemaSize = getBufferManager().getSchemaSize(getOutputElements());
    }

    public void reset() {
        super.reset();
        this.tupleSources.clear();
        isUpdate = false;
        returnedRows = false;
        nextCommand = null;
        if (connectorBindingExpression != null) {
            connectorBindingId = null;
        }
        processingCommand = null;
        shouldExecute = true;
        this.evaluatedPlans = null;
        open = false;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public Command getCommand() {
        return this.command;
    }

    public void setModelId(Object id) {
        this.modelId = id;
    }

    public Object getModelId() {
        return this.modelId;
    }

    public void setModelName(String name) {
        this.modelName = name;
    }

    public String getModelName() {
        return this.modelName;
    }

    public void setShouldEvaluateExpressions(boolean shouldEvaluate) {
        if (shouldEvaluate && projection != null) {
            //restore the original as we'll minimize each time
            ((Query)this.command).getSelect().setSymbols(this.originalSelect);
            this.projection = null;
        }
        this.shouldEvaluate = shouldEvaluate;
    }

    @Override
    public void open() throws TeiidComponentException, TeiidProcessingException {
        try {
            openInternal();
            open = true;
        } catch (BlockedException e) {
            //we don't want to let blocked exceptions bubble up during open
        }
    }

    private void openInternal()
        throws TeiidComponentException, TeiidProcessingException {

        //TODO: support a partitioning concept with multi-source and full dependent join pushdown
        if (subPlans != null) {
            if (this.evaluatedPlans == null) {
                this.evaluatedPlans = new HashMap<GroupSymbol, SubqueryState>();
                for (Map.Entry<GroupSymbol, RelationalPlan> entry : subPlans.entrySet()) {
                    SubqueryState state = new SubqueryState();
                    RelationalPlan value = entry.getValue();
                    value.reset();
                    state.processor = new QueryProcessor(value, getContext().clone(), getBufferManager(), getDataManager());
                    state.collector = state.processor.createBatchCollector();
                    this.evaluatedPlans.put(entry.getKey(), state);
                }
            }
            BlockedException be = null;
            for (SubqueryState state : evaluatedPlans.values()) {
                try {
                    state.collector.collectTuples();
                } catch (BlockedException e) {
                    be = e;
                }
            }
            if (be != null) {
                throw be;
            }
        }

        /*
         * Check to see if we need a multi-source expansion.  If the connectorBindingExpression != null, then
         * the logic below will handle that case
         */
        if (multiSource && connectorBindingExpression == null) {
            synchronized (this) { //the description can be obtained asynchly, so we need to synchronize
                VDBMetaData vdb = getContext().getVdb();
                ModelMetaData model = vdb.getModel(getModelName());
                List<String> sources = model.getSourceNames();
                //make sure that we have the right nodes
                if (this.getChildCount() != 0 && (this.sourceNames == null || !this.sourceNames.equals(sources))) {
                    this.childCount--;
                    this.getChildren()[0] = null;
                }
                if (this.getChildCount() == 0) {
                    sourceNames = sources;
                    RelationalNode node = multiSourceModify(this, connectorBindingExpression, getContext().getMetadata(), sourceNames);
                    RelationalPlan.connectExternal(node, getContext(), getDataManager(), getBufferManager());
                    this.addChild(node);
                }
            }
            this.getChildren()[0].open();
            return;
        }

        // Copy command and resolve references if necessary
        if (processingCommand == null) {
            processingCommand = command;
            isUpdate = RelationalNodeUtil.isUpdate(command);
        }
        boolean needProcessing = true;

        if (this.connectorBindingExpression != null && connectorBindingId == null) {
            this.connectorBindingId = (String) getEvaluator(Collections.emptyMap()).evaluate(this.connectorBindingExpression, null);
            VDBMetaData vdb = getContext().getVdb();
            ModelMetaData model = vdb.getModel(getModelName());
            List<String> sources = model.getSourceNames();
            String replacement = this.connectorBindingId;
            if (!sources.contains(this.connectorBindingId)) {
                shouldExecute = false;
                if (command instanceof StoredProcedure) {
                    StoredProcedure sp = (StoredProcedure)command;
                    if (sp.returnParameters() && sp.getProjectedSymbols().size() > sp.getResultSetColumns().size()) {
                        throw new TeiidProcessingException(QueryPlugin.Event.TEIID30561, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30561, command));
                    }
                }
                return;
            }
            if (!(command instanceof StoredProcedure || command instanceof Insert)) {
                processingCommand = (Command) command.clone();
                MultiSourceElementReplacementVisitor.visit(replacement, getContext().getMetadata(), processingCommand);
            }
        }

        do {
            Command atomicCommand = nextCommand();
            if(shouldEvaluate) {
                needProcessing = prepareNextCommand(atomicCommand);
                nextCommand = null;
            } else {
                needProcessing = RelationalNodeUtil.shouldExecute(atomicCommand, true);
            }
            if(needProcessing) {
                registerRequest(atomicCommand);
            }
            //We use an upper limit here to the currency because these commands have potentially large in-memory value sets
        } while (!processCommandsIndividually() && hasNextCommand() && this.tupleSources.size() < Math.max(Math.min(MAX_CONCURRENT, this.getContext().getUserRequestSourceConcurrency()), this.getContext().getUserRequestSourceConcurrency()/2));
    }

    public boolean isShouldEvaluate() {
        return shouldEvaluate;
    }

    public void minimizeProject(Command atomicCommand) {
        if (!(atomicCommand instanceof Query)) {
            return;
        }
        Query query = (Query)atomicCommand;
        Select select = query.getSelect();
        List<Expression> symbols = select.getSymbols();
        if (symbols.size() == 1) {
            return;
        }
        boolean shouldProject = false;
        LinkedHashMap<Expression, Integer> uniqueSymbols = new LinkedHashMap<Expression, Integer>();
        projection = new Object[symbols.size()];
        this.originalSelect = new ArrayList<Expression>(query.getSelect().getSymbols());
        int i = 0;
        int j = 0;
        for (Iterator<Expression> iter = symbols.iterator(); iter.hasNext(); ) {
            Expression ss = iter.next();
            Expression ex = SymbolMap.getExpression(ss);
            if (ex instanceof Constant) {
                projection[i] = ex;
                if (iter.hasNext() || j!=0) {
                    iter.remove();
                    shouldProject = true;
                } else {
                    projection[i] = j++;
                }
            } else {
                Integer index = uniqueSymbols.get(ex);
                if (index == null) {
                    uniqueSymbols.put(ex, j);
                    index = j++;
                } else {
                    iter.remove();
                    shouldProject = true;
                }
                projection[i] = index;
            }
            i++;
        }
        if (!shouldProject) {
            this.projection = NO_PROJECTION;
        } else if (query.getOrderBy() != null) {
            for (OrderByItem item : query.getOrderBy().getOrderByItems()) {
                Integer index = uniqueSymbols.get(SymbolMap.getExpression(item.getSymbol()));
                if (index != null) {
                    item.setExpressionPosition(index);
                    item.setSymbol(select.getSymbols().get(index));
                }
            }
        }
    }

    public List<Expression> getOriginalSelect() {
        return originalSelect;
    }

    public Object[] getProjection() {
        return projection;
    }

    static void rewriteAndEvaluate(Command atomicCommand, Evaluator eval, CommandContext context, QueryMetadataInterface metadata)
            throws TeiidProcessingException, TeiidComponentException {
        try {
            // Defect 16059 - Rewrite the command to replace references, etc. with values.
            Command cmd = QueryRewriter.evaluateAndRewrite(atomicCommand, eval, context, metadata);
            //we don't expect the command to change at this point
            Assertion.assertTrue(cmd == atomicCommand);
        } catch (QueryValidatorException e) {
             throw new TeiidProcessingException(QueryPlugin.Event.TEIID30174, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30174, atomicCommand));
        }
    }

    @SuppressWarnings("unused")
    protected Command nextCommand() throws TeiidProcessingException, TeiidComponentException {
        //it's important to save the next command
        //to ensure that the subquery ids remain stable
        if (nextCommand == null) {
            nextCommand = (Command) processingCommand.clone();
            if (evaluatedPlans != null) {
                for (WithQueryCommand with : ((QueryCommand)nextCommand).getWith()) {
                    TupleBuffer tb = evaluatedPlans.get(with.getGroupSymbol()).collector.getTupleBuffer();
                    with.setTupleBuffer(tb);
                }
            }
        }
        return nextCommand;
    }

    protected boolean prepareNextCommand(Command atomicCommand) throws TeiidComponentException, TeiidProcessingException {
        rewriteAndEvaluate(atomicCommand, getEvaluator(Collections.emptyMap()), this.getContext(), this.getContext().getMetadata());
        return RelationalNodeUtil.shouldExecute(atomicCommand, true);
    }

    public TupleBatch nextBatchDirect()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if (!open) {
            openInternal(); //-- blocked during actual open
            open = true;
        }

        if (multiSource && connectorBindingExpression == null) {
            return this.getChildren()[0].nextBatch();
        }

        while (shouldExecute && (!tupleSources.isEmpty() || hasNextCommand())) {

            if (tupleSources.isEmpty() && processCommandsIndividually()) {
                registerNext();
            }

            //drain the tuple source(s)
            for (int i = 0; i < this.tupleSources.size(); i++) {
                TupleSource tupleSource = tupleSources.get(i);
                try {
                    List<?> tuple = null;

                    while ((tuple = tupleSource.nextTuple()) != null) {
                        returnedRows = true;
                        if (this.projection != null && this.projection.length > 0) {
                            List<Object> newTuple = new ArrayList<Object>(this.projection.length);
                            for (Object object : this.projection) {
                                if (object instanceof Integer) {
                                    newTuple.add(tuple.get((Integer)object));
                                } else {
                                    newTuple.add(((Constant)object).getValue());
                                }
                            }
                            tuple = newTuple;
                        }
                        addBatchRow(tuple);

                        if (isBatchFull()) {
                            return pullBatch();
                        }
                    }

                    //end of source
                    tupleSource.closeSource();
                    tupleSources.remove(i--);
                    if (reserved > 0) {
                        reserved -= schemaSize;
                        getBufferManager().releaseBuffers(schemaSize);
                    }
                    if (!processCommandsIndividually()) {
                        registerNext();
                    }
                    continue;
                } catch (BlockedException e) {
                    if (processCommandsIndividually()) {
                        if (hasPendingRows()) {
                            return pullBatch();
                        }
                        throw e;
                    }
                    continue;
                }
            }

            if (processCommandsIndividually()) {
                if (hasPendingRows()) {
                    return pullBatch();
                }
                continue;
            }

            if (!this.tupleSources.isEmpty()) {
                if (hasPendingRows()) {
                    return pullBatch();
                }
                throw BlockedException.block(getContext().getRequestId(), "Blocking on source request(s)."); //$NON-NLS-1$
            }
        }

        if(isUpdate && !returnedRows) {
            List<Integer> tuple = new ArrayList<Integer>(1);
            tuple.add(Integer.valueOf(0));
            // Add tuple to current batch
            addBatchRow(tuple);
        }
        terminateBatches();
        return pullBatch();
    }

    @Override
    protected void addBatchRow(List<?> row) {
        if (this.getOutputElements().isEmpty()) {
            //a dummy column was added to the query, just remove it now
            row = Collections.emptyList();
        }
        super.addBatchRow(row);
    }

    private void registerNext() throws TeiidComponentException,
            TeiidProcessingException {
        while (hasNextCommand()) {
            Command atomicCommand = nextCommand();
            if (prepareNextCommand(atomicCommand)) {
                nextCommand = null;
                registerRequest(atomicCommand);
                break;
            }
            nextCommand = null;
        }
    }

    private void registerRequest(Command atomicCommand)
            throws TeiidComponentException, TeiidProcessingException {
        if (shouldEvaluate) {
            projection = null;
            minimizeProject(atomicCommand);
        }
        int limit = -1;
        if (getParent() instanceof LimitNode) {
            LimitNode parent = (LimitNode)getParent();
            if (parent.getLimit() > 0) {
                limit = parent.getLimit() + parent.getOffset();
            }
        }
        RegisterRequestParameter param = new RegisterRequestParameter(connectorBindingId, getID(), limit);
        param.info = info;
        param.fetchSize = this.getBatchSize();
        RowBasedSecurityHelper.checkConstraints(atomicCommand, getEvaluator(Collections.emptyMap()));
        tupleSources.add(getDataManager().registerRequest(getContext(), atomicCommand, modelName, param));
        if (tupleSources.size() > 1) {
            reserved += getBufferManager().reserveBuffers(schemaSize, BufferReserveMode.FORCE);
        }
    }

    protected boolean processCommandsIndividually() {
        return false;
    }

    protected boolean hasNextCommand() {
        return false;
    }

    public void closeDirect() {
        if (reserved > 0) {
            getBufferManager().releaseBuffers(reserved);
            reserved = 0;
        }
        if (this.evaluatedPlans != null) {
            for (SubqueryState state : this.evaluatedPlans.values()) {
                state.close(true);
            }
            this.evaluatedPlans = null;
        }
        super.closeDirect();
        closeSources();
    }

    private void closeSources() {
        for (TupleSource ts : this.tupleSources) {
            ts.closeSource();
        }
        this.tupleSources.clear();
    }

    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
        str.append(command);
        if (this.info != null) {
            str.append(" [SHARED ").append(this.info.id).append("]"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public Object clone(){
        AccessNode clonedNode = new AccessNode();
        this.copyTo(clonedNode);
        return clonedNode;
    }

    protected void copyTo(AccessNode target){
        super.copyTo(target);
        target.modelName = modelName;
        target.modelId = modelId;
        if (this.connectorBindingExpression == null) {
            target.connectorBindingId = this.connectorBindingId;
        }
        target.shouldEvaluate = shouldEvaluate;
        if (!shouldEvaluate) {
            target.projection = projection;
            target.originalSelect = originalSelect;
        }
        target.command = command;
        target.info = info;
        target.connectorBindingExpression = this.connectorBindingExpression;
        target.multiSource = multiSource;
        target.sourceNames = sourceNames;
        target.conformedTo = this.conformedTo;
        if (this.subPlans != null) {
            target.subPlans = new HashMap<GroupSymbol, RelationalPlan>();
            for (Map.Entry<GroupSymbol, RelationalPlan> entry : this.subPlans.entrySet()) {
                target.subPlans.put(entry.getKey(), entry.getValue().clone());
            }
        }
        target.transactionSupport = transactionSupport;
    }

    public synchronized PlanNode getDescriptionProperties() {
        if (getChildCount() > 0) {
            return this.getChildren()[0].getDescriptionProperties();
        }
        PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_SQL, this.command.toString());
        props.addProperty(PROP_MODEL_NAME, this.modelName);
        Collection<? extends SubqueryContainer<?>> objects = getObjects();
        if (!objects.isEmpty()) {
            int index = 0;
            for (Iterator<? extends SubqueryContainer<?>> iterator = objects.iterator(); iterator.hasNext();) {
                SubqueryContainer<?> subqueryContainer = iterator.next();
                props.addProperty(PROP_SQL + " Subplan " + index++, subqueryContainer.getCommand().getProcessorPlan().getDescriptionProperties()); //$NON-NLS-1$
            }
        }
        if (this.projection != null && this.projection.length > 0 && this.originalSelect != null) {
            props.addProperty(PROP_SELECT_COLS, this.originalSelect.toString());
        }
        if (this.info != null) {
            props.addProperty(PROP_SHARING_ID, String.valueOf(this.info.id));
        }
        if (this.subPlans != null) {
            for (Map.Entry<GroupSymbol, RelationalPlan> entry : this.subPlans.entrySet()) {
                props.addProperty(entry.getKey() + " Dependent Subplan", entry.getValue().getDescriptionProperties()); //$NON-NLS-1$
            }
        }
        return props;
    }

    public String getConnectorBindingId() {
        return connectorBindingId;
    }

    public void setConnectorBindingId(String connectorBindingId) {
        this.connectorBindingId = connectorBindingId;
    }

    public Expression getConnectorBindingExpression() {
        return connectorBindingExpression;
    }

    public void setConnectorBindingExpression(
            Expression connectorBindingExpression) {
        this.connectorBindingExpression = connectorBindingExpression;
    }

    @Override
    public Collection<? extends SubqueryContainer<?>> getObjects() {
        ArrayList<SubqueryContainer<?>> list = new ArrayList<SubqueryContainer<?>>();
        if (shouldEvaluate) {
            //collect any evaluatable subqueries
            collectEvaluatable(list, this.command);
        }
        return list;
    }

    private void collectEvaluatable(ArrayList<SubqueryContainer<?>> list, Command cmd) {
        for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(cmd)) {
            if (container instanceof Evaluatable<?> && ((Evaluatable<?>)container).shouldEvaluate()) {
                list.add(container);
            } else {
                collectEvaluatable(list, container.getCommand());
            }
        }
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        int subqueryTxn = 0;
        for (SubqueryContainer<?> subquery : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(getObjects())) {
            if (!(subquery instanceof SubqueryContainer.Evaluatable) || !((SubqueryContainer.Evaluatable<?>)subquery).shouldEvaluate()) {
                continue;
            }
            ProcessorPlan plan = subquery.getCommand().getProcessorPlan();
            if (plan != null) {
                Boolean txn = plan.requiresTransaction(transactionalReads);
                if (txn != null) {
                    if (txn) {
                        return true;
                    }
                } else {
                    subqueryTxn++;
                }
            }
        }
        if (subqueryTxn > 1) {
            return true;
        }
        if ((multiSource && connectorBindingExpression == null) && (subqueryTxn > 0)) {
            return true;
        }
        if (transactionSupport == TransactionSupport.NONE) {
            return subqueryTxn > 0?null:false;
        }
        if ((command instanceof StoredProcedure && ((StoredProcedure)command).getUpdateCount() > 1)) {
            return true;
        }
        if (transactionalReads || RelationalNodeUtil.isUpdate(command) || (command instanceof StoredProcedure && !((StoredProcedure)command).isReadOnly())) {
            if ((multiSource && connectorBindingExpression == null)) {
                return true;
            }
            return subqueryTxn > 0?true:null;
        }
        return false;
    }

    private static RelationalNode multiSourceModify(AccessNode accessNode, Expression ex, QueryMetadataInterface metadata, List<String> sourceNames) throws TeiidComponentException, TeiidProcessingException {
        List<AccessNode> accessNodes = new ArrayList<AccessNode>();

        boolean hasOutParams = RelationalNodeUtil.hasOutputParams(accessNode.getCommand());
        if (!Constant.NULL_CONSTANT.equals(ex)) {
            for(String sourceName:sourceNames) {
                Command command = accessNode.getCommand();
                // Modify the command to pull the instance column and evaluate the criteria
                if (!(command instanceof Insert || command instanceof StoredProcedure)) {
                    command = (Command)command.clone();
                    MultiSourceElementReplacementVisitor.visit(sourceName, metadata, command);
                    if (!RelationalNodeUtil.shouldExecute(command, false, true)) {
                        continue;
                    }
                }

                // Create a new cloned version of the access node and set it's model name to be the bindingUUID
                AccessNode instanceNode = (AccessNode) accessNode.clone();
                instanceNode.setMultiSource(false);
                instanceNode.setCommand(command);
                accessNodes.add(instanceNode);

                if (accessNodes.size() > 1 && command instanceof Insert) {
                    throw new AssertionError("Multi-source insert must target a single source.  Should have been caught in validation"); //$NON-NLS-1$
                }

                instanceNode.setConnectorBindingId(sourceName);
            }
        }

        if (hasOutParams && accessNodes.size() != 1) {
            throw new QueryProcessingException(QueryPlugin.Event.TEIID30561, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30561, accessNode.getCommand()));
        }

        switch(accessNodes.size()) {
            case 0:
            {
                if (RelationalNodeUtil.isUpdate(accessNode.getCommand())) {
                    //should return a 0 update count
                    ProjectNode pnode = new ProjectNode(accessNode.getID());
                    pnode.setSelectSymbols(Arrays.asList(new Constant(0)));
                    return pnode;
                }
                // Replace existing access node with a NullNode
                NullNode nullNode = new NullNode(accessNode.getID());
                return nullNode;
            }
            case 1:
            {
                // Replace existing access node with new access node (simplified command)
                return accessNodes.get(0);
            }
            default:
            {
                UnionAllNode unionNode = new UnionAllNode(accessNode.getID());
                unionNode.setElements(accessNode.getElements());
                for (AccessNode newNode : accessNodes) {
                    unionNode.addChild(newNode);
                }

                RelationalNode parent = unionNode;

                // More than 1 access node - replace with a union
                if (RelationalNodeUtil.isUpdate(accessNode.getCommand())) {
                    GroupingNode groupNode = new GroupingNode(accessNode.getID());
                    AggregateSymbol sumCount = new AggregateSymbol(NonReserved.SUM, false, accessNode.getElements().get(0));
                    groupNode.setElements(Arrays.asList(sumCount));
                    groupNode.addChild(unionNode);

                    ProjectNode projectNode = new ProjectNode(accessNode.getID());

                    Expression intSum = ResolverUtil.getConversion(sumCount, DataTypeManager.getDataTypeName(sumCount.getType()), DataTypeManager.DefaultDataTypes.INTEGER, false, metadata.getFunctionLibrary());

                    List<Expression> outputElements = Arrays.asList(intSum);
                    projectNode.setElements(outputElements);
                    projectNode.setSelectSymbols(outputElements);
                    projectNode.addChild(groupNode);

                    parent = projectNode;
                }
                return parent;
            }
        }
    }

    public void setMultiSource(boolean ex) {
        this.multiSource = ex;
    }

    public void setSubPlans(Map<GroupSymbol, RelationalPlan> plans) {
        this.subPlans = plans;
    }

    public Map<GroupSymbol, RelationalPlan> getSubPlans() {
        return subPlans;
    }

    public Set<Object> getConformedTo() {
        return conformedTo;
    }

    public void setConformedTo(Set<Object> conformedTo) {
        this.conformedTo = conformedTo;
    }

    public void setTransactionSupport(TransactionSupport transactionSupport) {
        this.transactionSupport = transactionSupport;
    }

}