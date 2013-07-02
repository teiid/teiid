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

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.teiid.dqp.internal.process.multisource.MultiSourceElementReplacementVisitor;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.RowBasedSecurityHelper;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.relational.SubqueryAwareEvaluator.SubqueryState;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


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

    // Processing state
	private ArrayList<TupleSource> tupleSources = new ArrayList<TupleSource>();
	private boolean isUpdate = false;
    private boolean returnedRows = false;
    protected Command nextCommand;
    private int reserved;
    private int schemaSize;
    private Command processingCommand;
    private boolean shouldExecute = true;
    
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
        this.shouldEvaluate = shouldEvaluate;
    }

	public void open()
		throws TeiidComponentException, TeiidProcessingException {
		
		//TODO: support a partitioning concept with multi-source and full dependent join pushdown
		if (subPlans != null) {
			if (this.evaluatedPlans == null) {
				this.evaluatedPlans = new HashMap<GroupSymbol, SubqueryState>();
				for (Map.Entry<GroupSymbol, RelationalPlan> entry : subPlans.entrySet()) {
					SubqueryState state = new SubqueryState();
					state.processor = new QueryProcessor(entry.getValue(), getContext().clone(), getBufferManager(), getDataManager());
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
        		PreOrPostOrderNavigator.doVisit(processingCommand, new MultiSourceElementReplacementVisitor(replacement, getContext().getMetadata()), PreOrPostOrderNavigator.PRE_ORDER, false);
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
			//We hardcode an upper limit on currency because these commands have potentially large in-memory value sets
        } while (!processCommandsIndividually() && hasNextCommand() && this.tupleSources.size() < Math.min(MAX_CONCURRENT, this.getContext().getUserRequestSourceConcurrency()));
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
			QueryRewriter.evaluateAndRewrite(atomicCommand, eval, context, metadata);
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
		if (this.subPlans != null) {
			target.subPlans = new HashMap<GroupSymbol, RelationalPlan>();
			for (Map.Entry<GroupSymbol, RelationalPlan> entry : this.subPlans.entrySet()) {
				target.subPlans.put(entry.getKey(), entry.getValue().clone());
			}
		}
	}

    public synchronized PlanNode getDescriptionProperties() {
    	if (getChildCount() > 0) {
    		return this.getChildren()[0].getDescriptionProperties();
    	}
    	PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_SQL, this.command.toString());
        props.addProperty(PROP_MODEL_NAME, this.modelName);
        if (this.projection != null && this.projection.length > 0 && this.originalSelect != null) {
        	props.addProperty(PROP_SELECT_COLS, this.originalSelect.toString());
        }
        if (this.info != null) {
        	props.addProperty(PROP_SHARING_ID, String.valueOf(this.info.id));
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
	protected Collection<? extends LanguageObject> getObjects() {
		ArrayList<LanguageObject> list = new ArrayList<LanguageObject>();
		if (shouldEvaluate) {
			//collect any evaluatable subqueries
			for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(this.command)) {
				if (container instanceof ExistsCriteria && ((ExistsCriteria)container).shouldEvaluate()) {
					list.add(container);
				}
				if (container instanceof ScalarSubquery && ((ScalarSubquery)container).shouldEvaluate()) {
					list.add(container);
				}
			}
		}
		return list;
	}
	
	@Override
	public Boolean requiresTransaction(boolean transactionalReads) {
		Boolean required = super.requiresTransaction(transactionalReads);
		if (Boolean.TRUE.equals(required) || (command instanceof StoredProcedure && ((StoredProcedure)command).getUpdateCount() > 1)) {
			return true;
		}
		if (multiSource && connectorBindingExpression == null && (transactionalReads || RelationalNodeUtil.isUpdate(command))) {
			return true;
		}
		return null;
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
                	PreOrPostOrderNavigator.doVisit(command, new MultiSourceElementReplacementVisitor(sourceName, metadata), PreOrPostOrderNavigator.PRE_ORDER, false);
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
	
}