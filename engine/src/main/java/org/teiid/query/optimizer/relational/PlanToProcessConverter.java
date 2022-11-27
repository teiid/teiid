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

package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.util.Assertion;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.source.XMLHelper;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.optimizer.relational.rules.RuleAssignOutputElements;
import org.teiid.query.optimizer.relational.rules.RuleChooseJoinStrategy;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.ArrayTableNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.DependentProcedureAccessNode;
import org.teiid.query.processor.relational.DependentProcedureExecutionNode;
import org.teiid.query.processor.relational.DupRemoveNode;
import org.teiid.query.processor.relational.EnhancedSortMergeJoinStrategy;
import org.teiid.query.processor.relational.GroupingNode;
import org.teiid.query.processor.relational.InsertPlanExecutionNode;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.MergeJoinStrategy;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.NestedLoopJoinStrategy;
import org.teiid.query.processor.relational.NestedTableJoinStrategy;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.ObjectTableNode;
import org.teiid.query.processor.relational.PlanExecutionNode;
import org.teiid.query.processor.relational.ProjectIntoNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.SelectNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.processor.relational.TextTableNode;
import org.teiid.query.processor.relational.UnionAllNode;
import org.teiid.query.processor.relational.WindowFunctionProjectNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.FilteredCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.lang.ObjectTable.ObjectColumn;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.SourceHint.SpecificHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor.EvaluationLevel;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.TransactionSupport;


public class PlanToProcessConverter {
    protected QueryMetadataInterface metadata;
    private IDGenerator idGenerator;
    private AnalysisRecord analysisRecord;
    private CapabilitiesFinder capFinder;

    //state for detecting and reusing source queries
    private Map<Command, AccessNode> sharedCommands = new HashMap<Command, AccessNode>();
    private CommandContext context;
    private static AtomicInteger sharedId = new AtomicInteger();

    public static class SharedStateKey {
        int id;
        int expectedReaders;
    }

    public PlanToProcessConverter(QueryMetadataInterface metadata, IDGenerator idGenerator, AnalysisRecord analysisRecord, CapabilitiesFinder capFinder, CommandContext context) {
        this.metadata = metadata;
        this.idGenerator = idGenerator;
        this.analysisRecord = analysisRecord;
        this.capFinder = capFinder;
        this.context = context;
    }

    public RelationalPlan convert(PlanNode planNode)
        throws QueryPlannerException, TeiidComponentException {
        try {
            boolean debug = analysisRecord.recordDebug();
            if(debug) {
                analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
                analysisRecord.println("CONVERTING PLAN TREE TO PROCESS TREE"); //$NON-NLS-1$
            }

            // Convert plan tree nodes into process tree nodes
            RelationalNode processNode;
            try {
                processNode = convertPlan(planNode);
            } catch (TeiidProcessingException e) {
                if (e instanceof QueryPlannerException) {
                    throw (QueryPlannerException)e;
                }
                throw new QueryPlannerException(e);
            }
            if(debug) {
                analysisRecord.println("\nPROCESS PLAN = \n" + processNode); //$NON-NLS-1$
                analysisRecord.println("============================================================================"); //$NON-NLS-1$
            }

            RelationalPlan processPlan = new RelationalPlan(processNode);
            return processPlan;
        } finally {
            sharedCommands.clear();
        }
    }

    private RelationalNode convertPlan(PlanNode planNode)
        throws TeiidComponentException, TeiidProcessingException {

        // Convert current node in planTree
        RelationalNode convertedNode = convertNode(planNode);

        if(convertedNode == null) {
            Assertion.assertTrue(planNode.getChildCount() == 1);
            return convertPlan(planNode.getFirstChild());
        }

        RelationalNode nextParent = convertedNode;

        // convertedNode may be the head of 1 or more nodes   - go to end of chain
        while(nextParent.getChildren()[0] != null) {
            nextParent = nextParent.getChildren()[0];
        }

        // Call convertPlan recursively on children
        for (PlanNode childNode : planNode.getChildren()) {
            RelationalNode child = convertPlan(childNode);
            if (planNode.getType() == NodeConstants.Types.SET_OP && nextParent instanceof UnionAllNode
                    && childNode.getProperty(Info.SET_OPERATION) == planNode.getProperty(Info.SET_OPERATION)
                    && childNode.getType() == NodeConstants.Types.SET_OP && childNode.hasBooleanProperty(Info.USE_ALL)) {
                for (RelationalNode grandChild : child.getChildren()) {
                    if (grandChild != null) {
                        nextParent.addChild(grandChild);
                    }
                }
            } else {
                nextParent.addChild(child);
            }
        }

        // Return root of tree for top node
        return convertedNode;
    }

    protected int getID() {
        return idGenerator.nextInt();
    }

    protected RelationalNode convertNode(PlanNode node)
        throws TeiidComponentException, TeiidProcessingException {

        RelationalNode processNode = null;

        switch(node.getType()) {
            case NodeConstants.Types.PROJECT:
                GroupSymbol intoGroup = (GroupSymbol) node.getProperty(NodeConstants.Info.INTO_GROUP);
                if(intoGroup != null) {
                    try {
                        Insert insert = (Insert)node.getFirstChild().getProperty(Info.VIRTUAL_COMMAND);
                        List<ElementSymbol> allIntoElements = insert.getVariables();

                        Object groupID = intoGroup.getMetadataID();
                        Object modelID = metadata.getModelID(groupID);
                        String modelName = metadata.getFullName(modelID);
                        if (metadata.isVirtualGroup(groupID) && !metadata.isTemporaryTable(groupID)) {
                            InsertPlanExecutionNode ipen = new InsertPlanExecutionNode(getID(), metadata);
                            ProcessorPlan plan = (ProcessorPlan)node.getFirstChild().getProperty(Info.PROCESSOR_PLAN);
                            Assertion.isNotNull(plan);
                            ipen.setProcessorPlan(plan);
                            ipen.setReferences(insert.getValues());
                            processNode = ipen;
                        } else {
                            ProjectIntoNode pinode = new ProjectIntoNode(getID());
                            pinode.setIntoGroup(intoGroup);
                            pinode.setIntoElements(allIntoElements);
                            pinode.setModelName(modelName);
                            pinode.setConstraint((Criteria) node.getProperty(Info.CONSTRAINT));
                            pinode.setSourceHint((SourceHint) node.getProperty(Info.SOURCE_HINT));
                            if (node.hasBooleanProperty(Info.UPSERT)) {
                                pinode.setUpsert(true);
                            }
                            processNode = pinode;
                            SourceCapabilities caps = capFinder.findCapabilities(modelName);
                            if (caps.supportsCapability(Capability.INSERT_WITH_ITERATOR)) {
                                pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.ITERATOR);
                            } else if (caps.supportsCapability(Capability.BATCHED_UPDATES)) {
                                pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.BATCH);
                            } else {
                                pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.SINGLE);
                            }
                            pinode.setTransactionSupport((TransactionSupport) caps.getSourceProperty(Capability.TRANSACTION_SUPPORT));
                        }
                    } catch(QueryMetadataException e) {
                         throw new TeiidComponentException(QueryPlugin.Event.TEIID30247, e);
                    }

                } else {
                    List<Expression> symbols = (List) node.getProperty(NodeConstants.Info.PROJECT_COLS);

                    ProjectNode pnode = new ProjectNode(getID());
                    pnode.setSelectSymbols(symbols);
                    processNode = pnode;

                    if (node.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
                        WindowFunctionProjectNode wfpn = new WindowFunctionProjectNode(getID());

                        //with partial projection the window function may already be pushed, we'll check for that here
                        ArrayList<Expression> filtered = new ArrayList<Expression>();
                        List<Expression> childSymbols = (List) node.getFirstChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
                        for (Expression ex : symbols) {
                            ex = SymbolMap.getExpression(ex);
                            if (childSymbols.contains(ex)) {
                                continue;
                            }
                            filtered.add(ex);
                        }
                        Set<WindowFunction> windowFunctions = RuleAssignOutputElements.getWindowFunctions(filtered);
                        if (!windowFunctions.isEmpty()) {
                            //TODO: check for selecting all window functions
                            List<Expression> outputElements = new ArrayList<Expression>(windowFunctions);
                            //collect the other projected expressions
                            for (Expression singleElementSymbol : (List<Expression>)node.getFirstChild().getProperty(Info.OUTPUT_COLS)) {
                                outputElements.add(singleElementSymbol);
                            }
                            wfpn.setElements(outputElements);
                            wfpn.init();
                            pnode.addChild(wfpn);
                            for (WindowFunction wf : windowFunctions) {
                                validateAggregateFunctionEvaluation(wf.getFunction());
                            }
                        }
                    }
                }
                break;

            case NodeConstants.Types.JOIN:
                JoinType jtype = (JoinType) node.getProperty(NodeConstants.Info.JOIN_TYPE);
                JoinStrategyType stype = (JoinStrategyType) node.getProperty(NodeConstants.Info.JOIN_STRATEGY);

                JoinNode jnode = new JoinNode(getID());
                jnode.setJoinType(jtype);
                jnode.setLeftDistinct(node.hasBooleanProperty(NodeConstants.Info.IS_LEFT_DISTINCT));
                jnode.setRightDistinct(node.hasBooleanProperty(NodeConstants.Info.IS_RIGHT_DISTINCT));
                List joinCrits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                String depValueSource = (String) node.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE);
                SortOption leftSort = (SortOption)node.getProperty(NodeConstants.Info.SORT_LEFT);
                if(stype == JoinStrategyType.MERGE || stype == JoinStrategyType.ENHANCED_SORT) {
                    MergeJoinStrategy mjStrategy = null;
                    if (stype.equals(JoinStrategyType.ENHANCED_SORT)) {
                        EnhancedSortMergeJoinStrategy esmjStrategy = new EnhancedSortMergeJoinStrategy(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT));
                        esmjStrategy.setSemiDep(node.hasBooleanProperty(Info.IS_SEMI_DEP));
                        mjStrategy = esmjStrategy;
                    } else {
                        mjStrategy = new MergeJoinStrategy(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT), false);
                    }
                    if (node.hasBooleanProperty(Info.SINGLE_MATCH)) {
                        Assertion.assertTrue(jtype == JoinType.JOIN_LEFT_OUTER);
                        mjStrategy.singleMatch(true);
                    }
                    jnode.setJoinStrategy(mjStrategy);
                    List leftExpressions = (List) node.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
                    List rightExpressions = (List) node.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
                    jnode.setJoinExpressions(leftExpressions, rightExpressions);
                    joinCrits = (List) node.getProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA);
                } else if (stype == JoinStrategyType.NESTED_TABLE) {
                    NestedTableJoinStrategy ntjStrategy = new NestedTableJoinStrategy();
                    jnode.setJoinStrategy(ntjStrategy);
                    SymbolMap references = (SymbolMap)node.getProperty(Info.RIGHT_NESTED_REFERENCES);
                    ntjStrategy.setRightMap(references);
                } else {
                    NestedLoopJoinStrategy nljStrategy = new NestedLoopJoinStrategy();
                    jnode.setJoinStrategy(nljStrategy);
                }
                Criteria joinCrit = Criteria.combineCriteria(joinCrits);
                jnode.setJoinCriteria(joinCrit);

                processNode = jnode;

                jnode.setDependentValueSource(depValueSource);

                break;

            case NodeConstants.Types.ACCESS:
                ProcessorPlan plan = (ProcessorPlan) node.getProperty(NodeConstants.Info.PROCESSOR_PLAN);
                if(plan != null) {

                    PlanExecutionNode peNode = null;

                    Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.PROCEDURE_CRITERIA);

                    if (crit != null) {
                        List references = (List)node.getProperty(NodeConstants.Info.PROCEDURE_INPUTS);
                        List defaults = (List)node.getProperty(NodeConstants.Info.PROCEDURE_DEFAULTS);

                        peNode = new DependentProcedureExecutionNode(getID(), crit, references, defaults);
                    } else {
                        peNode = new PlanExecutionNode(getID());
                    }

                    peNode.setProcessorPlan(plan);
                    processNode = peNode;

                } else {
                    AccessNode aNode = null;
                    Command command = (Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
                    Object modelID = node.getProperty(NodeConstants.Info.MODEL_ID);
                    if (modelID != null) {
                        //TODO: we ideally want to handle the partial results case here differently
                        //      by adding a null node / and a source warning
                        //      for now it's just as easy to say that the user needs to take steps to
                        //      return static capabilities
                        checkForValidCapabilities(modelID, metadata, capFinder);
                    }
                    EvaluatableVisitor ev = null;
                    if(node.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
                        if (command instanceof StoredProcedure) {
                            List references = (List)node.getProperty(NodeConstants.Info.PROCEDURE_INPUTS);
                            List defaults = (List)node.getProperty(NodeConstants.Info.PROCEDURE_DEFAULTS);
                            Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.PROCEDURE_CRITERIA);

                            DependentProcedureAccessNode depAccessNode = new DependentProcedureAccessNode(getID(), crit, references, defaults);
                            processNode = depAccessNode;
                            aNode = depAccessNode;
                        } else {
                            //create dependent access node
                            DependentAccessNode depAccessNode = new DependentAccessNode(getID());

                            if(modelID != null){
                                depAccessNode.setPushdown(CapabilitiesUtil.supports(Capability.DEPENDENT_JOIN, modelID, metadata, capFinder));
                                depAccessNode.setMaxSetSize(CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder));
                                depAccessNode.setMaxPredicates(CapabilitiesUtil.getMaxDependentPredicates(modelID, metadata, capFinder));
                                depAccessNode.setUseBindings(CapabilitiesUtil.supports(Capability.DEPENDENT_JOIN_BINDINGS, modelID, metadata, capFinder));
                                //TODO: allow the translator to drive this property
                                //simplistic check of whether this query is complex to re-execute
                                Query query = (Query)command;
                                if (query.getGroupBy() != null
                                        || query.getFrom().getClauses().size() > 1
                                        || !(query.getFrom().getClauses().get(0) instanceof UnaryFromClause)
                                        || query.getWith() != null) {
                                    depAccessNode.setComplexQuery(true);
                                } else {
                                    //check to see if there in an index on at least one of the dependent sets
                                    Set<GroupSymbol> groups = new HashSet<GroupSymbol>(query.getFrom().getGroups());
                                    boolean found = false;
                                    for (Criteria crit : Criteria.separateCriteriaByAnd(query.getCriteria())) {
                                        if (crit instanceof DependentSetCriteria) {
                                            DependentSetCriteria dsc = (DependentSetCriteria)crit;
                                            if (NewCalculateCostUtil.getKeyUsed(ElementCollectorVisitor.getElements(dsc.getExpression(), true), groups, metadata, null) != null) {
                                                found = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (!found) {
                                        depAccessNode.setComplexQuery(true);
                                    }
                                }
                            }
                            processNode = depAccessNode;
                            aNode = depAccessNode;
                        }
                        aNode.setShouldEvaluateExpressions(true);
                    } else {

                        // create access node
                        aNode = new AccessNode(getID());
                        processNode = aNode;

                    }
                    //-- special handling for system tables. currently they cannot perform projection
                    try {
                        if (command instanceof Query) {
                            processNode = correctProjectionInternalTables(node, aNode);
                        }
                    } catch (QueryMetadataException err) {
                        throw new TeiidComponentException(QueryPlugin.Event.TEIID30248, err);
                    }
                    setRoutingName(aNode, node, command);
                    boolean shouldEval = false;
                    if (command instanceof Insert) {
                        Insert insert = (Insert)command;
                        if (insert.getQueryExpression() != null) {
                            insert.setQueryExpression((QueryCommand)aliasCommand(aNode, insert.getQueryExpression(), modelID));
                        } else {
                            for (int i = 0; i < insert.getValues().size(); i++) {
                                Expression ex = (Expression)insert.getValues().get(i);
                                if (!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(ex, modelID, metadata, capFinder, analysisRecord)) {
                                    //replace with an expression symbol to let the rewriter know that it should be replaced
                                    insert.getValues().set(i, new ExpressionSymbol("x", ex));
                                    shouldEval = true;
                                }
                            }
                        }
                    } else if (command instanceof QueryCommand) {
                        command = aliasCommand(aNode, command, modelID);
                    }
                    ev = EvaluatableVisitor.needsEvaluationVisitor(modelID, metadata, capFinder);
                    if (!shouldEval && modelID != null) {
                        //do a capabilities sensitive check for needs eval
                        String modelName = metadata.getFullName(modelID);
                        SourceCapabilities caps = capFinder.findCapabilities(modelName);
                        final CriteriaCapabilityValidatorVisitor capVisitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder, caps);
                        capVisitor.setCheckEvaluation(false);
                        DeepPreOrderNavigator nav = new DeepPreOrderNavigator(ev) {
                            protected void visitNode(org.teiid.query.sql.LanguageObject obj) {
                                if (capVisitor.isValid() && obj instanceof Expression) {
                                    obj.acceptVisitor(capVisitor);
                                }
                                super.visitNode(obj);
                            }
                        };
                        command.acceptVisitor(nav);
                        if (!capVisitor.isValid()) {
                            //there's a non-supported construct pushed, we should eval
                            ev.evaluationNotPossible(EvaluationLevel.PROCESSING);
                        }
                    } else {
                        DeepPreOrderNavigator.doVisit(command, ev);
                    }
                    aNode.setShouldEvaluateExpressions(ev.requiresEvaluation(EvaluationLevel.PROCESSING) || shouldEval);
                    aNode.setCommand(command);
                    if (modelID != null) {
                        String fullName = metadata.getFullName(modelID);
                        SourceCapabilities caps = capFinder.findCapabilities(fullName);
                        aNode.setTransactionSupport((TransactionSupport) caps.getSourceProperty(Capability.TRANSACTION_SUPPORT));
                    }
                    Map<GroupSymbol, PlanNode> subPlans = (Map<GroupSymbol, PlanNode>) node.getProperty(Info.SUB_PLANS);

                    //it makes more sense to allow the multisource affect to be elevated above just access nodes
                    if (aNode.getModelId() != null && metadata.isMultiSource(aNode.getModelId())) {
                        VDBMetaData vdb = context.getVdb();
                        aNode.setShouldEvaluateExpressions(true); //forces a rewrite
                        aNode.setElements( (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS) );
                        if (node.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
                            Expression ex = rewriteMultiSourceCommand(aNode.getCommand());
                            aNode.setConnectorBindingExpression(ex);
                            aNode.setMultiSource(true);
                        } else {
                            String sourceName = (String)node.getProperty(Info.SOURCE_NAME);
                            aNode.setConnectorBindingExpression(new Constant(sourceName));
                        }
                    } else if (subPlans == null){
                        if (!aNode.isShouldEvaluate()) {
                            aNode.minimizeProject(command);
                        }
                        //check if valid to share this with other nodes
                        if (ev != null && ev.getDeterminismLevel().compareTo(Determinism.INSTRUCTION_DETERMINISTIC) >= 0 && command.areResultsCachable()) {
                            checkForSharedSourceCommand(aNode, node);
                        }
                    }
                    if (subPlans != null) {
                        QueryCommand qc = (QueryCommand)command;
                        if (qc.getWith() == null) {
                            qc.setWith(new ArrayList<WithQueryCommand>(subPlans.size()));
                        }
                        Map<GroupSymbol, RelationalPlan> plans = new LinkedHashMap<GroupSymbol, RelationalPlan>();
                        for (Map.Entry<GroupSymbol, PlanNode> entry : subPlans.entrySet()) {
                            RelationalPlan subPlan = convert(entry.getValue());
                            List<ElementSymbol> elems = ResolverUtil.resolveElementsInGroup(entry.getKey(), metadata);
                            subPlan.setOutputElements(elems);
                            plans.put(entry.getKey(), subPlan);
                            WithQueryCommand withQueryCommand = new WithQueryCommand(entry.getKey(), elems, null);
                            qc.getWith().add(withQueryCommand);
                        }
                        aNode.setSubPlans(plans);
                    }
                }
                break;

            case NodeConstants.Types.SELECT:

                Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                if (!node.hasCollectionProperty(Info.OUTPUT_COLS)) {
                    //the late optimization to create a dependent join from a subquery introduces
                    //criteria that have no output elements set
                    //TODO that should be cleaner, but the logic currently expects to be run after join implementation
                    //and rerunning assign output elements seems excessive
                    node.setProperty(Info.OUTPUT_COLS, node.getFirstChild().getProperty(Info.OUTPUT_COLS));
                }
                SelectNode selnode = new SelectNode(getID());
                selnode.setCriteria(crit);
                //in case the parent was a source
                selnode.setProjectedExpressions((List<Expression>) node.getProperty(NodeConstants.Info.PROJECT_COLS));
                //there's a chance that we have positional references that can be pre-evaluated
                boolean postitional = false;
                for (Reference ref : ReferenceCollectorVisitor.getReferences(crit)) {
                    if (ref.isPositional()) {
                        postitional = true;
                        break;
                    }
                }
                selnode.setShouldEvaluateExpressions(postitional);

                processNode = selnode;

                break;

            case NodeConstants.Types.SORT:
            case NodeConstants.Types.DUP_REMOVE:
                if (node.getType() == NodeConstants.Types.DUP_REMOVE) {
                    processNode = new DupRemoveNode(getID());
                } else {
                    SortNode sortNode = new SortNode(getID());
                    OrderBy orderBy = (OrderBy) node.getProperty(NodeConstants.Info.SORT_ORDER);
                    if (orderBy != null) {
                        sortNode.setSortElements(orderBy.getOrderByItems());
                    }
                    if (node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
                        sortNode.setMode(Mode.DUP_REMOVE_SORT);
                    }

                    processNode = sortNode;
                }
                break;
            case NodeConstants.Types.GROUP:
                GroupingNode gnode = new GroupingNode(getID());
                gnode.setRollup(node.hasBooleanProperty(Info.ROLLUP));
                SymbolMap groupingMap = (SymbolMap)node.getProperty(NodeConstants.Info.SYMBOL_MAP);
                gnode.setOutputMapping(groupingMap);
                gnode.setRemoveDuplicates(node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL));
                List<Expression> gCols = (List) node.getProperty(NodeConstants.Info.GROUP_COLS);
                OrderBy orderBy = (OrderBy) node.getProperty(Info.SORT_ORDER);
                if (orderBy == null) {
                    if (gCols != null) {
                        LinkedHashSet<Expression> exprs = new LinkedHashSet<Expression>();
                        for (Expression ex : gCols) {
                            exprs.add(SymbolMap.getExpression(ex));
                        }
                        orderBy = new OrderBy(RuleChooseJoinStrategy.createExpressionSymbols(new ArrayList<Expression>(exprs)));
                    }
                } else {
                    HashSet<Expression> seen = new HashSet<Expression>();
                    for (int i = 0; i < gCols.size(); i++) {
                        if (i < orderBy.getOrderByItems().size()) {
                            OrderByItem orderByItem = orderBy.getOrderByItems().get(i);
                            Expression ex = SymbolMap.getExpression(orderByItem.getSymbol());
                            if (!seen.add(ex)) {
                                continue;
                            }
                            if (ex instanceof ElementSymbol) {
                                ex = groupingMap.getMappedExpression((ElementSymbol) ex);
                                orderByItem.setSymbol(new ExpressionSymbol("expr", ex)); //$NON-NLS-1$
                            }
                        } else {
                            orderBy.addVariable(new ExpressionSymbol("expr", gCols.get(i)), OrderBy.ASC); //$NON-NLS-1$
                        }
                    }
                }
                if (orderBy != null) {
                    gnode.setOrderBy(orderBy.getOrderByItems());
                }
                for (Expression ex : groupingMap!=null?groupingMap.getValues():(List<Expression>)node.getFirstChild().getProperty(NodeConstants.Info.PROJECT_COLS)) {
                    if (ex instanceof AggregateSymbol) {
                        validateAggregateFunctionEvaluation((AggregateSymbol)ex);
                    }
                }
                processNode = gnode;
                break;

            case NodeConstants.Types.SOURCE:
                Object source = node.getProperty(NodeConstants.Info.TABLE_FUNCTION);
                if (source instanceof XMLTable) {
                    XMLTable xt = (XMLTable)source;
                    //we handle the projection filtering once here rather than repeating the
                    //path analysis on a per plan basis
                    updateGroupName(node, xt);
                    Map<Expression, Integer> elementMap = RelationalNode.createLookupMap(xt.getProjectedSymbols());
                    List cols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
                    int[] projectionIndexes = RelationalNode.getProjectionIndexes(elementMap, cols);
                    ArrayList<XMLColumn> filteredColumns = new ArrayList<XMLColumn>(projectionIndexes.length);
                    for (int col : projectionIndexes) {
                        filteredColumns.add(xt.getColumns().get(col));
                    }
                    xt.getXQueryExpression().useDocumentProjection(filteredColumns, analysisRecord);
                    processNode = XMLHelper.getInstance().newXMLTableNode(getID(), xt, filteredColumns);
                    break;
                }
                if (source instanceof ObjectTable) {
                    ObjectTable ot = (ObjectTable)source;
                    ObjectTableNode otn = new ObjectTableNode(getID());
                    //we handle the projection filtering once here rather than repeating the
                    //path analysis on a per plan basis
                    updateGroupName(node, ot);
                    Map<Expression, Integer> elementMap = RelationalNode.createLookupMap(ot.getProjectedSymbols());
                    List<Expression> cols = (List<Expression>) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
                    int[] projectionIndexes = RelationalNode.getProjectionIndexes(elementMap, cols);
                    ArrayList<ObjectColumn> filteredColumns = new ArrayList<ObjectColumn>(projectionIndexes.length);
                    for (int col : projectionIndexes) {
                        filteredColumns.add(ot.getColumns().get(col));
                    }
                    otn.setProjectedColumns(filteredColumns);
                    otn.setTable(ot);
                    processNode = otn;
                    break;
                }
                if (source instanceof TextTable) {
                    TextTableNode ttn = new TextTableNode(getID());
                    TextTable tt = (TextTable)source;
                    updateGroupName(node, tt);
                    ttn.setTable(tt);
                    processNode = ttn;
                    break;
                }
                if (source instanceof ArrayTable) {
                    ArrayTableNode atn = new ArrayTableNode(getID());
                    ArrayTable at = (ArrayTable)source;
                    updateGroupName(node, at);
                    atn.setTable(at);
                    processNode = atn;
                    break;
                }
                SymbolMap symbolMap = (SymbolMap) node.getProperty(NodeConstants.Info.SYMBOL_MAP);
                if(symbolMap != null) {
                    PlanNode child = node.getLastChild();

                    if (child.getType() == NodeConstants.Types.PROJECT
                            || child.getType() == NodeConstants.Types.SELECT) {
                        //update the project cols based upon the original output
                        child.setProperty(NodeConstants.Info.PROJECT_COLS, child.getProperty(NodeConstants.Info.OUTPUT_COLS));
                    }
                    if (child.getType() != NodeConstants.Types.SET_OP || child.getProperty(Info.SET_OPERATION) == Operation.UNION) {
                        child.setProperty(NodeConstants.Info.OUTPUT_COLS, node.getProperty(NodeConstants.Info.OUTPUT_COLS));
                    } else {
                        //else we cannot directly update the child properties as the child will get converted to a join
                        //create a projection instead for initialization purposes, but won't impact performance
                        ProjectNode pNode = new ProjectNode(getID());
                        pNode.setSelectSymbols((List<? extends Expression>) child.getProperty(NodeConstants.Info.OUTPUT_COLS));
                        return prepareToAdd(node, pNode);
                    }
                }
                return null;
            case NodeConstants.Types.SET_OP:
                Operation setOp = (Operation) node.getProperty(NodeConstants.Info.SET_OPERATION);
                boolean useAll = ((Boolean) node.getProperty(NodeConstants.Info.USE_ALL)).booleanValue();
                if(setOp == Operation.UNION) {
                    RelationalNode unionAllNode = new UnionAllNode(getID());

                    if(useAll) {
                        processNode = unionAllNode;
                    } else {
                        boolean onlyDupRemoval = node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL);
                        if (onlyDupRemoval) {
                            processNode = new DupRemoveNode(getID());
                        } else {
                            SortNode sNode = new SortNode(getID());
                            sNode.setMode(Mode.DUP_REMOVE_SORT);
                            processNode = sNode;
                        }

                        unionAllNode.setElements( (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS) );
                        processNode.addChild(unionAllNode);
                    }
                } else {
                    JoinNode joinAsSet = new JoinNode(getID());
                    joinAsSet.setJoinStrategy(new MergeJoinStrategy(SortOption.SORT_DISTINCT, SortOption.SORT_DISTINCT, true));
                    //If we push these sorts, we will have to enforce null order, since nulls are equal here
                    List leftExpressions = (List) node.getFirstChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
                    List rightExpressions = (List) node.getLastChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
                    joinAsSet.setJoinType(setOp == Operation.EXCEPT ? JoinType.JOIN_ANTI_SEMI : JoinType.JOIN_SEMI);
                    joinAsSet.setJoinExpressions(leftExpressions, rightExpressions);
                    processNode = joinAsSet;
                }

                break;

            case NodeConstants.Types.TUPLE_LIMIT:
                Expression rowLimit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                LimitNode ln = new LimitNode(getID(), rowLimit, offset);
                ln.setImplicit(node.hasBooleanProperty(Info.IS_IMPLICIT_LIMIT));
                processNode = ln;
                break;

            case NodeConstants.Types.NULL:
                processNode = new NullNode(getID());
                break;

            default:
                 throw new QueryPlannerException(QueryPlugin.Event.TEIID30250, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30250, NodeConstants.getNodeTypeString(node.getType())));
        }

        if(processNode != null) {
            processNode = prepareToAdd(node, processNode);
        }

        return processNode;
    }

    /**
     * If the capabilities for the given model are invalid, this will throw the original exception
     */
    public static void checkForValidCapabilities(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
            throws TeiidComponentException, QueryMetadataException,
            QueryPlannerException {
        String fullName = metadata.getFullName(modelID);
        if (!capFinder.isValid(fullName)) {
            SourceCapabilities caps = capFinder.findCapabilities(fullName);
            Exception cause = null;
            if (caps != null) {
                cause = (Exception) caps.getSourceProperty(Capability.INVALID_EXCEPTION);
            }
            throw new QueryPlannerException(QueryPlugin.Event.TEIID30498, cause, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30498, fullName));
        }
    }

    private void validateAggregateFunctionEvaluation(AggregateSymbol as) throws QueryPlannerException {
        if (as.getFunctionDescriptor() != null && as.getFunctionDescriptor().getPushdown() == PushDown.MUST_PUSHDOWN) {
            throw new QueryPlannerException(QueryPlugin.Event.TEIID31211, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31211, as.getFunctionDescriptor().getFullName()));
        }
    }

    private Command aliasCommand(AccessNode aNode, Command command,
            Object modelID) throws TeiidComponentException,
            QueryPlannerException {
        try {
            command = (Command)command.clone();
            boolean aliasGroups = modelID != null && (CapabilitiesUtil.supportsGroupAliases(modelID, metadata, capFinder)
                    || CapabilitiesUtil.supports(Capability.QUERY_FROM_INLINE_VIEWS, modelID, metadata, capFinder));
            boolean aliasColumns = modelID != null && (CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION, modelID, metadata, capFinder)
                    || CapabilitiesUtil.supports(Capability.QUERY_FROM_INLINE_VIEWS, modelID, metadata, capFinder));
            AliasGenerator visitor = new AliasGenerator(aliasGroups, !aliasColumns);
            SourceHint sh = command.getSourceHint();
            if (sh != null && aliasGroups) {
                VDBMetaData vdb = context.getDQPWorkContext().getVDB();
                ModelMetaData model = vdb.getModel(aNode.getModelName());
                List<String> sourceNames = model.getSourceNames();
                SpecificHint sp = null;
                if (sourceNames.size() == 1) {
                    sp = sh.getSpecificHint(sourceNames.get(0));
                }
                if (sh.isUseAliases() || (sp != null && sp.isUseAliases())) {
                    visitor.setAliasMapping(context.getAliasMapping());
                }
            }
            List<Reference> references = ReferenceCollectorVisitor.getReferences(command);
            if (!references.isEmpty()) {
                Set<String> correleatedGroups = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                for (Reference ref : references) {
                    if (ref.isCorrelated() && ref.getExpression().getGroupSymbol() != null) {
                        correleatedGroups.add(ref.getExpression().getGroupSymbol().getName());
                    }
                }
                visitor.setCorrelationGroups(correleatedGroups);
            }
            command.acceptVisitor(visitor);
        } catch (QueryMetadataException err) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30249, err);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof QueryPlannerException) {
                throw (QueryPlannerException)e.getCause();
            }
            throw e;
        }
        return command;
    }

    private void checkForSharedSourceCommand(AccessNode aNode, PlanNode node) {
        //create a top level key to avoid the full command toString
        String modelName = aNode.getModelName();
        Command cmd = aNode.getCommand();

        //don't share full scans against internal sources, it's a waste of buffering
        if (CoreConstants.SYSTEM_MODEL.equals(modelName)
                || CoreConstants.SYSTEM_ADMIN_MODEL.equals(modelName)
                || TempMetadataAdapter.TEMP_MODEL.getName().equals(modelName)) {
            if (!(cmd instanceof Query)) {
                return;
            }
            Query query = (Query)cmd;
            if (query.getOrderBy() == null && query.getCriteria() == null) {
                return;
            }
        }

        AccessNode other = sharedCommands.get(cmd);

        //lateral may be reused any number of times,
        //so this requires special handling
        //we will clean it up at the end
        boolean lateral = false;
        while (node.getParent() != null) {
            if (node.getParent().getType() == NodeConstants.Types.JOIN
                    && node.getParent().getProperty(Info.JOIN_STRATEGY) == JoinStrategyType.NESTED_TABLE
                    && node.getParent().getLastChild() == node) {
                lateral = true;
                break;
            }
            node = node.getParent();
        }

        if (other == null) {
            sharedCommands.put(cmd, aNode);
            if (lateral) {
                aNode.info = new RegisterRequestParameter.SharedAccessInfo();
                aNode.info.id = sharedId.getAndIncrement();
                aNode.info.sharingCount = -1;
            }
        } else {
            if (other.info == null) {
                other.info = new RegisterRequestParameter.SharedAccessInfo();
                other.info.id = sharedId.getAndIncrement();
            }
            if (other.info.sharingCount != -1) {
                other.info.sharingCount++;
            } else if (lateral) {
                other.info.sharingCount = -1;
            }
            aNode.info = other.info;
        }
    }

    private void updateGroupName(PlanNode node, TableFunctionReference tt) {
        String groupName = node.getGroups().iterator().next().getName();
        tt.getGroupSymbol().setName(groupName);
        for (ElementSymbol symbol : tt.getProjectedSymbols()) {
            symbol.setGroupSymbol(new GroupSymbol(groupName));
        }
    }

    private RelationalNode correctProjectionInternalTables(PlanNode node,
                                                                AccessNode aNode) throws QueryMetadataException,
                                                                                                       TeiidComponentException {
        if (node.getGroups().size() != 1) {
            return aNode;
        }
        GroupSymbol group = node.getGroups().iterator().next();
        if (!CoreConstants.SYSTEM_MODEL.equals(metadata.getFullName(metadata.getModelID(group.getMetadataID())))
                && !CoreConstants.SYSTEM_ADMIN_MODEL.equals(metadata.getFullName(metadata.getModelID(group.getMetadataID())))) {
            return aNode;
        }
        List projectSymbols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
        List<ElementSymbol> acutalColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
        if (projectSymbols.equals(acutalColumns)) {
            return aNode;
        }
        node.setProperty(NodeConstants.Info.OUTPUT_COLS, acutalColumns);
        if (node.getParent() != null && node.getParent().getType() == NodeConstants.Types.PROJECT) {
            //if the parent is already a project, just correcting the output cols is enough
            return aNode;
        }
        ProjectNode pnode = new ProjectNode(getID());

        pnode.setSelectSymbols(projectSymbols);
        aNode = (AccessNode)prepareToAdd(node, aNode);
        node.setProperty(NodeConstants.Info.OUTPUT_COLS, projectSymbols);
        pnode.addChild(aNode);
        return pnode;
    }

    private RelationalNode prepareToAdd(PlanNode node,
                                          RelationalNode processNode) {
        // Set the output elements from the plan node
        List cols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);

        processNode.setElements(cols);

        // Set the Cost Estimates
        Number estimateNodeCardinality = (Number) node.getProperty(NodeConstants.Info.EST_CARDINALITY);
        processNode.setEstimateNodeCardinality(estimateNodeCardinality);
        Number estimateNodeSetSize = (Number) node.getProperty(NodeConstants.Info.EST_SET_SIZE);
        processNode.setEstimateNodeSetSize(estimateNodeSetSize);
        Number estimateDepAccessCardinality = (Number) node.getProperty(NodeConstants.Info.EST_DEP_CARDINALITY);
        processNode.setEstimateDepAccessCardinality(estimateDepAccessCardinality);
        Number estimateDepJoinCost = (Number) node.getProperty(NodeConstants.Info.EST_DEP_JOIN_COST);
        processNode.setEstimateDepJoinCost(estimateDepJoinCost);
        Number estimateJoinCost = (Number) node.getProperty(NodeConstants.Info.EST_JOIN_COST);
        processNode.setEstimateJoinCost(estimateJoinCost);

        return processNode;
    }

    private void setRoutingName(AccessNode accessNode, PlanNode node, Command command)
        throws QueryPlannerException, TeiidComponentException {

        // Look up connector binding name
        try {
            Object modelID = node.getProperty(NodeConstants.Info.MODEL_ID);
            if(modelID == null || modelID instanceof TempMetadataID) {
                if(command instanceof StoredProcedure){
                    modelID = ((StoredProcedure)command).getModelID();
                }else if (!(command instanceof Create || command instanceof Drop)){
                    Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(command, true);
                    GroupSymbol group = groups.iterator().next();

                    modelID = metadata.getModelID(group.getMetadataID());
                }
            }
            String cbName = metadata.getFullName(modelID);
            accessNode.setModelName(cbName);
            accessNode.setModelId(modelID);
            accessNode.setConformedTo((Set<Object>) node.getProperty(Info.CONFORMED_SOURCES));
        } catch(QueryMetadataException e) {
             throw new QueryPlannerException(QueryPlugin.Event.TEIID30251, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30251));
        }
    }

    private Expression rewriteMultiSourceCommand(Command command) throws TeiidComponentException {
        Expression result = null;
        if (command instanceof StoredProcedure) {
            StoredProcedure obj = (StoredProcedure)command;
            for (Iterator<SPParameter> params = obj.getMapOfParameters().values().iterator(); params.hasNext();) {
                SPParameter param = params.next();
                if (param.getParameterType() != SPParameter.IN) {
                    continue;
                }
                if(metadata.isMultiSourceElement(param.getMetadataID())) {
                    Expression source = param.getExpression();
                    params.remove();
                    if (param.isUsingDefault() && source instanceof Constant && ((Constant)source).isNull()) {
                        continue;
                    }
                    result = source;
                    break;
                }
            }
        } if (command instanceof Insert) {
            Insert obj = (Insert)command;
            for (int i = 0; i < obj.getVariables().size(); i++) {
                ElementSymbol elem = obj.getVariables().get(i);
                Object metadataID = elem.getMetadataID();
                if(metadata.isMultiSourceElement(metadataID)) {
                    Expression source = (Expression)obj.getValues().get(i);
                    obj.getVariables().remove(i);
                    obj.getValues().remove(i);
                    result = source;
                    break;
                }
            }
        } else if (command instanceof FilteredCommand) {
            for (Criteria c : Criteria.separateCriteriaByAnd(((FilteredCommand) command).getCriteria())) {
                if (!(c instanceof CompareCriteria)) {
                    continue;
                }
                CompareCriteria cc = (CompareCriteria)c;
                if (cc.getLeftExpression() instanceof ElementSymbol) {
                    ElementSymbol es = (ElementSymbol)cc.getLeftExpression();
                    if (metadata.isMultiSourceElement(es.getMetadataID()) && EvaluatableVisitor.willBecomeConstant(cc.getRightExpression())) {
                        if (result != null && !result.equals(cc.getRightExpression())) {
                            return Constant.NULL_CONSTANT;
                        }
                        result = cc.getRightExpression();
                    }
                }
            }
        }
        return result;
    }

}
