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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.common.buffer.LobManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Procedure;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.BasicQueryMetadata;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.MaterializationMetadataRepository.ErrorAction;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TriggerActionPlanner;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.optimizer.relational.rules.FrameUtil;
import org.teiid.query.optimizer.relational.rules.RuleApplySecurity;
import org.teiid.query.optimizer.relational.rules.RuleAssignOutputElements;
import org.teiid.query.optimizer.relational.rules.RuleChooseDependent;
import org.teiid.query.optimizer.relational.rules.RuleCollapseSource;
import org.teiid.query.optimizer.relational.rules.RuleConstants;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.optimizer.relational.rules.RulePlanSubqueries;
import org.teiid.query.optimizer.relational.rules.RulePushAggregates;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.PlanExecutionNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.FilteredCommand;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryContainer.Evaluatable;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TargetedCommand;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.CommandCollectorVisitor;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.ValidationVisitor;


/**
 * This class generates a relational plan for query execution.  The output of
 * this class is a {@link org.teiid.query.optimizer.relational.plantree.PlanNode PlanNode}
 * object - this object then becomes the input to
 * {@link PlanToProcessConverter PlanToProcessConverter}
 * to  produce a
 * {@link org.teiid.query.processor.relational.RelationalPlan RelationalPlan}.
 */
public class RelationalPlanner {

    public static final String MAT_PREFIX = "#MAT_"; //$NON-NLS-1$

    private AnalysisRecord analysisRecord;
    private Command parentCommand;
    private IDGenerator idGenerator;
    CommandContext context;
    CapabilitiesFinder capFinder;
    QueryMetadataInterface metadata;
    private PlanHints hints = new PlanHints();
    private Option option;
    private SourceHint sourceHint;
    private WithPlanningState withPlanningState;
    private Set<GroupSymbol> withGroups;

    private boolean processWith = true;

    private static final Comparator<GroupSymbol> nonCorrelatedComparator = new Comparator<GroupSymbol>() {
        @Override
        public int compare(GroupSymbol arg0, GroupSymbol arg1) {
            return arg0.getNonCorrelationName().compareTo(arg1.getNonCorrelationName());
        }
    };

    private static class PlanningStackEntry {
        Command command;
        GroupSymbol group;

        public PlanningStackEntry(Command command, GroupSymbol group) {
            this.command = command;
            this.group = group;
        }

        @Override
        public int hashCode() {
            return HashCodeUtil.hashCode(group.getMetadataID().hashCode(), command.getType());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof PlanningStackEntry)) {
                return false;
            }
            PlanningStackEntry other = (PlanningStackEntry)obj;
            return group.getMetadataID().equals(other.group.getMetadataID())
                    && command.getType() == other.command.getType();
        }

        @Override
        public String toString() {
            return command.getClass().getSimpleName() + " " + group.getNonCorrelationName().toString(); //$NON-NLS-1$
        }
    }

    private static ThreadLocal<HashSet<PlanningStackEntry>> planningStack = new ThreadLocal<HashSet<PlanningStackEntry>>() {
        @Override
        protected HashSet<PlanningStackEntry> initialValue() {
            return new LinkedHashSet<PlanningStackEntry>();
        }
    };

    private static class WithPlanningState {
        LinkedHashMap<QueryCommand, WithQueryCommand> withList = new LinkedHashMap<QueryCommand, WithQueryCommand>();
        LinkedHashMap<String, WithQueryCommand> pushdownWith = new LinkedHashMap<String, WithQueryCommand>();
        //the value will be TRUE for repeated, FALSE for non-shared single use, or an integer for a shared Command
        LinkedHashMap<String, Object> pushdownState = new LinkedHashMap<String, Object>();
    }

    public RelationalPlan optimize(
        Command command)
        throws
            QueryPlannerException,
            QueryMetadataException,
            TeiidComponentException, QueryResolverException {

        boolean debug = analysisRecord.recordDebug();
        if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("GENERATE CANONICAL: \n" + command); //$NON-NLS-1$
        }

        SourceHint previous = this.sourceHint;
        this.sourceHint = SourceHint.combine(previous, command.getSourceHint());

        PlanToProcessConverter planToProcessConverter = new PlanToProcessConverter(metadata, idGenerator, analysisRecord, capFinder, context);

        WithPlanningState saved = this.withPlanningState;

        this.withPlanningState = new WithPlanningState();

        Command original = (Command) command.clone();

        PlanNode plan;
        try {
            plan = generatePlan(command);
        } catch (TeiidProcessingException e) {
             throw new QueryPlannerException(e);
        }

        if (!this.withPlanningState.withList.isEmpty()) {
            // set correlated references before planning with, but
            // don't fully plan the subqueries as that needs to happen after
            // with planning
            connectSubqueryContainers(plan, true);
            //use the original to plan with as the command object may have been modified
            //in particular the replacement of aggregate expressions
            planWith(plan, original);
        }
        // Connect ProcessorPlan to SubqueryContainer (if any) of SELECT, PROJECT, and other nodes
        connectSubqueryContainers(plan, false); //TODO: merge with node creation

        if (plan.getType() == NodeConstants.Types.SOURCE) {
            //this was effectively a rewrite
            return (RelationalPlan)plan.getProperty(Info.PROCESSOR_PLAN);
        }

        if(debug) {
            analysisRecord.println("\nCANONICAL PLAN: \n" + plan); //$NON-NLS-1$
        }

        // Set top column information on top node
        List<Expression> topCols = Util.deepClone(command.getProjectedSymbols(), Expression.class);

        // Build rule set based on hints
        RuleStack rules = buildRules();
        // Run rule-based optimizer
        plan = executeRules(rules, plan);

        RelationalPlan result = planToProcessConverter.convert(plan);
        boolean fullPushdown = false;
        if (!this.withPlanningState.pushdownWith.isEmpty()) {
            AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(result);
             if (aNode != null) {
                 QueryCommand queryCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
                 if (queryCommand != null) {
                     fullPushdown = true;
                     for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(queryCommand)) {
                         if (container instanceof Evaluatable<?> && ((Evaluatable<?>)container).shouldEvaluate()) {
                             //we could more deeply check, but we'll just assume that the references are needed
                             fullPushdown = false;
                             break;
                         }
                     }
                 }
             }
             //distribute the appropriate clauses to the pushdowns
             assignWithClause(result.getRootNode(), this.withPlanningState.pushdownWith, false);

             List<String> toReplan = new ArrayList<String>();
             for (Map.Entry<String, Object> entry : this.withPlanningState.pushdownState.entrySet()) {
                 if (Boolean.TRUE.equals(entry.getValue())) {
                     GroupSymbol gs = this.withPlanningState.pushdownWith.get(entry.getKey()).getGroupSymbol();
                     TempMetadataID tmi = (TempMetadataID) gs.getMetadataID();
                     tmi.getTableData().setModel(TempMetadataAdapter.TEMP_MODEL);
                     toReplan.add(entry.getKey());
                 }
             }
             if (!toReplan.isEmpty()) {
                 for (WithQueryCommand wqc : this.withPlanningState.withList.values()) {
                     this.context.getGroups().remove(wqc.getGroupSymbol().getName());
                 }
                 this.sourceHint = previous;
                 this.withPlanningState = saved;
                 if (debug) {
                     analysisRecord.println("\nReplanning due to multiple common table references: " + toReplan + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
                 }
                 return optimize(original);
             }
        }
        if (!fullPushdown && !this.withPlanningState.withList.isEmpty()) {
            //generally any with item associated with a pushdown will not be needed as we're converting to a source query
            result.setWith(new ArrayList<WithQueryCommand>(this.withPlanningState.withList.values()));

            //assign any with clauses in this subplan
            for (WithQueryCommand wqc : this.withPlanningState.withList.values()) {
                if (wqc.isRecursive()) {
                    SetQuery sq = (SetQuery)wqc.getCommand();
                    assignWithClause(((RelationalPlan)sq.getLeftQuery().getProcessorPlan()).getRootNode(), this.withPlanningState.pushdownWith, false);
                    assignWithClause(((RelationalPlan)sq.getRightQuery().getProcessorPlan()).getRootNode(), this.withPlanningState.pushdownWith, false);
                } else {
                    assignWithClause(((RelationalPlan)wqc.getCommand().getProcessorPlan()).getRootNode(), this.withPlanningState.pushdownWith, false);
                }
            }
        }
        result.setOutputElements(topCols);
        this.sourceHint = previous;
        this.withPlanningState = saved;
        return result;
    }

    private void planWith(PlanNode plan, Command command) throws QueryPlannerException,
            QueryMetadataException, TeiidComponentException,
            QueryResolverException {
        //TODO: merge this logic inline with the main rule execution.
        Set<TempMetadataID> accessed = Collections.newSetFromMap(new IdentityHashMap<TempMetadataID, Boolean>());
        this.context.setAccessed(accessed);
        RuleStack stack = new RuleStack();
        stack.push(new RuleAssignOutputElements(false));
        if (hints.hasRowBasedSecurity) {
            stack.push(new RuleApplySecurity());
        }
        //use a temporary planner to run just the assign output elements
        RelationalPlanner planner = new RelationalPlanner();
        planner.processWith = false; //we don't want to trigger the with processing for just projection
        planner.initialize(command, idGenerator, metadata, capFinder, analysisRecord, context);
        planner.executeRules(stack, plan);
        //discover all of usage not covered in the main plan above
        //-- that's WITH clauses and subqueries
        LinkedHashSet<Command> commands = new LinkedHashSet<>();
        collectWithCommands(command, commands);
        for (PlanNode node : getAllPossibleSubqueryNodes(plan)) {
            List<SubqueryContainer<?>> subqueryContainers = node.getSubqueryContainers();
            for (SubqueryContainer<?> subqueryContainer : subqueryContainers) {
                commands.add(subqueryContainer.getCommand());
            }
            if (node.getType() == NodeConstants.Types.SOURCE) {
                Command nested = (Command) node.getProperty(Info.NESTED_COMMAND);
                if (nested != null) {
                    collectWithCommands(nested, commands);
                }
            }
        }
        List<Command> toExplore = new ArrayList<>(commands);
        while (!toExplore.isEmpty()) {
            Command cmd = toExplore.remove(toExplore.size() - 1);
            for (Command subCommand : CommandCollectorVisitor.getCommands(cmd, true)) {
                if (commands.add(subCommand)) {
                    toExplore.add(subCommand);
                }
            }
            try {
                PlanNode temp = planner.generatePlan((Command) cmd.clone());
                stack.push(new RuleAssignOutputElements(false));
                planner.executeRules(stack, temp);
            } catch (TeiidProcessingException e) {
                throw new QueryPlannerException(e);
            }
        }
        //plan and minimize projection
        for (WithQueryCommand with : this.withPlanningState.withList.values()) {
            QueryCommand subCommand = with.getCommand();

            TempMetadataID tid = (TempMetadataID) with.getGroupSymbol().getMetadataID();
            if (tid.getTableData().getModel() != TempMetadataAdapter.TEMP_MODEL) {
                tid.getTableData().setModel(null);
            }

            subCommand = minimizeWithProjection(with, subCommand, tid, accessed);
            if (with.isRecursive()) {
                SetQuery setQuery = (SetQuery) subCommand;

                QueryCommand qc = setQuery.getLeftQuery();
                final RelationalPlan subPlan = optimize(qc);
                qc.setProcessorPlan(subPlan);

                AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(subPlan);
                Object modelID = null;
                QueryCommand withCommand = null;
                if (aNode != null) {
                    modelID = CriteriaCapabilityValidatorVisitor.validateCommandPushdown(null, metadata, capFinder, aNode, false);
                    if (modelID != null) {
                        if (with.getGroupSymbol().getModelMetadataId() != null || !CapabilitiesUtil.supports(Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS, modelID, metadata, capFinder) || with.isMaterialize()) {
                            modelID = null;
                        } else {
                            withCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
                            if (withCommand != null) {
                                //provisionally set the source
                                ((TempMetadataID)with.getGroupSymbol().getMetadataID()).getTableData().setModel(modelID);
                            }
                        }
                    }
                }
                //now that we possibly have a model id, plan the recursive part
                QueryCommand qc1 = setQuery.getRightQuery();
                RelationalPlan subPlan1 = optimize((Command) qc1.clone());
                qc1.setProcessorPlan(subPlan1);

                if (!isPushdownValid(with, setQuery, modelID, withCommand, subPlan1) && withCommand != null) {
                    //reset the source to null and replan
                    ((TempMetadataID)with.getGroupSymbol().getMetadataID()).getTableData().setModel(null);
                    subPlan1 = optimize(qc1);
                    qc1.setProcessorPlan(subPlan1);
                }
                continue;
            }
            RelationalPlan subPlan = optimize(subCommand);
            subCommand.setProcessorPlan(subPlan);
            RelationalPlan procPlan = subPlan;
            RelationalNode root = procPlan.getRootNode();
            Number planCardinality = root.getEstimateNodeCardinality();
            if (planCardinality != null) {
                ((TempMetadataID)with.getGroupSymbol().getMetadataID()).setCardinality(planCardinality.intValue());
            }
            AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(procPlan);
            if (aNode == null) {
                continue;
            }
            Object modelID = CriteriaCapabilityValidatorVisitor.validateCommandPushdown(null, metadata, capFinder, aNode, false);
            QueryCommand withCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
            if (modelID == null || withCommand == null) {
                continue;
            }
            if (with.getGroupSymbol().getModelMetadataId() != null || !CapabilitiesUtil.supports(Capability.COMMON_TABLE_EXPRESSIONS, modelID, metadata, capFinder) || with.isMaterialize()) {
                continue;
            }
            WithQueryCommand wqc = new WithQueryCommand(with.getGroupSymbol(), with.getColumns(), withCommand);
            wqc.setNoInline(with.isNoInline());
            ((TempMetadataID)with.getGroupSymbol().getMetadataID()).getTableData().setModel(modelID);
            this.withPlanningState.pushdownWith.put(with.getGroupSymbol().getName(), wqc);
        }
        this.context.setAccessed(null);
    }

    private void collectWithCommands(Command command, Collection<Command> commands) {
        if (command instanceof QueryCommand) {
            QueryCommand query = (QueryCommand)command;
            List<WithQueryCommand> with = query.getWith();
            if (with != null) {
                for (WithQueryCommand withQueryCommand : with) {
                    commands.add(withQueryCommand.getCommand());
                }
            }
        }
    }

    private List<PlanNode> getAllPossibleSubqueryNodes(PlanNode plan) {
        return NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT | NodeConstants.Types.SELECT | NodeConstants.Types.JOIN | NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP | NodeConstants.Types.SORT);
    }

    private QueryCommand minimizeWithProjection(WithQueryCommand with,
            QueryCommand subCommand, TempMetadataID tid, Collection<TempMetadataID> accessed)
            throws QueryMetadataException, QueryResolverException,
            TeiidComponentException {
        List<TempMetadataID> elements = tid.getElements();
        List<Integer> toRemove = new ArrayList<Integer>();
        for (int i = elements.size()-1; i >= 0; i--) {
            TempMetadataID elem = elements.get(i);
            if (!accessed.contains(elem)) {
                toRemove.add(i);
            }
        }
        //the strategy here is to replace the actual projections with null.  this keeps
        //the definition of the with clause consistent
        if (!toRemove.isEmpty()) {
            if (with.isRecursive()) {
                SetQuery setQuery = (SetQuery) subCommand;
                setQuery.setLeftQuery(removeUnusedProjection(with, setQuery.getLeftQuery(), elements, toRemove));
                setQuery.setRightQuery(removeUnusedProjection(with, setQuery.getRightQuery(), elements, toRemove));
            } else {
                subCommand = removeUnusedProjection(with, subCommand, elements,
                        toRemove);
                with.setCommand(subCommand);
            }
        }
        return subCommand;
    }

    /**
     * Remove unused projects by replacing with null
     * @param with
     * @param subCommand
     * @param elements
     * @param toRemove
     * @return
     * @throws QueryMetadataException
     * @throws QueryResolverException
     * @throws TeiidComponentException
     */
    private Query removeUnusedProjection(WithQueryCommand with,
            QueryCommand subCommand, List<TempMetadataID> elements,
            List<Integer> toRemove) throws QueryMetadataException,
            QueryResolverException, TeiidComponentException {
        Query query = null;
        if (!(subCommand instanceof Query) || subCommand.getOrderBy() != null) {
            GroupSymbol gs = new GroupSymbol("x"); //$NON-NLS-1$
            gs = RulePlaceAccess.recontextSymbol(gs, context.getGroups());
            query = QueryRewriter.createInlineViewQuery(gs, subCommand, metadata, ResolverUtil.resolveElementsInGroup(with.getGroupSymbol(), metadata));
        } else {
            query = (Query)subCommand;
        }
        for (int i : toRemove) {
            query.getSelect().getSymbols().set(i, new ExpressionSymbol(elements.get(i).getName(), new Constant(null, elements.get(i).getType())));
        }
        return query;
    }

    private boolean isPushdownValid(WithQueryCommand with, SetQuery setQuery,
            Object modelID, QueryCommand withCommand, RelationalPlan subPlan1)
            throws QueryMetadataException, TeiidComponentException {
        AccessNode aNode1 = CriteriaCapabilityValidatorVisitor.getAccessNode(subPlan1);
        if (aNode1 == null) {
            return false;
        }
        Object modelID1 = CriteriaCapabilityValidatorVisitor.validateCommandPushdown(null, metadata, capFinder, aNode1, false);
        QueryCommand withCommand1 = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode1);
        if (modelID1 == null || withCommand1 == null) {
            return false;
        }

        //if we are the same connector for each, then we should be good to proceed
        if (CapabilitiesUtil.isSameConnector(modelID, modelID1, metadata, capFinder)) {
            SetQuery pushdownSetQuery = new SetQuery(Operation.UNION, setQuery.isAll(), withCommand, withCommand1);
            WithQueryCommand wqc = new WithQueryCommand(with.getGroupSymbol(), with.getColumns(), pushdownSetQuery);
            wqc.setRecursive(true);
            this.withPlanningState.pushdownWith.put(with.getGroupSymbol().getName(), wqc);
            return true;
        }

        return false;
    }

    private void processWith(final QueryCommand command, List<WithQueryCommand> withList)
            throws QueryMetadataException, TeiidComponentException {
        for (int i = 0; i < withList.size(); i++) {
            WithQueryCommand with = withList.get(i);
            //check for a duplicate with clause, which can occur in a self-join scenario
            WithQueryCommand existing = this.withPlanningState.withList.get(with.getCommand());
            if (existing != null) {
                final GroupSymbol old = with.getGroupSymbol();
                replaceSymbol(command, old, existing.getGroupSymbol());
                continue;
            }
            final GroupSymbol old = with.getGroupSymbol();
            if (!context.getGroups().add(old.getName()) || old.getName().matches("(g|v)_\\d*")) {
                final GroupSymbol gs = RulePlaceAccess.recontextSymbol(old, context.getGroups());
                LinkedHashMap<ElementSymbol, Expression> replacementSymbols = FrameUtil.buildSymbolMap(old, gs, metadata);
                gs.setDefinition(null);
                //update the with clause with the new group name / columns
                with.setGroupSymbol(gs);
                with.setColumns(new ArrayList(replacementSymbols.values()));
                //we use equality checks here because there may be a similarly named at lower scopes
                replaceSymbol(command, old, gs);
            }
            this.context.getAliasMapping().put(with.getGroupSymbol().getName(), old.getName());
            this.withPlanningState.withList.put(with.getCommand(), with);
        }
    }

    private void replaceSymbol(final QueryCommand command,
            final GroupSymbol old, final GroupSymbol gs) {
        PreOrPostOrderNavigator nav = new PreOrPostOrderNavigator(new LanguageVisitor() {
            @Override
            public void visit(UnaryFromClause obj) {
                if (old.getMetadataID() == obj.getGroup().getMetadataID()) {
                    String def = obj.getGroup().getDefinition();
                    if (def != null) {
                        String name = obj.getGroup().getName();
                        obj.setGroup(gs.clone());
                        obj.getGroup().setDefinition(gs.getName());
                        obj.getGroup().setName(name);
                    } else {
                        obj.setGroup(gs);
                    }
                }
            }

            @Override
            public void visit(ElementSymbol es) {
                if (es.getGroupSymbol().getMetadataID() == old.getMetadataID()) {
                    String def = es.getGroupSymbol().getDefinition();
                    if (def != null) {
                        String name = es.getGroupSymbol().getName();
                        es.setGroupSymbol(gs.clone());
                        es.getGroupSymbol().setDefinition(gs.getName());
                        es.getGroupSymbol().setName(name);
                    } else {
                        es.setGroupSymbol(gs);
                    }
                }
            }

            @Override
            public void visit(Reference obj) {
                if (obj.getExpression() != null) {
                    visit(obj.getExpression());
                }
            }

        }, PreOrPostOrderNavigator.PRE_ORDER, true) {

            /**
             * Add to the navigation the visitation of expanded commands
             * which are inlined with clauses
             */
            @Override
            public void visit(UnaryFromClause obj) {
                super.visit(obj);
                if (obj.getExpandedCommand() != null && !obj.getGroup().isProcedure()) {
                    obj.getExpandedCommand().acceptVisitor(this);
                }
            }
        };
        command.acceptVisitor(nav);
    }

    private void assignWithClause(RelationalNode node, LinkedHashMap<String, WithQueryCommand> pushdownWith, boolean repeated) throws QueryPlannerException, TeiidComponentException {
        List<SubqueryContainer<?>> subCommands = new ArrayList<SubqueryContainer<?>>();
        if (node instanceof SubqueryAwareRelationalNode) {
            for (LanguageObject lo : ((SubqueryAwareRelationalNode)node).getObjects()) {
                ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(lo, subCommands);
                if (!subCommands.isEmpty()) {
                    for (SubqueryContainer<?> subquery : subCommands) {
                        if (subquery.getCommand().getProcessorPlan() instanceof RelationalPlan) {
                            assignWithClause(((RelationalPlan)subquery.getCommand().getProcessorPlan()).getRootNode(),
                                    pushdownWith, repeated | (subquery.getCommand().getCorrelatedReferences() != null && !subquery.getCommand().getCorrelatedReferences().asMap().isEmpty()));
                        }
                    }
                    subCommands.clear();
                }
            }
        }
        if (node instanceof PlanExecutionNode) {
            //need to check for nested relational plans.  these are created by things such as the semi-join optimization in rulemergevirtual
            ProcessorPlan plan = ((PlanExecutionNode)node).getProcessorPlan();
            if (plan instanceof RelationalPlan) {
                //other types of plans will be contained under non-relational plans, which would be out of scope for the parent with
                node = ((RelationalPlan)plan).getRootNode();
            }
        }
        if(node instanceof AccessNode) {
            AccessNode accessNode = (AccessNode) node;
            Map<GroupSymbol, RelationalPlan> subplans = accessNode.getSubPlans();
            if (subplans != null) {
                for (RelationalPlan subplan : subplans.values()) {
                    assignWithClause(subplan.getRootNode(), pushdownWith, false);
                }
            }
            Command command = accessNode.getCommand();
            if (command instanceof Insert && ((Insert)command).getQueryExpression() != null) {
                command = ((Insert)command).getQueryExpression();
            }
            if (command instanceof QueryCommand) {
                if (this.withGroups == null) {
                    this.withGroups = new TreeSet<GroupSymbol>(nonCorrelatedComparator);
                } else {
                    this.withGroups.clear();
                }
                GroupCollectorVisitor.getGroupsIgnoreInlineViewsAndEvaluatableSubqueries(command, this.withGroups);
                List<WithQueryCommand> with = new ArrayList<WithQueryCommand>();
                discoverWith(pushdownWith, command, with, new ArrayList<GroupSymbol>(this.withGroups));
                if (!with.isEmpty()) {
                    List<WithQueryCommand> pushed = new ArrayList<WithQueryCommand>(with);
                    final Map<GroupSymbol, Integer> order = new HashMap<GroupSymbol, Integer>();
                    for (WithQueryCommand withQueryCommand : pushdownWith.values()) {
                        order.put(withQueryCommand.getGroupSymbol(), order.size());
                    }
                    Collections.sort(with, new Comparator<WithQueryCommand>() {
                        @Override
                        public int compare(WithQueryCommand o1,
                                WithQueryCommand o2) {
                            return order.get(o1.getGroupSymbol()).compareTo(order.get(o2.getGroupSymbol()));
                        }
                    });
                    //pull up the with from the subqueries
                    for (int i = 0; i < with.size(); i++) {
                        WithQueryCommand wqc = with.get(i);
                        List<WithQueryCommand> with2 = wqc.getCommand().getWith();
                        if (with2 != null) {
                            with.addAll(i, with2);
                            i += with2.size();
                            wqc.getCommand().setWith(null);
                        }
                    }
                    QueryCommand query = (QueryCommand)command;
                    List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(query);
                    this.withGroups.clear();
                    for (WithQueryCommand wqc : with) {
                        withGroups.add(wqc.getGroupSymbol());
                    }
                    pullupWith(with, subqueries, withGroups);
                    if (query.getWith() != null) {
                        //we need to accumulate as a with clause could have been used at a lower scope
                        query.getWith().addAll(with);
                    } else {
                        query.setWith(with);
                    }
                    for (WithQueryCommand wqc : pushed) {
                        Object o = this.withPlanningState.pushdownState.get(wqc.getGroupSymbol().getName());
                        if (o == null) {
                            if (!repeated) {
                                if (accessNode.info != null) {
                                    o = accessNode.info.id;
                                } else {
                                    o = Boolean.FALSE;
                                }
                            } else {
                                o = Boolean.TRUE;
                            }
                        } else if (o instanceof Integer) {
                            //check for shared
                            if (accessNode.info == null || !o.equals(accessNode.info.id)) {
                                o = Boolean.TRUE;
                            }
                        } else {
                            o = Boolean.TRUE;
                        }
                        this.withPlanningState.pushdownState.put(wqc.getGroupSymbol().getName(), o);
                    }
                    //TODO: this should be based upon whether any of the need evaluated
                    accessNode.setShouldEvaluateExpressions(true);
                }
            }
        }

        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<node.getChildCount(); i++) {
            assignWithClause(children[i], pushdownWith, repeated);
        }
    }

    private void pullupWith(List<WithQueryCommand> with,
            List<SubqueryContainer<?>> subqueries, Set<GroupSymbol> knownWithGroups) {
        for (SubqueryContainer<?> subquery : subqueries) {
            if (subquery.getCommand() instanceof QueryCommand) {
                QueryCommand qc = (QueryCommand)subquery.getCommand();
                if (qc.getWith() != null) {
                    for (Iterator<WithQueryCommand> i = qc.getWith().iterator(); i.hasNext();) {
                        WithQueryCommand wqc = i.next();
                        if (knownWithGroups.contains(wqc.getGroupSymbol())) {
                            i.remove();
                        }
                    }
                    if (qc.getWith().isEmpty()) {
                        qc.setWith(null);
                    }
                }
                pullupWith(with, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(qc), knownWithGroups);
            }
        }
    }

    private void discoverWith(
            LinkedHashMap<String, WithQueryCommand> pushdownWith,
            Command command, List<WithQueryCommand> with, Collection<GroupSymbol> groups) throws QueryMetadataException, TeiidComponentException {
        for (GroupSymbol groupSymbol : groups) {
            if (!groupSymbol.isPushedCommonTable()) {
                continue;
            }
            WithQueryCommand clause = pushdownWith.get(groupSymbol.getNonCorrelationName());
            if (clause == null) {
                continue;
            }
            TreeSet<GroupSymbol> temp = new TreeSet<GroupSymbol>(nonCorrelatedComparator);
            GroupCollectorVisitor.getGroupsIgnoreInlineViewsAndEvaluatableSubqueries(clause.getCommand(), temp);
            temp.removeAll(this.withGroups);
            discoverWith(pushdownWith, command, with, temp);
            with.add(clause.clone());
            this.withGroups.add(clause.getGroupSymbol());
            command.setSourceHint(SourceHint.combine(command.getSourceHint(), clause.getCommand().getSourceHint()));
        }
    }

    public void initialize(Command command, IDGenerator idGenerator,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            AnalysisRecord analysisRecord, CommandContext context) {
        this.parentCommand = command;
        this.idGenerator = idGenerator;
        this.metadata = metadata;
        this.capFinder = capFinder;
        this.analysisRecord = analysisRecord;
        this.context = context;
    }

    private void connectSubqueryContainers(PlanNode plan, boolean skipPlanning) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        for (PlanNode node : getAllPossibleSubqueryNodes(plan)) {
            Set<GroupSymbol> groupSymbols = getGroupSymbols(node);
            List<SubqueryContainer<?>> subqueryContainers = node.getSubqueryContainers();
            planSubqueries(groupSymbols, node, subqueryContainers, false, skipPlanning);
            node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
        }
    }

    public void planSubqueries(
            Set<GroupSymbol> groupSymbols,
            PlanNode node, List<SubqueryContainer<?>> subqueryContainers, boolean isStackEntry, boolean skipPlanning)
            throws QueryMetadataException, TeiidComponentException,
            QueryPlannerException {
        if (subqueryContainers.isEmpty()){
            return;
        }
        Set<GroupSymbol> localGroupSymbols = groupSymbols;
        if (node != null) {
            if (node.getType() == NodeConstants.Types.JOIN) {
                localGroupSymbols = getGroupSymbols(node);
            } else if (node.getType() == NodeConstants.Types.GROUP) {
                localGroupSymbols = getGroupSymbols(node.getFirstChild());
            }
        }
        for (SubqueryContainer container : subqueryContainers) {
            if (container.getCommand().getProcessorPlan() != null) {
                continue;
            }
            //a clone is needed here because the command could get modified during planning
            Command subCommand = container.getCommand();
            if (!skipPlanning) {
                subCommand = (Command)container.getCommand().clone();
            }

            Set<PlanningStackEntry> entries = null;
            PlanningStackEntry stackEntry = null;
            if (isStackEntry) {
                entries = planningStack.get();
                stackEntry = createPlanningStackEntry(groupSymbols.iterator().next(), subCommand, false, entries);
            }
            try {
                ArrayList<Reference> correlatedReferences = new ArrayList<Reference>();
                CorrelatedReferenceCollectorVisitor.collectReferences(subCommand, localGroupSymbols, correlatedReferences, metadata);
                setCorrelatedReferences(container, correlatedReferences);

                List<SubqueryContainer<?>> containers = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container.getCommand());
                List<SubqueryContainer<?>> cloneContainers = null;

                if (!skipPlanning) {
                    if (!containers.isEmpty()) {
                        cloneContainers = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(subCommand);
                    }
                    Option mergedOption = mergeOptions(parentCommand.getOption(), subCommand.getOption());
                    if (mergedOption != null && mergedOption == parentCommand.getOption()) {
                        //clone for safety to prevent any unintended modifications
                        mergedOption = (Option) mergedOption.clone();
                    }
                    subCommand.setOption(mergedOption);
                    ProcessorPlan procPlan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
                    container.getCommand().setProcessorPlan(procPlan);
                }
                //ensure plans are set on the original nested subqueries
                if (!containers.isEmpty()) {
                    for (int i = 0; i < containers.size(); i++) {
                        Command c = containers.get(i).getCommand();
                        List<Reference> refs = new ArrayList<Reference>();
                        //re-detect the correlated references
                        CorrelatedReferenceCollectorVisitor.collectReferences(c, localGroupSymbols, refs, metadata);
                        setCorrelatedReferences(containers.get(i), refs);

                        if (cloneContainers != null) {
                            Command clone = cloneContainers.get(i).getCommand();
                            c.setProcessorPlan(clone.getProcessorPlan());
                        }
                    }
                }

                //update the correlated references to the appropriate grouping symbols
                if (node != null && node.getType() != NodeConstants.Types.JOIN && node.getType() != NodeConstants.Types.GROUP  && !correlatedReferences.isEmpty()) {
                    PlanNode grouping = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
                    if (grouping != null) {
                        SymbolMap map = (SymbolMap) grouping.getProperty(Info.SYMBOL_MAP);
                        SymbolMap symbolMap = container.getCommand().getCorrelatedReferences();
                        for (Map.Entry<ElementSymbol, Expression> entry : map.asMap().entrySet()) {
                            if (!(entry.getValue() instanceof ElementSymbol)) {
                                continue; //currently can't be correlated on an aggregate
                            }
                            ElementSymbol es = (ElementSymbol)entry.getValue();
                            if (symbolMap.getMappedExpression(es) != null) {
                                symbolMap.addMapping(es, entry.getKey());
                            }
                        }
                    }
                }
            } finally {
                if (entries != null) {
                    entries.remove(stackEntry);
                }
            }
        }
    }

    private void setCorrelatedReferences(SubqueryContainer<?> container,
            List<Reference> correlatedReferences) {
        if (!correlatedReferences.isEmpty()) {
            SymbolMap map = new SymbolMap();
            for (Reference reference : correlatedReferences) {
                map.addMapping(reference.getExpression(), reference.getExpression());
            }
            container.getCommand().setCorrelatedReferences(map);
        }
    }

    private static Set<GroupSymbol> getGroupSymbols(PlanNode plan) {
        Set<GroupSymbol> groupSymbols = new HashSet<GroupSymbol>();
        for (PlanNode source : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP, NodeConstants.Types.GROUP | NodeConstants.Types.SOURCE)) {
            groupSymbols.addAll(source.getGroups());
        }
        return groupSymbols;
    }

    /**
     * Distribute and "make (not) dependent" hints specified in the query into the
     * fully resolved query plan.  This is done after virtual group resolution so
     * that all groups in the plan are known.  The hint is attached to all SOURCE
     * nodes for each group that should be made dependent/not dependent.
     * @param groups List of groups (Strings) to be made dependent
     * @param plan The canonical plan
     */
    private void distributeDependentHints(Collection<String> groups, PlanNode plan, NodeConstants.Info hintProperty, Collection<? extends Object> vals)
        throws QueryMetadataException, TeiidComponentException {

        if(groups == null || groups.isEmpty()) {
            return;
        }
        // Get all source nodes
        List<PlanNode> nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE);
        Iterator<? extends Object> valIter = vals.iterator();
        // Walk through each dependent group hint and
        // attach to the correct source node
        for (String groupName : groups) {
            Object val = valIter.next();
            // Walk through nodes and apply hint to all that match group name
            boolean appliedHint = false;
            if (groupName.startsWith("@")) { //$NON-NLS-1$
                appliedHint = applyGlobalTableHint(plan, hintProperty, groupName.substring(1), val);
            }
            if (!appliedHint) {
                appliedHint = applyHint(nodes, groupName, hintProperty, val);
            }
            if(! appliedHint) {
                //check if it is partial group name
                Collection groupNames = metadata.getGroupsForPartialName(groupName);
                if(groupNames.size() == 1) {
                    groupName = (String)groupNames.iterator().next();
                    appliedHint = applyHint(nodes, groupName, hintProperty, val);
                }

                if(! appliedHint && this.analysisRecord.recordAnnotations()) {
                    String msg = QueryPlugin.Util.getString("ERR.015.004.0010", groupName); //$NON-NLS-1$
                    this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, msg, "ignoring hint", Priority.MEDIUM)); //$NON-NLS-1$
                }
            }
        }
    }

    private static boolean applyHint(List<PlanNode> nodes, String groupName, NodeConstants.Info hintProperty, Object value) {
        boolean appliedHint = false;
        for (PlanNode node : nodes) {
            GroupSymbol nodeGroup = node.getGroups().iterator().next();

            String sDefinition = nodeGroup.getDefinition();

            if (nodeGroup.getName().equalsIgnoreCase(groupName)
             || (sDefinition != null && sDefinition.equalsIgnoreCase(groupName)) ) {
                node.setProperty(hintProperty, value);
                appliedHint = true;
            }
        }
        return appliedHint;
    }

    private boolean applyGlobalTableHint(PlanNode plan,
            NodeConstants.Info hintProperty, String groupName, Object value) {
        GroupSymbol gs = new GroupSymbol(groupName);
        List<String> nameParts = StringUtil.split(gs.getName(), "."); //$NON-NLS-1$
        PlanNode root = plan;
        boolean found = true;
        for (int i = 0; i < nameParts.size() && found; i++) {
            String part = nameParts.get(i);
            boolean leaf = i == nameParts.size() - 1;
            found = false;
            for (PlanNode child : root.getChildren()) {
                for (PlanNode planNode : NodeEditor.findAllNodes(child, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE)) {
                    if (part.equalsIgnoreCase(planNode.getGroups().iterator().next().getShortName())) {
                        if (leaf) {
                            planNode.setProperty(hintProperty, value);
                            return true;
                        } else if (planNode.getChildren().isEmpty()) {
                            return false;
                        }
                        root = planNode;
                        found = true;
                        break;
                    }
                }
            }
        }
        return false;
    }

    public RuleStack buildRules() {
        RuleStack rules = new RuleStack();
        rules.setPlanner(this);
        rules.push(RuleConstants.COLLAPSE_SOURCE);

        rules.push(RuleConstants.PLAN_SORTS);

        //TODO: update plan sorts to take advantage of semi-join ordering
        if (hints.hasJoin || hints.hasCriteria || hints.hasRowBasedSecurity) {
            rules.push(RuleConstants.MERGE_CRITERIA);
        }

        rules.push(new RulePlanSubqueries(idGenerator, capFinder, analysisRecord, context, metadata));

        if(hints.hasJoin) {
            rules.push(RuleConstants.IMPLEMENT_JOIN_STRATEGY);
        }

        rules.push(RuleConstants.CALCULATE_COST);

        //rules.push(new RuleAssignOutputElements(true));

        if (hints.hasLimit && !hints.hasJoin) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }

        rules.push(new RuleAssignOutputElements(true));

        if (hints.hasRelationalProc) {
            rules.push(RuleConstants.PLAN_PROCEDURES);
        }
        if (hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_DEPENDENT);
            if (hints.hasLimit) {
                //run before choosing dependent based upon costing
                rules.push(RuleConstants.PUSH_LIMIT);
                //push limit is designed to run with the full output symbols
                rules.push(new RuleAssignOutputElements(false));
            }
        }
        if (hints.hasCriteria) {
            rules.push(RuleConstants.PUSH_LARGE_IN);
        }
        if(hints.hasAggregates) {
            rules.push(new RulePushAggregates(idGenerator));
            if (hints.hasJoin) {
                //we want to consider full pushdown prior to aggregate decomposition
                rules.push(new RuleChooseDependent(true));
            }
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_JOIN_STRATEGY);
            rules.push(RuleConstants.PLAN_OUTER_JOINS);
            rules.push(RuleConstants.RAISE_ACCESS);
            //after planning the joins, let the criteria be pushed back into place
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
            rules.push(RuleConstants.PLAN_JOINS);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CLEAN_CRITERIA);
            rules.push(RuleConstants.COPY_CRITERIA);
            rules.push(RuleConstants.PLAN_OUTER_JOINS);
        }
        rules.push(RuleConstants.RAISE_ACCESS);
        if (hints.hasFunctionBasedColumns) {
            rules.push(RuleConstants.SUBSTITUTE_EXPRESSIONS);
        }
        if (hints.hasSetQuery) {
            rules.push(RuleConstants.PLAN_UNIONS);
        }
        if(hints.hasCriteria || hints.hasJoin || hints.hasVirtualGroups) {
            //after copy criteria, it is no longer necessary to have phantom criteria nodes, so do some cleaning
            //also remove possible erroneous output elements
            rules.push(RuleConstants.CLEAN_CRITERIA);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.PUSH_NON_JOIN_CRITERIA);
        }
        if(hints.hasVirtualGroups) {
            rules.push(RuleConstants.MERGE_VIRTUAL);
        }
        if (hints.hasJoin && hints.hasSetQuery) {
            rules.push(RuleConstants.DECOMPOSE_JOIN);
            rules.push(RuleConstants.MERGE_VIRTUAL);
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        } else if(hints.hasCriteria) {
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        if (hints.hasJoin) {
            rules.push(RuleConstants.REMOVE_OPTIONAL_JOINS);
        }
        if (hints.hasVirtualGroups || hints.hasJoin) {
            //do initial filtering to make merging and optional join logic easier
            rules.push(new RuleAssignOutputElements(false));
        }
        if (hints.hasRowBasedSecurity && this.withPlanningState.withList.isEmpty()) {
            rules.push(new RuleApplySecurity());
        }
        rules.push(RuleConstants.PLACE_ACCESS);
        return rules;
    }

    public PlanNode executeRules(RuleStack rules, PlanNode plan)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        boolean debug = analysisRecord.recordDebug();
        while(! rules.isEmpty()) {
            if(debug) {
                analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            }

            OptimizerRule rule = rules.pop();
            if(debug) {
                analysisRecord.println("EXECUTING " + rule); //$NON-NLS-1$
            }

            plan = rule.execute(plan, metadata, capFinder, rules, analysisRecord, context);
            if(debug) {
                analysisRecord.println("\nAFTER: \n" + plan.nodeToString(true)); //$NON-NLS-1$
            }
        }
        return plan;
    }

    public PlanNode generatePlan(Command cmd) throws TeiidComponentException, TeiidProcessingException {
        //cascade the option clause nocache
        Option savedOption = option;
        option = mergeOptions(savedOption, cmd.getOption());

        PlanNode result = null;
        switch (cmd.getType()) {
        case Command.TYPE_QUERY:
            result = createQueryPlan((QueryCommand)cmd, null);
            break;
        case Command.TYPE_INSERT:
        case Command.TYPE_UPDATE:
        case Command.TYPE_DELETE:
        case Command.TYPE_CREATE:
        case Command.TYPE_DROP:
            result = createUpdatePlan(cmd);
            break;
        case Command.TYPE_STORED_PROCEDURE:
            result = createStoredProcedurePlan((StoredProcedure)cmd);
            break;
        default:
            throw new AssertionError("Invalid command type"); //$NON-NLS-1$
        }
        // Distribute make dependent hints as necessary
        if (cmd.getOption() != null) {
            if(cmd.getOption().getMakeDepOptions() != null) {
                distributeDependentHints(cmd.getOption().getDependentGroups(), result, NodeConstants.Info.MAKE_DEP, cmd.getOption().getMakeDepOptions());
            }
            if (cmd.getOption().getNotDependentGroups() != null) {
                distributeDependentHints(cmd.getOption().getNotDependentGroups(), result, NodeConstants.Info.MAKE_NOT_DEP, Collections.nCopies(cmd.getOption().getNotDependentGroups().size(), Boolean.TRUE));
            }
        }
        this.option = savedOption;
        return result;
    }

    static Option mergeOptions(Option parent, Option child) {
        if (child == null) {
            return parent;
        }
        if (parent != null && parent.isNoCache() && parent != child) { //merge no cache settings
            if (parent.getNoCacheGroups() == null || parent.getNoCacheGroups().isEmpty()) {
                if (child.getNoCacheGroups() != null) {
                    child.getNoCacheGroups().clear(); // full no cache
                }
            } else if (child.getNoCacheGroups() != null && !child.getNoCacheGroups().isEmpty()) {
                for (String noCache : parent.getNoCacheGroups()) {
                    child.addNoCacheGroup(noCache); // only groups
                }
            }
            child.setNoCache(true);
        }
        return child;
    }

    PlanNode createUpdatePlan(Command command) throws TeiidComponentException, TeiidProcessingException {
        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        // Set output columns
        List<Expression> cols = command.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, command);
        boolean usingTriggerAction = false;
        if (command instanceof ProcedureContainer) {
            ProcedureContainer container = (ProcedureContainer)command;
            usingTriggerAction = addNestedProcedure(sourceNode, container, container.getGroup().getMetadataID());
        }
        GroupSymbol target = ((TargetedCommand)command).getGroup();
        sourceNode.addGroup(target);
        Object id = getTrackableGroup(target, metadata);
        if (id != null) {
            context.accessedPlanningObject(id);
        }
        attachLast(projectNode, sourceNode);

        //for INTO query, attach source and project nodes
        if(!usingTriggerAction && command instanceof Insert){
            Insert insert = (Insert)command;
            if (insert.getQueryExpression() != null) {
                PlanNode plan = generatePlan(insert.getQueryExpression());
                attachLast(sourceNode, plan);
                mergeTempMetadata(insert.getQueryExpression(), insert);
                projectNode.setProperty(NodeConstants.Info.INTO_GROUP, insert.getGroup());
                if (this.sourceHint != null) {
                    projectNode.setProperty(Info.SOURCE_HINT, this.sourceHint);
                }
                if (insert.getConstraint() != null) {
                    projectNode.setProperty(NodeConstants.Info.CONSTRAINT, insert.getConstraint());
                }
                if (insert.isUpsert()) {
                    projectNode.setProperty(NodeConstants.Info.UPSERT, true);
                }
            }
        }
        if (usingTriggerAction && FrameUtil.getNestedPlan(projectNode) instanceof RelationalPlan) {
            sourceNode.removeFromParent();
            return sourceNode;
        }
        return projectNode;
    }

    private boolean addNestedProcedure(PlanNode sourceNode,
            ProcedureContainer container, Object metadataId) throws TeiidComponentException,
            QueryMetadataException, TeiidProcessingException {
        if (container instanceof StoredProcedure) {
            StoredProcedure sp = (StoredProcedure)container;
            if (sp.getProcedureID() instanceof Procedure) {
                context.accessedPlanningObject(sp.getProcedureID());
            }
        }
        for (SubqueryContainer<?> subqueryContainer : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container)) {
            if (subqueryContainer.getCommand().getCorrelatedReferences() != null) {
                continue;
            }
            List<Reference> correlatedReferences = new ArrayList<Reference>();
            CorrelatedReferenceCollectorVisitor.collectReferences(subqueryContainer.getCommand(), Arrays.asList(container.getGroup()), correlatedReferences, metadata);
            setCorrelatedReferences(subqueryContainer, correlatedReferences);
        }
        String cacheString = "transformation/" + container.getClass().getSimpleName().toUpperCase(); //$NON-NLS-1$
        Command c = (Command)metadata.getFromMetadataCache(metadataId, cacheString);
        if (c == null) {
            c = QueryResolver.expandCommand(container, metadata, analysisRecord);
            if (c != null) {
                if (c instanceof CreateProcedureCommand) {
                    //TODO: find a better way to do this
                    ((CreateProcedureCommand)c).setProjectedSymbols(container.getProjectedSymbols());
                }
                Request.validateWithVisitor(new ValidationVisitor(), metadata, c);
                metadata.addToMetadataCache(metadataId, cacheString, c.clone());
            }
        } else {
            c = (Command)c.clone();
            if (c instanceof CreateProcedureCommand) {
                //TODO: find a better way to do this
                ((CreateProcedureCommand)c).setProjectedSymbols(container.getProjectedSymbols());
            }
        }
        boolean checkRowBasedSecurity = true;
        if (!container.getGroup().isProcedure() && !metadata.isVirtualGroup(metadataId)) {
            Set<PlanningStackEntry> entries = planningStack.get();
            if (entries.contains(new PlanningStackEntry(container, container.getGroup()))) {
                checkRowBasedSecurity = false;
            }
        }
        if (checkRowBasedSecurity) {
            c = RowBasedSecurityHelper.checkUpdateRowBasedFilters(container, c, this);
        }
        if (c != null) {
            if (c instanceof TriggerAction) {
                TriggerAction ta = (TriggerAction)c;
                ProcessorPlan plan = new TriggerActionPlanner().optimize((ProcedureContainer)container.clone(), ta, idGenerator, metadata, capFinder, analysisRecord, context);
                sourceNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
                return true;
            }
            if (c.getCacheHint() != null) {
                if (container instanceof StoredProcedure) {
                    StoredProcedure sp = (StoredProcedure)container;
                    boolean noCache = isNoCacheGroup(metadata, sp.getProcedureID(), option);
                    if (!noCache) {
                        if (!context.isResultSetCacheEnabled()) {
                            recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.MEDIUM, "SimpleQueryResolver.procedure_cache_not_usable", container.getGroup(), "result set cache disabled"); //$NON-NLS-1$ //$NON-NLS-2$
                        } else if (!container.areResultsCachable()) {
                            recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.MEDIUM, "SimpleQueryResolver.procedure_cache_not_usable", container.getGroup(), "procedure performs updates"); //$NON-NLS-1$ //$NON-NLS-2$
                        } else if (LobManager.getLobIndexes(new ArrayList<ElementSymbol>(sp.getProcedureParameters().keySet())) != null) {
                            recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.MEDIUM, "SimpleQueryResolver.procedure_cache_not_usable", container.getGroup(), "lob parameters"); //$NON-NLS-1$ //$NON-NLS-2$
                        }
                        container.getGroup().setGlobalTable(true);
                        container.setCacheHint(c.getCacheHint());
                        recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.LOW, "SimpleQueryResolver.procedure_cache_used", container.getGroup()); //$NON-NLS-1$*/
                        return false;
                    }
                    recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.LOW, "SimpleQueryResolver.procedure_cache_not_used", container.getGroup()); //$NON-NLS-1$
                }
            }
            //skip the rewrite here, we'll do that in the optimizer
            //so that we know what the determinism level is.
            addNestedCommand(sourceNode, container.getGroup(), container, c, false, true);
        }

        List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container);

        if (c == null
                && container instanceof FilteredCommand) { //we force the evaluation of procedure params - TODO: inserts are fine except for nonpushdown functions on columns
            //for non-temp source queries, we must pre-plan subqueries to know if they can be pushed down
            boolean compensate = false;
            boolean isTemp = container.getGroup().isTempTable() && metadata.getModelID(container.getGroup().getMetadataID()) == TempMetadataAdapter.TEMP_MODEL;
            try {
                planSubqueries(container, c, subqueries, true);
            } catch (QueryPlannerException e) {
                if (!isTemp) {
                    throw e;
                }
                compensate = true;
            }

            if (!isTemp && !CriteriaCapabilityValidatorVisitor.canPushLanguageObject(container, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder, analysisRecord)) {
                compensate = true;
            }

            if (compensate) {
                //do a workaround of row-by-row processing for update/delete
                validateRowProcessing(container);

                //treat this as an update procedure
                if (container instanceof Update) {
                    c = QueryRewriter.createUpdateProcedure((Update)container, metadata, context);
                } else {
                    c = QueryRewriter.createDeleteProcedure((Delete)container, metadata, context);
                }
                addNestedCommand(sourceNode, container.getGroup(), container, c, false, true);
                return false;
            }
        }
        //plan any subqueries in criteria/parameters/values
        planSubqueries(container, c, subqueries, false);
        return false;
    }

    private void planSubqueries(ProcedureContainer container, Command c, List<SubqueryContainer<?>> subqueries, boolean initial)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {

        boolean isSourceTemp = c == null && container.getGroup().isTempTable() && metadata.getModelID(container.getGroup().getMetadataID()) == TempMetadataAdapter.TEMP_MODEL;

        for (SubqueryContainer<?> subqueryContainer : subqueries) {
            if (isSourceTemp) {
                if (subqueryContainer.getCommand().getCorrelatedReferences() == null) {
                    if (subqueryContainer instanceof Evaluatable) {
                        ((Evaluatable) subqueryContainer).setShouldEvaluate(true);
                    } else {
                        throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
                    }
                } else {
                    throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
                }
            }
            if (subqueryContainer.getCommand().getProcessorPlan() == null) {
                Command subCommand = initial?(Command) subqueryContainer.getCommand().clone():subqueryContainer.getCommand();
                ProcessorPlan plan = QueryOptimizer.optimizePlan(subCommand, metadata, null, capFinder, analysisRecord, context);
                subqueryContainer.getCommand().setProcessorPlan(plan);
            }

            if (c == null && !initial) {
                RuleCollapseSource.prepareSubquery(subqueryContainer);
            }
        }
    }

    void validateRowProcessing(ProcedureContainer container)
            throws TeiidComponentException, QueryMetadataException,
            QueryPlannerException {
        if (metadata.getUniqueKeysInGroup(container.getGroup().getMetadataID()).isEmpty()
                || !CapabilitiesUtil.supports(Capability.CRITERIA_COMPARE_EQ, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder)) {
             throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
        }
    }

    PlanNode createStoredProcedurePlan(StoredProcedure storedProc) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = attachProject(null, storedProc.getProjectedSymbols());

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, storedProc);
        addNestedProcedure(sourceNode, storedProc, storedProc.getProcedureID());

        hints.hasRelationalProc |= storedProc.isProcedureRelational();

        if (!hints.hasRowBasedSecurity && RowBasedSecurityHelper.applyRowSecurity(metadata, storedProc.getGroup(), context)) {
            hints.hasRowBasedSecurity = true;
        }

        // Set group on source node
        sourceNode.addGroup(storedProc.getGroup());

        attachLast(projectNode, sourceNode);

        return projectNode;
    }

    PlanNode createQueryPlan(QueryCommand command, List<OrderBy> parentOrderBys)
        throws TeiidComponentException, TeiidProcessingException {
        if (this.processWith) {
            //plan with
            List<WithQueryCommand> withList = command.getWith();
            if (withList != null) {
                processWith(command, withList);
            }
        }

        // Build canonical plan
        PlanNode node = null;
        if(command instanceof Query) {
            node = createQueryPlan((Query) command, parentOrderBys);
        } else {
            hints.hasSetQuery = true;
            SetQuery query = (SetQuery)command;
            SourceHint previous = this.sourceHint;
            this.sourceHint = SourceHint.combine(previous, query.getProjectedQuery().getSourceHint());
            //allow the affect of attaching grouping to tunnel up through the parent order bys
            if (command.getOrderBy() != null) {
                if (parentOrderBys == null) {
                    parentOrderBys = new ArrayList<OrderBy>(2);
                }
                parentOrderBys.add(command.getOrderBy());
            }
            PlanNode leftPlan = createQueryPlan( query.getLeftQuery(), parentOrderBys);
            if (command.getOrderBy() != null) {
                parentOrderBys.remove(parentOrderBys.size()-1);
            }
            PlanNode rightPlan = createQueryPlan( query.getRightQuery(), null);
            node = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
            node.setProperty(NodeConstants.Info.SET_OPERATION, query.getOperation());
            node.setProperty(NodeConstants.Info.USE_ALL, query.isAll());
            this.sourceHint = previous;
            attachLast(node, leftPlan);
            attachLast(node, rightPlan);
        }

        if(command.getOrderBy() != null) {
            node = attachSorting(node, command.getOrderBy());
        }

        if (command.getLimit() != null) {
            node = attachTupleLimit(node, command.getLimit(), hints);
        }

        return node;
    }

    private PlanNode createQueryPlan(Query query, List<OrderBy> parentOrderBys)
        throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {

        PlanNode plan = null;

        LinkedHashSet<WindowFunction> windowFunctions = new LinkedHashSet<WindowFunction>();

        if(query.getFrom() != null){
            FromClause fromClause = mergeClauseTrees(query.getFrom());

            PlanNode dummyRoot = new PlanNode();

            buildTree(fromClause, dummyRoot);

            plan = dummyRoot.getFirstChild();

            hints.hasJoin |= plan.getType() == NodeConstants.Types.JOIN;

            // Attach criteria on top
            if(query.getCriteria() != null) {
                plan = attachCriteria(plan, query.getCriteria(), false);
                hints.hasCriteria = true;
            }

            // Attach grouping node on top
            LinkedHashSet<AggregateSymbol> aggs = new LinkedHashSet<AggregateSymbol>();
            AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), aggs, null, null, windowFunctions, null);
            boolean hasGrouping = !aggs.isEmpty();
            if (query.getHaving() != null) {
                aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getHaving(), true));
                hasGrouping = true;
            }
            if (query.getGroupBy() != null) {
                hasGrouping = true;
            }
            if(hasGrouping) {
                plan = attachGrouping(plan, query, aggs, parentOrderBys);
            }

            // Attach having criteria node on top
            if(query.getHaving() != null) {
                plan = attachCriteria(plan, query.getHaving(), true);
                hints.hasCriteria = true;
            }

        }

        // Attach project on top
        plan = attachProject(plan, query.getSelect().getProjectedSymbols());
        if (query.getOrderBy() != null) {
            AggregateSymbolCollectorVisitor.getAggregates(query.getOrderBy(), null, null, null, windowFunctions, null);
        }
        if (!windowFunctions.isEmpty()) {
            plan.setProperty(Info.HAS_WINDOW_FUNCTIONS, true);
        }

        // Attach dup removal on top
        if(query.getSelect().isDistinct()) {
            plan = attachDupRemoval(plan);
        }

        return plan;
    }

    /**
     * Merges the from clause into a single join predicate if there are more than 1 from clauses
     */
    private static FromClause mergeClauseTrees(From from) {
        List<FromClause> clauses = from.getClauses();

        while (clauses.size() > 1) {
            FromClause first = from.getClauses().remove(0);
            FromClause second = from.getClauses().remove(0);
            JoinPredicate jp = new JoinPredicate(first, second, JoinType.JOIN_CROSS);
            clauses.add(0, jp);
        }

        return clauses.get(0);
    }

    /**
     * Build a join plan based on the structure in a clause.  These structures should be
     * essentially the same tree, but with different objects and details.
     * @param clause Clause to build tree from
     * @param parent Parent node to attach join node structure to
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws TeiidProcessingException
     */
    void buildTree(FromClause clause, final PlanNode parent)
        throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {

        PlanNode node = null;

        if(clause instanceof UnaryFromClause) {
            // No join required
            UnaryFromClause ufc = (UnaryFromClause)clause;
            GroupSymbol group = ufc.getGroup();
            if (metadata.isVirtualGroup(group.getMetadataID()) && !group.isTempGroupSymbol()) {
                hints.hasVirtualGroups = true;
            }
            if (!hints.hasRowBasedSecurity && RowBasedSecurityHelper.applyRowSecurity(metadata, group, context)) {
                hints.hasRowBasedSecurity = true;
            }
            if (metadata.getFunctionBasedExpressions(group.getMetadataID()) != null) {
                hints.hasFunctionBasedColumns = true;
            }
            boolean planningStackEntry = true;
            Command nestedCommand = ufc.getExpandedCommand();
            if (nestedCommand != null) {
                //only proc relational counts toward the planning stack
                //other paths are inlining, so there isn't a proper virtual layer
                if (!group.isProcedure()) {
                    planningStackEntry = false;
                    hints.hasVirtualGroups = true;
                }
            } else if (!group.isProcedure()) {
                Object id = getTrackableGroup(group, metadata);
                if (id != null) {
                    context.accessedPlanningObject(id);
                }
                if (!group.isTempGroupSymbol() && metadata.isVirtualGroup(group.getMetadataID())) {
                    nestedCommand = resolveVirtualGroup(group);
                }
            }
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            if (ufc.isNoUnnest()) {
                node.setProperty(Info.NO_UNNEST, Boolean.TRUE);
            }
            node.addGroup(group);
            if (nestedCommand != null) {
                UpdateInfo info = ProcedureContainerResolver.getUpdateInfo(group, metadata);
                if (info != null && info.getPartitionInfo() != null && !info.getPartitionInfo().isEmpty()) {
                    Map<ElementSymbol, List<Set<Constant>>> partitionInfo = info.getPartitionInfo();
                    if (group.getDefinition() != null) {
                        partitionInfo = remapPartitionInfo(group, partitionInfo);
                    }
                    node.setProperty(NodeConstants.Info.PARTITION_INFO, partitionInfo);
                }
                SourceHint previous = this.sourceHint;
                if (nestedCommand.getSourceHint() != null) {
                    this.sourceHint = SourceHint.combine(previous, nestedCommand.getSourceHint());
                }
                addNestedCommand(node, group, nestedCommand, nestedCommand, true, planningStackEntry);
                this.sourceHint = previous;
            } else if (this.sourceHint != null) {
                node.setProperty(Info.SOURCE_HINT, this.sourceHint);
            }
            if (group.getName().contains(RulePlaceAccess.RECONTEXT_STRING)) {
                this.context.getGroups().add(group.getName());
            }
            parent.addLastChild(node);
        } else if(clause instanceof JoinPredicate) {
            JoinPredicate jp = (JoinPredicate) clause;

            // Set up new join node corresponding to this join predicate
            node = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            node.setProperty(NodeConstants.Info.JOIN_TYPE, jp.getJoinType());
            node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
            node.setProperty(NodeConstants.Info.JOIN_CRITERIA, jp.getJoinCriteria());
            if (jp.isPreserve()) {
                node.setProperty(Info.PRESERVE, Boolean.TRUE);
            }
            // Attach join node to parent
            parent.addLastChild(node);

            // Handle each child
            FromClause[] clauses = new FromClause[] {jp.getLeftClause(), jp.getRightClause()};
            for(int i=0; i<2; i++) {
                if (jp.isPreserve() && clauses[i] instanceof JoinPredicate) {
                    ((JoinPredicate)clauses[i]).setPreserve(true);
                }
                buildTree(clauses[i], node);
                // Add groups to joinNode
                node.addGroups(node.getLastChild().getGroups());
            }
        } else if (clause instanceof SubqueryFromClause) {
            SubqueryFromClause sfc = (SubqueryFromClause)clause;
            GroupSymbol group = sfc.getGroupSymbol();
            Command nestedCommand = sfc.getCommand();
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            if (sfc.isLateral()) {
                sfc.getCommand().setCorrelatedReferences(getCorrelatedReferences(parent, node, sfc));
            }
            if (sfc.isNoUnnest()) {
                node.setProperty(Info.NO_UNNEST, Boolean.TRUE);
            }
            SourceHint previous = this.sourceHint;
            if (nestedCommand.getSourceHint() != null) {
                this.sourceHint = SourceHint.combine(previous, nestedCommand.getSourceHint());
            }
            node.addGroup(group);
            addNestedCommand(node, group, nestedCommand, nestedCommand, true, false);
            this.sourceHint = previous;
            if (nestedCommand instanceof SetQuery) {
                Map<ElementSymbol, List<Set<Constant>>> partitionInfo = PartitionAnalyzer.extractPartionInfo((SetQuery)nestedCommand, ResolverUtil.resolveElementsInGroup(group, metadata));
                if (!partitionInfo.isEmpty()) {
                    node.setProperty(NodeConstants.Info.PARTITION_INFO, partitionInfo);
                }
            }
            hints.hasVirtualGroups = true;
            parent.addLastChild(node);
            if (group.getName().contains(RulePlaceAccess.RECONTEXT_STRING)) {
                this.context.getGroups().add(group.getName());
            }
        } else if (clause instanceof TableFunctionReference) {
            TableFunctionReference tt = (TableFunctionReference)clause;
            GroupSymbol group = tt.getGroupSymbol();
            if (group.getName().contains(RulePlaceAccess.RECONTEXT_STRING)) {
                this.context.getGroups().add(group.getName());
            }
            //special handling to convert array table into a mergable construct
            if (parent.getType() == NodeConstants.Types.JOIN && tt instanceof ArrayTable) {
                JoinType jt = (JoinType) parent.getProperty(Info.JOIN_TYPE);
                if (jt != JoinType.JOIN_FULL_OUTER && parent.getChildCount() > 0) {
                    ArrayTable at = (ArrayTable)tt;
                    //rewrite if free of subqueries
                    if ((at.getSingleRow() == null || at.getSingleRow()) && ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(at).isEmpty()) {
                        List<ElementSymbol> symbols = at.getProjectedSymbols();
                        FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
                        FunctionDescriptor descriptor = funcLib.findFunction(FunctionLibrary.ARRAY_GET,
                                new Class[] { DataTypeManager.DefaultDataClasses.OBJECT, DataTypeManager.DefaultDataClasses.INTEGER });
                        Query query = new Query();
                        Select select = new Select();
                        query.setSelect(select);
                        for (int i = 0; i < symbols.size(); i++) {
                            ElementSymbol es = symbols.get(i);
                            Function f = new Function(FunctionLibrary.ARRAY_GET, new Expression[] {(Expression) at.getArrayValue().clone(), new Constant(i + 1)});
                            f.setType(DataTypeManager.DefaultDataClasses.OBJECT);
                            f.setFunctionDescriptor(descriptor);
                            Expression ex = f;
                            if (es.getType() != DataTypeManager.DefaultDataClasses.OBJECT) {
                                ex = ResolverUtil.getConversion(ex, DataTypeManager.DefaultDataTypes.OBJECT, DataTypeManager.getDataTypeName(es.getType()), false, metadata.getFunctionLibrary());
                            }
                            select.addSymbol(new AliasSymbol(es.getShortName(), ex));
                        }
                        SubqueryFromClause sfc = new SubqueryFromClause(at.getGroupSymbol(), query);
                        sfc.setLateral(true);
                        buildTree(sfc, parent);
                        if (!jt.isOuter()) {
                            //insert is null criteria
                            IsNullCriteria criteria = new IsNullCriteria((Expression) at.getArrayValue().clone());
                            if (sfc.getCommand().getCorrelatedReferences() != null) {
                                RulePlanSubqueries.ReferenceReplacementVisitor rrv = new RulePlanSubqueries.ReferenceReplacementVisitor(sfc.getCommand().getCorrelatedReferences());
                                PreOrPostOrderNavigator.doVisit(criteria, rrv, PreOrPostOrderNavigator.PRE_ORDER);
                            }
                            criteria.setNegated(true);
                            if (jt == JoinType.JOIN_CROSS) {
                                parent.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
                            }
                            List<Criteria> joinCriteria = (List<Criteria>) parent.getProperty(Info.JOIN_CRITERIA);
                            if (joinCriteria == null) {
                                joinCriteria = new ArrayList<Criteria>(2);
                            }
                            joinCriteria.add(criteria);
                            parent.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
                        }
                        return;
                    }
                }
            }
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.setProperty(NodeConstants.Info.TABLE_FUNCTION, tt);
            tt.setCorrelatedReferences(getCorrelatedReferences(parent, node, tt));
            node.addGroup(group);
            parent.addLastChild(node);
        } else {
            throw new AssertionError("Unknown Type"); //$NON-NLS-1$
        }

        if (clause.isOptional()) {
            node.setProperty(NodeConstants.Info.IS_OPTIONAL, Boolean.TRUE);
        }

        if (clause.getMakeDep() != null) {
            node.setProperty(NodeConstants.Info.MAKE_DEP, clause.getMakeDep());
        } else if (clause.isMakeNotDep()) {
            node.setProperty(NodeConstants.Info.MAKE_NOT_DEP, Boolean.TRUE);
        }
        if (clause.getMakeInd() != null) {
            node.setProperty(NodeConstants.Info.MAKE_IND, clause.getMakeInd());
        }
    }

    public static Map<ElementSymbol, List<Set<Constant>>> remapPartitionInfo(
            GroupSymbol group,
            Map<ElementSymbol, List<Set<Constant>>> partitionInfo) {
        Map<ElementSymbol, List<Set<Constant>>> aliasedPartitionInfo = new LinkedHashMap<ElementSymbol, List<Set<Constant>>>();
        for (Map.Entry<ElementSymbol, List<Set<Constant>>> entry : partitionInfo.entrySet()) {
            ElementSymbol es = entry.getKey().clone();
            es.setGroupSymbol(group.clone());
            aliasedPartitionInfo.put(es, entry.getValue());
        }
        return aliasedPartitionInfo;
    }

    public static Object getTrackableGroup(GroupSymbol group, QueryMetadataInterface metadata)
            throws TeiidComponentException, QueryMetadataException {
        Object metadataID = group.getMetadataID();
        if (group.isTempGroupSymbol()) {
            QueryMetadataInterface qmi = metadata.getSessionMetadata();
            try {
                //exclude proc scoped temp tables
                if (group.isGlobalTable()) {
                    return metadataID;
                }
                if (qmi != null) {
                    Object mid = qmi.getGroupID(group.getNonCorrelationName());
                    if (mid == metadataID || metadata.isVirtualGroup(metadataID)) {
                        //global temp should use the session metadata reference instead
                        return mid;
                    }
                }
            } catch (QueryMetadataException e) {
                //not a session table
            }
            if (metadata.isVirtualGroup(metadataID)) {
                //global temp table
                return metadataID;
            }
        } else {
            return metadataID;
        }
        return null;
    }

    private SymbolMap getCorrelatedReferences(PlanNode parent, PlanNode node,
            LanguageObject lo) {
        PlanNode rootJoin = parent;
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>(rootJoin.getGroups());
        while (rootJoin.getParent() != null && rootJoin.getParent().getType() == NodeConstants.Types.JOIN) {
            rootJoin = rootJoin.getParent();
            //accumulate groups as we go, as intermediate joins may not contribute groups to the final join
            groups.addAll(rootJoin.getGroups());
        }
        List<Reference> correlatedReferences = new ArrayList<Reference>();
        CorrelatedReferenceCollectorVisitor.collectReferences(lo, groups, correlatedReferences, metadata);

        if (correlatedReferences.isEmpty()) {
            return null;
        }
        SymbolMap map = new SymbolMap();
        for (Reference reference : correlatedReferences) {
            map.addMapping(reference.getExpression(), reference.getExpression());
        }
        node.setProperty(NodeConstants.Info.CORRELATED_REFERENCES, map);
        return map;
    }

    private void addNestedCommand(PlanNode node,
            GroupSymbol group, Command nestedCommand, Command toPlan, boolean merge, boolean isStackEntry) throws TeiidComponentException, QueryMetadataException, TeiidProcessingException {
        if (nestedCommand instanceof QueryCommand) {
            //remove unnecessary order by
            QueryCommand queryCommand = (QueryCommand)nestedCommand;
            if (queryCommand.getLimit() == null) {
                queryCommand.setOrderBy(null);
            }
        }
        Set<PlanningStackEntry> entries = null;
        PlanningStackEntry entry = null;
        if (isStackEntry) {
            entries = planningStack.get();
            entry = createPlanningStackEntry(group, nestedCommand, toPlan.getType() == Command.TYPE_UPDATE_PROCEDURE, entries);
        }
        try {
            node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);

            if (merge) {
                mergeTempMetadata(nestedCommand, parentCommand);
                PlanNode childRoot = generatePlan(nestedCommand);
                node.addFirstChild(childRoot);
                List<Expression> projectCols = nestedCommand.getProjectedSymbols();
                SymbolMap map = SymbolMap.createSymbolMap(group, projectCols, metadata);
                node.setProperty(NodeConstants.Info.SYMBOL_MAP, map);
            } else {
                QueryMetadataInterface actualMetadata = metadata;
                if (actualMetadata instanceof TempMetadataAdapter) {
                    actualMetadata = ((TempMetadataAdapter)metadata).getMetadata();
                }
                ProcessorPlan plan = QueryOptimizer.optimizePlan(toPlan, actualMetadata, idGenerator, capFinder, analysisRecord, context);
                //hack for the optimizer not knowing the containing command when forming the plan
                if (nestedCommand instanceof StoredProcedure && plan instanceof ProcedurePlan) {
                    StoredProcedure container = (StoredProcedure)nestedCommand;
                    ProcedurePlan pp = (ProcedurePlan)plan;
                    pp.setUpdateCount(container.getUpdateCount());
                    if (container.returnParameters()) {
                        List<ElementSymbol> outParams = new LinkedList<ElementSymbol>();
                        for (SPParameter param : container.getParameters()) {
                            if (param.getParameterType() == SPParameter.RETURN_VALUE) {
                                outParams.add(param.getParameterSymbol());
                            }
                        }
                        for (SPParameter param : container.getParameters()) {
                            if (param.getParameterType() == SPParameter.INOUT ||
                                    param.getParameterType() == SPParameter.OUT) {
                                outParams.add(param.getParameterSymbol());
                            }
                        }
                        if (outParams.size() > 0) {
                            pp.setOutParams(outParams);
                        }
                    }
                    pp.setParams(container.getProcedureParameters());
                }
                node.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
            }
        } finally {
            if (entries != null) {
                entries.remove(entry);
            }
        }
    }

    public PlanningStackEntry createPlanningStackEntry(GroupSymbol group,
            Command nestedCommand, boolean isUpdateProcedure,
            Set<PlanningStackEntry> entries) throws TeiidComponentException,
            QueryMetadataException, QueryPlannerException {
        PlanningStackEntry entry = new PlanningStackEntry(nestedCommand, group);
        if (!entries.add(entry)) {
            if (isUpdateProcedure && !metadata.isVirtualGroup(group.getMetadataID())) {
                //must be a compensating update/delete
                throw new QueryPlannerException(QueryPlugin.Event.TEIID30254, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30254, nestedCommand));
            }
            throw new QueryPlannerException(QueryPlugin.Event.TEIID31124, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31124, nestedCommand.getClass().getSimpleName(), group.getNonCorrelationName(), entries));
        }
        return entry;
    }

    /**
     * Attach all criteria above the join nodes.  The optimizer will push these
     * criteria down to the appropriate source.
     * @param plan Existing plan, which joins all source groups
     * @param criteria Criteria from query
     * @return Updated tree
     */
    private static PlanNode attachCriteria(PlanNode plan, Criteria criteria, boolean isHaving) {
        List<Criteria> crits = Criteria.separateCriteriaByAnd(criteria);

        for (Criteria crit : crits) {
            PlanNode critNode = createSelectNode(crit, isHaving);
            attachLast(critNode, plan);
            plan = critNode;
        }

        return plan;
    }

    public static PlanNode createSelectNode(final Criteria crit, boolean isHaving) {
        PlanNode critNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
        critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, crit);
        if (isHaving && (!ElementCollectorVisitor.getAggregates(crit, false).isEmpty()
            //it's not needed to check for correlated grouping expression as those will be replaced as expected when needed
                /*|| !ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(crit).isEmpty()*/)) {
            critNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        }
        if (crit instanceof DependentSetCriteria) {
            critNode.setProperty(Info.IS_DEPENDENT_SET, true);
        }
        // Add groups to crit node
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(crit));
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(critNode.getCorrelatedReferenceElements()));
        return critNode;
    }

    /**
     * Attach a grouping node at top of tree.
     * @param plan Existing plan
     * @param aggs
     * @param parentOrderBys
     * @return Updated plan
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    private PlanNode attachGrouping(PlanNode plan, Query query, Collection<AggregateSymbol> aggs, List<OrderBy> parentOrderBys) throws QueryMetadataException, TeiidComponentException {
        GroupBy groupBy = query.getGroupBy();
        List<Expression> groupingCols = null;
        PlanNode groupNode = NodeFactory.getNewNode(NodeConstants.Types.GROUP);
        if (groupBy != null) {
            groupingCols = groupBy.getSymbols();
            if (groupBy.isRollup()) {
                groupNode.setProperty(Info.ROLLUP, Boolean.TRUE);
            }
        }

        Map<Expression, ElementSymbol> mapping = buildGroupingNode(aggs, groupingCols, groupNode, this.context, this.idGenerator).inserseMapping();

        attachLast(groupNode, plan);

        // special handling if there is an expression in the grouping.  we need to create the appropriate
        // correlations
        Map<Expression, Expression> subMapping = null;
        for (Map.Entry<Expression, ElementSymbol> entry : mapping.entrySet()) {
            if (entry.getKey() instanceof ElementSymbol) {
                continue;
            }
            ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
                @Override
                public Expression replaceExpression(Expression element) {
                    if (element instanceof ElementSymbol) {
                        return new Reference((ElementSymbol)element);
                    }
                    return element;
                }
            };
            Expression key = (Expression)entry.getKey().clone();
            PostOrderNavigator.doVisit(key, emv);
            if (subMapping == null) {
                subMapping = new HashMap<Expression, Expression>();
            }
            ElementSymbol value = entry.getValue().clone();
            value.setIsExternalReference(true);
            subMapping.put(key, new Reference(value));
        }
        replaceExpressions(query.getHaving(), mapping, subMapping);
        replaceExpressions(query.getSelect(), mapping, subMapping);
        replaceExpressions(query.getOrderBy(), mapping, subMapping);
        if (parentOrderBys != null) {
            for (OrderBy parentOrderBy : parentOrderBys) {
                replaceExpressions(parentOrderBy, mapping, subMapping);
            }
        }
        // Mark in hints
        hints.hasAggregates = true;

        return groupNode;
    }

    private void replaceExpressions(LanguageObject lo,
            Map<Expression, ElementSymbol> mapping,
            Map<Expression, Expression> subMapping) {
        if (lo == null) {
            return;
        }
        ExpressionMappingVisitor.mapExpressions(lo, mapping);
        if (subMapping != null) {
            //support only 1 level of correlation
            for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(lo)) {
                ExpressionMappingVisitor.mapExpressions(container.getCommand(), subMapping);
            }
        }
    }

    /**
     * Build a grouping node that introduces a anon group (without a inline view source node)
     */
    public static SymbolMap buildGroupingNode(
            Collection<AggregateSymbol> aggs, List<? extends Expression> groupingCols,
            PlanNode groupNode, CommandContext cc, IDGenerator idGenerator) throws QueryMetadataException, TeiidComponentException {
        SymbolMap map = new SymbolMap();
        aggs = LanguageObject.Util.deepClone(aggs, AggregateSymbol.class);
        groupingCols = LanguageObject.Util.deepClone(groupingCols, Expression.class);
        GroupSymbol group = new GroupSymbol("anon_grp" + idGenerator.nextInt()); //$NON-NLS-1$
        if (!cc.getGroups().add(group.getName())) {
            group = RulePlaceAccess.recontextSymbol(group, cc.getGroups());
        }

        TempMetadataStore tms = new TempMetadataStore();

        int i = 0;

        List<AliasSymbol> symbols = new LinkedList<AliasSymbol>();
        List<Expression> targets = new LinkedList<Expression>();

        if(groupingCols != null) {
            groupNode.setProperty(NodeConstants.Info.GROUP_COLS, groupingCols);
            groupNode.addGroups(GroupsUsedByElementsVisitor.getGroups(groupingCols));
            for (Expression ex : groupingCols) {
                AliasSymbol as = new AliasSymbol("gcol" + i++, new ExpressionSymbol("expr", ex)); //$NON-NLS-1$ //$NON-NLS-2$
                targets.add(ex);
                symbols.add(as);
            }
        }

        i = 0;
        for (AggregateSymbol ex : aggs) {
            AliasSymbol as = new AliasSymbol("agg" + i++, new ExpressionSymbol("expr", ex)); //$NON-NLS-1$ //$NON-NLS-2$
            targets.add(ex);
            symbols.add(as);
        }

        group.setMetadataID(tms.addTempGroup(group.getName(), symbols, true, false));
        Iterator<Expression> targetIter = targets.iterator();
        for (ElementSymbol es : ResolverUtil.resolveElementsInGroup(group, new TempMetadataAdapter(new BasicQueryMetadata(), tms))) {
            Expression target = targetIter.next();
            es.setAggregate(target instanceof AggregateSymbol);
            map.addMapping(es, target);
        }

        groupNode.setProperty(NodeConstants.Info.SYMBOL_MAP, map);
        groupNode.addGroup(group);
        return map;
    }

    /**
     * Attach SORT node at top of tree.  The SORT may be pushed down to a source (or sources)
     * if possible by the optimizer.
     * @param plan Existing plan
     * @param orderBy Sort description from the query
     * @return Updated plan
     */
    private static PlanNode attachSorting(PlanNode plan, OrderBy orderBy) {
        PlanNode sortNode = createSortNode(orderBy);

        attachLast(sortNode, plan);
        return sortNode;
    }

    public static PlanNode createSortNode(OrderBy orderBy) {
        PlanNode sortNode = NodeFactory.getNewNode(NodeConstants.Types.SORT);

        sortNode.setProperty(NodeConstants.Info.SORT_ORDER, orderBy);
        if (orderBy.hasUnrelated()) {
            sortNode.setProperty(Info.UNRELATED_SORT, true);
        }
        sortNode.addGroups(GroupsUsedByElementsVisitor.getGroups(orderBy));
        return sortNode;
    }

    private static PlanNode attachTupleLimit(PlanNode plan, Limit limit, PlanHints hints) {
        hints.hasLimit = true;
        PlanNode limitNode = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);

        boolean attach = false;
        if (limit.getOffset() != null) {
            limitNode.setProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT, limit.getOffset());
            attach = true;
        }
        if (limit.getRowLimit() != null) {
            limitNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, limit.getRowLimit());
            attach = true;
        }
        if (attach) {
            if (limit.isImplicit()) {
                limitNode.setProperty(Info.IS_IMPLICIT_LIMIT, true);
            }
            if (!limit.isStrict()) {
                limitNode.setProperty(Info.IS_NON_STRICT, true);
            }
            attachLast(limitNode, plan);
            plan = limitNode;
        }
        return plan;
    }

    /**
     * Attach DUP_REMOVE node at top of tree.  The DUP_REMOVE may be pushed down
     * to a source (or sources) if possible by the optimizer.
     * @param plan Existing plan
     * @return Updated plan
     */
    private static PlanNode attachDupRemoval(PlanNode plan) {
        PlanNode dupNode = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
        attachLast(dupNode, plan);
        return dupNode;
    }

    private static PlanNode attachProject(PlanNode plan, List<? extends Expression> select) {
        PlanNode projectNode = createProjectNode(select);

        attachLast(projectNode, plan);
        return projectNode;
    }

    public static PlanNode createProjectNode(List<? extends Expression> select) {
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, select);

        // Set groups
        projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(select));
        return projectNode;
    }

    static final void attachLast(PlanNode parent, PlanNode child) {
        if(child != null) {
            parent.addLastChild(child);
        }
    }

    /**
     * Adds temp metadata (if any) of child command to temp metadata
     * (if any) of parent command.
     * @param childCommand
     * @param parentCommand
     */
    static void mergeTempMetadata(
        Command childCommand,
        Command parentCommand) {
        TempMetadataStore childTempMetadata = childCommand.getTemporaryMetadata();
        if (childTempMetadata != null && !childTempMetadata.getData().isEmpty()){
            // Add to parent temp metadata
            TempMetadataStore parentTempMetadata = parentCommand.getTemporaryMetadata();
            if (parentTempMetadata == null){
                parentCommand.setTemporaryMetadata(childTempMetadata);
            } else {
                parentTempMetadata.getData().putAll(childTempMetadata.getData());
            }
        }
    }

    private Command resolveVirtualGroup(GroupSymbol virtualGroup)
    throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {

        QueryNode qnode = null;

        Object metadataID = virtualGroup.getMetadataID();
        boolean noCache = isNoCacheGroup(metadata, metadataID, option);
        boolean isMaterializedGroup = metadata.hasMaterialization(metadataID);
        String cacheString = SQLConstants.Reserved.SELECT;

        if( isMaterializedGroup) {
            Object matMetadataId = metadata.getMaterialization(metadataID);
            String matTableName = null;
            CacheHint hint = null;
            boolean isImplicitGlobal = matMetadataId == null;
            if (isImplicitGlobal) {
                TempMetadataID tid = context.getGlobalTableStore().getGlobalTempTableMetadataId(metadataID);
                matTableName = tid.getID();
                hint = tid.getCacheHint();
                matMetadataId = tid;
            } else {
                matTableName = metadata.getFullName(matMetadataId);
            }

            if(noCache){
                //not use cache
                qnode = metadata.getVirtualPlan(metadataID);
                //TODO: update the table for defaultMat
                recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.materialized_table_not_used", virtualGroup, matTableName); //$NON-NLS-1$
            }else{
                this.context.accessedPlanningObject(matMetadataId);
                qnode = new QueryNode(null);
                List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
                for (ElementSymbol el : ResolverUtil.resolveElementsInGroup(virtualGroup, metadata)) {
                    symbols.add(new ElementSymbol(el.getShortName()));
                }

                Query query = createMatViewQuery(metadataID, matMetadataId, matTableName, symbols, isImplicitGlobal);
                query.setCacheHint(hint);
                qnode.setCommand(query);
                cacheString = "matview"; //$NON-NLS-1$
                recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.Query_was_redirected_to_Mat_table", virtualGroup, matTableName); //$NON-NLS-1$
            }
        } else {
            // Not a materialized view - query the primary transformation
            qnode = metadata.getVirtualPlan(metadataID);
        }

        Command result = (Command)QueryResolver.resolveView(virtualGroup, qnode, cacheString, metadata, false).getCommand().clone();
        return QueryRewriter.rewrite(result, metadata, context);
    }

    public static Query createMatViewQuery(Object matMetadataId, String matTableName, List<? extends Expression> select, boolean isGlobal) {
        Query query = new Query();
        query.setSelect(new Select(select));
        GroupSymbol gs = new GroupSymbol(matTableName);
        gs.setGlobalTable(isGlobal);
        gs.setMetadataID(matMetadataId);
        query.setFrom(new From(Arrays.asList(new UnaryFromClause(gs))));
        return query;
    }

    public Query createMatViewQuery(Object viewMatadataId, Object matMetadataId, String matTableName, List<? extends Expression> select, boolean isGlobal) throws QueryMetadataException, TeiidComponentException {
        Query query = new Query();
        query.setSelect(new Select(select));
        GroupSymbol gs = new GroupSymbol(matTableName);
        gs.setGlobalTable(isGlobal);
        gs.setMetadataID(matMetadataId);
        query.setFrom(new From(Arrays.asList(new UnaryFromClause(gs))));

        boolean allow = false;
        if (!(viewMatadataId instanceof TempMetadataID)) {
            allow = Boolean.parseBoolean(metadata.getExtensionProperty(viewMatadataId, MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, false));
            allow &= metadata.getMaterialization(viewMatadataId) != null;
        }
        if (allow) {
            String onErrorAction = metadata.getExtensionProperty(viewMatadataId, MaterializationMetadataRepository.MATVIEW_ONERROR_ACTION, false);
            if (onErrorAction == null || !ErrorAction.IGNORE.name().equalsIgnoreCase(onErrorAction)) {
                gs.setCheckMatStatus(viewMatadataId);
            }
        }
        return query;
    }

    public static boolean isNoCacheGroup(QueryMetadataInterface metadata,
                                          Object metadataID,
                                          Option option) throws QueryMetadataException,
                                                        TeiidComponentException {
        if(option == null || !option.isNoCache()){
            return false;
        }
        if(option.getNoCacheGroups() == null || option.getNoCacheGroups().isEmpty()){
            //only OPTION NOCACHE, no group specified
            return true;
        }
        String fullName = metadata.getFullName(metadataID);
        for (String groupName : option.getNoCacheGroups()) {
            if(groupName.equalsIgnoreCase(fullName)){
                return true;
            }
        }
        return false;
    }

    private static void recordAnnotation(AnalysisRecord analysis, String type, Priority priority, String msgKey, Object... parts) {
        if (analysis.recordAnnotations()) {
            Annotation annotation = new Annotation(type,
                    QueryPlugin.Util.getString(msgKey, parts),
                    null,
                    priority);
            analysis.addAnnotation(annotation);
        }
    }

}
