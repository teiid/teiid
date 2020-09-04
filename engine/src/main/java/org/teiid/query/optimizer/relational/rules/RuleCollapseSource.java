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

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryContainer.Evaluatable;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RuleCollapseSource implements OptimizerRule {

    static final String PARTIAL_PROPERTY = AbstractMetadataRecord.RELATIONAL_PREFIX + "partial_filter"; //$NON-NLS-1$

    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {

            // Get nested non-relational plan if there is one
            ProcessorPlan nonRelationalPlan = FrameUtil.getNestedPlan(accessNode);
            Command command = FrameUtil.getNonQueryCommand(accessNode);
            Object modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);

            if(nonRelationalPlan != null) {
                accessNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, nonRelationalPlan);
            } else if (RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata) == null) {
                //with query or processor plan already set
            } else if(command == null) {
                PlanNode commandRoot = accessNode;
                GroupSymbol intoGroup = (GroupSymbol)accessNode.getFirstChild().getProperty(NodeConstants.Info.INTO_GROUP);
                Set<Object> toCheck = (Set<Object>)commandRoot.getProperty(NodeConstants.Info.CHECK_MAT_VIEW);
                if (intoGroup != null) {
                    commandRoot = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.SOURCE).getFirstChild();
                } else {
                    plan = removeUnnecessaryInlineView(plan, commandRoot);
                }
                QueryCommand queryCommand = createQuery(context, capFinder, accessNode, commandRoot);
                if (toCheck != null) {
                    modifyToCheckMatViewStatus(metadata, queryCommand, toCheck);
                }

                if (queryCommand instanceof Query
                        && CapabilitiesUtil.supports(Capability.PARTIAL_FILTERS, modelId, metadata, capFinder)) {
                    //this logic relies on the capability restrictions made in capabilities converter
                    Query query = (Query)queryCommand;
                    if (query.getCriteria() != null) {
                        List<Criteria> toFilter = new ArrayList<Criteria>();

                        HashSet<ElementSymbol> select = new LinkedHashSet(query.getSelect().getProjectedSymbols());

                        outer: for (Criteria crit : Criteria.separateCriteriaByAnd(query.getCriteria())) {
                            for (ElementSymbol es :ElementCollectorVisitor.getElements(crit, true)) {
                                if (Boolean.valueOf(metadata.getExtensionProperty(es.getMetadataID(), PARTIAL_PROPERTY, false))) {
                                     toFilter.add((Criteria) crit.clone());
                                     continue outer;
                                }
                            }
                        }
                        if (!toFilter.isEmpty()) {
                            PlanNode postFilter = RelationalPlanner.createSelectNode(CompoundCriteria.combineCriteria(toFilter), false);
                            ElementCollectorVisitor.getElements(toFilter, select);
                            postFilter.setProperty(Info.OUTPUT_COLS, new ArrayList<Expression>(query.getSelect().getProjectedSymbols()));
                            if (accessNode.getParent() != null) {
                                accessNode.addAsParent(postFilter);
                            } else {
                                plan = postFilter;
                                postFilter.addFirstChild(accessNode);
                            }
                            if (select.size() != query.getSelect().getProjectedSymbols().size()) {
                                //correct projection
                                for (ElementSymbol es : new ArrayList<>(select).subList(query.getSelect().getProjectedSymbols().size(), select.size())) {
                                    if (!RuleRaiseAccess.canPushSymbol(es, true, modelId, metadata, capFinder, analysisRecord)) {
                                        throw new QueryPlannerException(QueryPlugin.Event.TEIID30258, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30258, es, modelId));
                                    }
                                }
                                query.getSelect().setSymbols(select);
                                accessNode.setProperty(Info.OUTPUT_COLS, new ArrayList<Expression>(select));
                            }
                        }
                    }
                }

                plan = addDistinct(metadata, capFinder, accessNode, plan, queryCommand, capFinder);
                command = queryCommand;
                queryCommand.setSourceHint((SourceHint) accessNode.getProperty(Info.SOURCE_HINT));
                queryCommand.getProjectedQuery().setSourceHint((SourceHint) accessNode.getProperty(Info.SOURCE_HINT));
                if (intoGroup != null) {
                    Insert insertCommand = (Insert)commandRoot.getParent().getProperty(NodeConstants.Info.VIRTUAL_COMMAND);
                    if (insertCommand == null) {
                        //TODO: this is probably no longer needed as we rewrite select into
                        insertCommand = new Insert(intoGroup, ResolverUtil.resolveElementsInGroup(intoGroup, metadata), null);
                    }
                    insertCommand.setQueryExpression(queryCommand);
                    command = insertCommand;
                }
            }
            if (command != null) {
                setEvalFlag(metadata, capFinder, command, modelId);
                accessNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
            }
            accessNode.removeAllChildren();
        }

        return plan;
    }

    /**
     * find all pushdown functions in evaluatable locations and mark them to be evaluated by the source
     * @param metadata
     * @param capFinder
     * @param command
     * @param modelId
     */
    private void setEvalFlag(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, Command command,
            Object modelId) {
        LanguageVisitor lv = new LanguageVisitor() {
            @Override
            public void visit(Function f) {
                FunctionDescriptor fd = f.getFunctionDescriptor();
                if (f.isEval()) {
                    try {
                        if (modelId != null && fd.getPushdown() == PushDown.MUST_PUSHDOWN
                                    && fd.getMethod() != null
                                    && CapabilitiesUtil.isSameConnector(modelId, fd.getMethod().getParent(), metadata, capFinder)) {
                            f.setEval(false);
                        } else if (fd.getDeterministic() == Determinism.NONDETERMINISTIC
                                && CapabilitiesUtil.supportsScalarFunction(modelId, f, metadata, capFinder)) {
                            f.setEval(false);
                        }
                    } catch (QueryMetadataException e) {
                        throw new TeiidRuntimeException(e);
                    } catch (TeiidComponentException e) {
                        throw new TeiidRuntimeException(e);
                    }
                }
            }
            @Override
            public void visit(SubqueryFromClause obj) {
                PreOrPostOrderNavigator.doVisit(obj.getCommand(), this, true);
            }
            @Override
            public void visit(WithQueryCommand obj) {
                PreOrPostOrderNavigator.doVisit(obj.getCommand(), this, true);
            }

            @Override
            public void visit(StoredProcedure obj) {
                try {
                    boolean supports = modelId != null
                            && CapabilitiesUtil.supports(Capability.PROCEDURE_PARAMETER_EXPRESSION, modelId, metadata, capFinder);
                    obj.setSupportsExpressionParameters(supports);
                } catch (TeiidComponentException e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        };
        PreOrPostOrderNavigator.doVisit(command, lv, true);
    }

    private void modifyToCheckMatViewStatus(QueryMetadataInterface metadata, QueryCommand queryCommand,
            Set<Object> ids) throws QueryMetadataException, TeiidComponentException {
        for (Object viewMatadataId : ids) {
            String schemaName = metadata.getName(metadata.getModelID(viewMatadataId));
            String viewName = metadata.getName(viewMatadataId);

            Expression expr1 = new Constant(schemaName);
            Expression expr2 = new Constant(viewName);

            Function status = new Function("mvstatus", new Expression[] {expr1, expr2}); //$NON-NLS-1$
            status.setType(DataTypeManager.DefaultDataClasses.INTEGER);
            FunctionDescriptor descriptor =
                    metadata.getFunctionLibrary().findFunction("mvstatus", new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING }); //$NON-NLS-1$
            status.setFunctionDescriptor(descriptor);
            Query query = queryCommand.getProjectedQuery();
            //insert first so that it gets evaluated ahead of any false predicate
            query.setCriteria(Criteria.combineCriteria(new CompareCriteria(status, CompareCriteria.EQ, new Constant(1)), query.getCriteria()));
        }
    }

    /**
     * This functions as "RulePushDistinct", however we do not bother
     * checking to see if a parent dup removal can actually be removed
     * - which can only happen if there are sources/selects/simple projects/limits/order by
     * between the access node and the parent dup removal.
     *
     * @param metadata
     * @param capFinder
     * @param accessNode
     * @param queryCommand
     * @param capabilitiesFinder
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private PlanNode addDistinct(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, PlanNode accessNode, PlanNode root,
            QueryCommand queryCommand, CapabilitiesFinder capabilitiesFinder) throws QueryMetadataException,
            TeiidComponentException {
        if (RuleRemoveOptionalJoins.useNonDistinctRows(accessNode.getParent())) {
            return root;
        }
        if (queryCommand instanceof Query) {
            boolean allConstants = true;
            for (Expression ex : (List<Expression>)accessNode.getProperty(Info.OUTPUT_COLS)) {
                if (!(EvaluatableVisitor.willBecomeConstant(SymbolMap.getExpression(ex)))) {
                    allConstants = false;
                    break;
                }
            }
            if (allConstants) {
                //distinct of all constants means just a single row
                //see also the logic in RuleAssignOutputElements for a dupremove
                Object mid = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
                if (!CapabilitiesUtil.supports(Capability.ROW_LIMIT, mid, metadata, capabilitiesFinder)) {
                    PlanNode limit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
                    limit.setProperty(Info.MAX_TUPLE_LIMIT, new Constant(1));
                    limit.setProperty(NodeConstants.Info.OUTPUT_COLS, accessNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
                    if (accessNode.getParent() != null) {
                        accessNode.addAsParent(limit);
                        return root;
                    }
                    limit.addFirstChild(accessNode);
                    return limit;
                }
                if (queryCommand.getLimit() != null) {
                    if (queryCommand.getLimit().getRowLimit() == null) {
                        queryCommand.getLimit().setRowLimit(new Constant(1));
                    } //else could have limit 0, so it takes more logic (case statement) to set this
                } else {
                    queryCommand.setLimit(new Limit(null, new Constant(1)));
                }
                return root;
            }
        }
        if (queryCommand.getLimit() != null) {
            return root; //TODO: could create an inline view
        }
        boolean canRemoveParentDup = false;
        if (queryCommand.getOrderBy() == null) {
            /*
             * we're assuming that a pushed order by implies that the cost of the distinct operation
             * will be marginal - which is not always true.
             *
             * TODO: we should add costing for the benefit of pushing distinct by itself
             * cardinality without = c
             * assume cost ~ c lg c for c' cardinality and a modification for associated bandwidth savings
             * recompute cost of processing plan with c' and see if new cost + c lg c < original cost
             *
             * We stop at join nodes they can alter the cardinality
             * - further checking could determine if the cardinality is preserved without the parent distinct
             */
            PlanNode dupRemove = NodeEditor.findParent(accessNode, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
            if (dupRemove != null) { //TODO: what about when sort/dup remove have been combined
                PlanNode project = NodeEditor.findParent(accessNode, NodeConstants.Types.PROJECT, NodeConstants.Types.DUP_REMOVE);
                if (project != null) {
                    List<Expression> projectCols = (List<Expression>) project.getProperty(Info.PROJECT_COLS);
                    for (Expression ex : projectCols) {
                        ex = SymbolMap.getExpression(ex);
                        if (!(ex instanceof ElementSymbol) && !(ex instanceof Constant) && !(EvaluatableVisitor.willBecomeConstant(ex, true))) {
                            return root;
                        }
                    }
                    /*
                     * If we can simply move the dupremove below the projection, then we'll do that as well
                     */
                    canRemoveParentDup = true;
                }
            }
            if (accessNode.hasBooleanProperty(Info.IS_MULTI_SOURCE)) {
                if (dupRemove == null) {
                    return root;
                }
                //if multi-source we still need to process above
                canRemoveParentDup = false;
            } else if (!canRemoveParentDup) {
                return root;
            }
        }
        // ensure that all columns are comparable - they might not be if there is an intermediate project
        for (Expression ses : queryCommand.getProjectedSymbols()) {
            if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(ses.getType()))) {
                return root;
            }
        }
        /*
         * TODO: if we are under a grouping/union not-all, then we should also fully order the results
         * and update the processing logic (this requires that we can guarantee null ordering) to assume sorted
         */
        if (queryCommand instanceof SetQuery) {
            ((SetQuery)queryCommand).setAll(false);
        } else if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_DISTINCT, RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder)) {
            Query query = (Query)queryCommand;
            HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
            ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
            if (!QueryRewriter.isDistinctWithGroupBy(query) && !NewCalculateCostUtil.usesKey(query.getSelect().getProjectedSymbols(), keyPreservingGroups, metadata, true)) {
                if (canRemoveParentDup) { //remove the upper dup remove
                    PlanNode dupRemove = NodeEditor.findParent(accessNode, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.SOURCE);
                    if (dupRemove.getParent() == null) {
                        root = dupRemove.getFirstChild();
                        dupRemove.getFirstChild().removeFromParent();
                    } else {
                        dupRemove.getParent().replaceChild(dupRemove, dupRemove.getFirstChild());
                    }
                }
                ((Query)queryCommand).getSelect().setDistinct(true);
            }
        }
        return root;
    }

    private PlanNode removeUnnecessaryInlineView(PlanNode root, PlanNode accessNode) {
        PlanNode child = accessNode.getFirstChild();

        //check for simple projection - may happen with non-deterministic functions
        //TODO: in most cases more logic in ruleremovevirtual (to make sure there is only a single use of the non-deterministic function)
        //will allow us to remove these views
        if (child.getType() == NodeConstants.Types.PROJECT && child.getFirstChild() != null
                && child.getFirstChild().hasBooleanProperty(NodeConstants.Info.INLINE_VIEW)) {
            Set<Expression> requiredElements = new HashSet<Expression>((List<Expression>) child.getFirstChild().getProperty(Info.OUTPUT_COLS));
            List<Expression> selectSymbols = (List<Expression>)child.getProperty(NodeConstants.Info.PROJECT_COLS);

            // check that it only performs simple projection and that all required symbols are projected
            LinkedHashSet<Expression> symbols = new LinkedHashSet<Expression>(); //ensuring there are no duplicates prevents problems with subqueries
            for (Expression symbol : selectSymbols) {
                Expression expr = SymbolMap.getExpression(symbol);
                if (!(expr instanceof ElementSymbol)) {
                    return root;
                }
                requiredElements.remove(expr);
                if (!symbols.add(expr)) {
                    return root;
                }
            }
            if (!requiredElements.isEmpty()) {
                return root;
            }
            root = RuleRaiseAccess.performRaise(root, child, accessNode);
            child = accessNode.getFirstChild();
        }

        if (child.hasBooleanProperty(NodeConstants.Info.INLINE_VIEW)) {
            //check for the projection from the access node
            //partial pushdown of select expressions creates expressions that reference the
            //view output columns
            for (Expression ex : (List<Expression>) accessNode.getProperty(Info.OUTPUT_COLS)) {
                if (!(SymbolMap.getExpression(ex) instanceof ElementSymbol)) {
                    return root;
                }
            }
            if (!accessNode.getProperty(Info.OUTPUT_COLS).equals(child.getProperty(Info.OUTPUT_COLS))) {
                //corner case - the output of a dup removal node will be all the columns
                //this will be reflected in the source node parent as well, but
                //will be minimized in the grand parent access if not all columns are needed
                return root;
            }
            child.removeProperty(NodeConstants.Info.INLINE_VIEW);
            root = RuleRaiseAccess.performRaise(root, child, accessNode);
            //add the groups from the lower project
            accessNode.getGroups().clear();
            PlanNode sourceNode = FrameUtil.findJoinSourceNode(accessNode.getFirstChild());
            if (sourceNode != null) {
                accessNode.addGroups(sourceNode.getGroups());
            }
            //update the output columns for both the source node and access node
            child.setProperty(Info.OUTPUT_COLS, accessNode.getProperty(Info.OUTPUT_COLS));
            accessNode.setProperty(Info.OUTPUT_COLS, accessNode.getFirstChild().getProperty(Info.OUTPUT_COLS));
        }

        return root;
    }

    private QueryCommand createQuery(CommandContext context, CapabilitiesFinder capFinder, PlanNode accessRoot, PlanNode node) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        QueryMetadataInterface metadata = context.getMetadata();
        PlanNode setOpNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
        Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessRoot, metadata);
        if (setOpNode != null) {
            Operation setOp = (Operation)setOpNode.getProperty(NodeConstants.Info.SET_OPERATION);
            SetQuery unionCommand = new SetQuery(setOp);
            boolean unionAll = ((Boolean)setOpNode.getProperty(NodeConstants.Info.USE_ALL)).booleanValue();
            unionCommand.setAll(unionAll);
            int count = 0;
            OrderBy orderBy = null;
            PlanNode sort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SORT, NodeConstants.Types.SET_OP);
            if (sort != null) {
                processOrderBy(sort, unionCommand, context);
                orderBy = unionCommand.getOrderBy();
                unionCommand.setOrderBy(null);
                //we have to remap if the primary projection is from a grouping
                PlanNode groupNode = NodeEditor.findNodePreOrder(setOpNode.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
                if (groupNode != null) {
                    SymbolMap symbolMap = (SymbolMap) groupNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
                    ExpressionMappingVisitor.mapExpressions(orderBy, symbolMap.asMap(), true);
                }
            }
            for (PlanNode child : setOpNode.getChildren()) {
                QueryCommand command = createQuery(context, capFinder, accessRoot, child);
                if (count == 0) {
                    unionCommand.setLeftQuery(command);
                } else if (count == 1) {
                    unionCommand.setRightQuery(command);
                } else {
                    unionCommand = new SetQuery(setOp, unionAll, unionCommand, command);
                }
                count++;
            }
            PlanNode limit = NodeEditor.findNodePreOrder(node, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SET_OP);
            if (limit != null) {
                processLimit(limit, unionCommand, metadata);
            }
            unionCommand.setOrderBy(orderBy);
            return unionCommand;
        }
        Query query = new Query();
        Select select = new Select();
        List<Expression> columns = (List<Expression>)node.getProperty(NodeConstants.Info.OUTPUT_COLS);
        prepareSubqueries(ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(columns));
        select.addSymbols(columns);
        query.setSelect(select);
        query.setFrom(new From());
        buildQuery(accessRoot, node, query, context, capFinder);
        if (!CapabilitiesUtil.useAnsiJoin(modelID, metadata, capFinder)) {
            simplifyFromClause(query);
        }
        if (query.getCriteria() instanceof CompoundCriteria) {
            query.setCriteria(QueryRewriter.optimizeCriteria((CompoundCriteria)query.getCriteria(), metadata));
        }
        if (columns.isEmpty()) {
            if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION, modelID, metadata, capFinder)) {
                select.addSymbol(new ExpressionSymbol("dummy", new Constant(1))); //$NON-NLS-1$
            } else {
                //TODO: need to ensure the type is consistent
                //- should be rare as the source would typically support select expression if it supports union
                select.addSymbol(selectOutputElement(query.getFrom().getGroups(), metadata));
            }
        }
        PlanNode groupNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
        if (groupNode != null) {
            if (query.getOrderBy() != null) {
                query.setOrderBy(query.getOrderBy().clone());
            }
            if (query.getHaving() != null) {
                query.setHaving((Criteria) query.getHaving().clone());
            }
            query.setSelect(query.getSelect().clone());
            SymbolMap symbolMap = (SymbolMap) groupNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

            //map back to expression form
            ExpressionMappingVisitor.mapExpressions(query.getOrderBy(), symbolMap.asMap(), true);
            ExpressionMappingVisitor.mapExpressions(query.getSelect(), symbolMap.asMap(), true);
            ExpressionMappingVisitor.mapExpressions(query.getHaving(), symbolMap.asMap(), true);

            if (query.getHaving() != null && !CapabilitiesUtil.supports(Capability.QUERY_HAVING, modelID, metadata, capFinder)) {
                Select sel = query.getSelect();
                GroupBy groupBy = query.getGroupBy();
                Criteria having = query.getHaving();
                query.setHaving(null);
                OrderBy orderBy = query.getOrderBy();
                query.setOrderBy(null);
                Limit limit = query.getLimit();
                query.setLimit(null);
                Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
                aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(having, true));
                Set<Expression> expr = new HashSet<Expression>();
                for (Expression ex : sel.getProjectedSymbols()) {
                    Expression selectExpression = SymbolMap.getExpression(ex);
                    aggs.remove(selectExpression);
                    expr.add(selectExpression);
                }
                int originalSelect = sel.getSymbols().size();
                sel.addSymbols(aggs);
                if (groupBy != null) {
                    for (Expression ex : groupBy.getSymbols()) {
                        ex = SymbolMap.getExpression(ex);
                        if (expr.add(ex)) {
                            sel.addSymbol(ex);
                        }
                    }
                }
                Query outerQuery = null;
                try {
                    outerQuery = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), query, metadata, query.getSelect().getProjectedSymbols()); //$NON-NLS-1$
                } catch (TeiidException err) {
                     throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30257, err);
                }
                Iterator<Expression> iter = outerQuery.getSelect().getProjectedSymbols().iterator();
                HashMap<Expression, Expression> expressionMap = new HashMap<Expression, Expression>();
                for (Expression symbol : query.getSelect().getProjectedSymbols()) {
                    //need to unwrap on both sides as the select expression could be aliased
                    //TODO: could add an option to createInlineViewQuery to disable alias creation
                    expressionMap.put(SymbolMap.getExpression(symbol), SymbolMap.getExpression(iter.next()));
                }
                ExpressionMappingVisitor.mapExpressions(having, expressionMap, true);
                outerQuery.setCriteria(having);
                ExpressionMappingVisitor.mapExpressions(orderBy, expressionMap, true);
                outerQuery.setOrderBy(orderBy);
                outerQuery.setLimit(limit);
                ExpressionMappingVisitor.mapExpressions(select, expressionMap, true);
                outerQuery.getSelect().setSymbols(outerQuery.getSelect().getProjectedSymbols().subList(0, originalSelect));
                outerQuery.setOption(query.getOption());
                query = outerQuery;

            }

            if (query.getGroupBy() != null) {
                // we check for group by expressions here to create an ANSI SQL plan
                boolean hasExpression = false;
                boolean hasLiteral = false;
                for (final Iterator<Expression> iterator = query.getGroupBy().getSymbols().iterator(); iterator.hasNext();) {
                    Expression ex = iterator.next();
                    hasExpression |= !(ex instanceof ElementSymbol);
                    hasLiteral |= EvaluatableVisitor.willBecomeConstant(ex, true);
                }
                if ((hasExpression && !CapabilitiesUtil.supports(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, modelID, metadata, capFinder)) || hasLiteral) {
                    //if group by expressions are not support, add an inline view to compensate
                    query = RuleCollapseSource.rewriteGroupByAsView(query, metadata, false);
                    if (query.getHaving() != null) {
                        //dependent sets will have been added a having
                        List<Criteria> crits = Criteria.separateCriteriaByAnd(query.getHaving());
                        for (Iterator<Criteria> iter = crits.iterator(); iter.hasNext();) {
                            Criteria crit = iter.next();
                            if (crit instanceof DependentSetCriteria) {
                                query.setCriteria(Criteria.combineCriteria(query.getCriteria(), crit));
                                iter.remove();
                            }
                        }
                        query.setHaving(Criteria.combineCriteria(crits));
                    }
                }
                if (query.getOrderBy() != null
                        && groupNode.hasBooleanProperty(Info.ROLLUP)
                        && !CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_EXTENDED_GROUPING, modelID, metadata, capFinder)) {
                    //if ordering is not directly supported over extended grouping, add an inline view to compensate
                    query = RuleCollapseSource.rewriteGroupByAsView(query, metadata, true);
                }
            }
        }
        return query;
    }

    /**
     * Find a selectable element in the specified groups.  This is a helper for fixing
     * the "no elements" case.
     *
     * @param groups Bunch of groups
     * @param metadata Metadata implementation
     */
    static ElementSymbol selectOutputElement(Collection<GroupSymbol> groups, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        // Find a group with selectable elements and pick the first one
        for (GroupSymbol group : groups) {
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);

            for (ElementSymbol element : elements) {
                if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.SELECT)) {
                    element = element.clone();
                    element.setGroupSymbol(group);
                    return element;
                }
            }
        }

        return null;
    }

    void buildQuery(PlanNode accessRoot, PlanNode node, Query query, CommandContext context, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        QueryMetadataInterface metadata = context.getMetadata();
        //visit source and join nodes as they appear
        Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessRoot, metadata);
        switch(node.getType()) {
            case NodeConstants.Types.JOIN:
            {
                prepareSubqueries(node.getSubqueryContainers());
                JoinType joinType = (JoinType) node.getProperty(NodeConstants.Info.JOIN_TYPE);
                List<Criteria> crits = (List<Criteria>) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);

                if (crits == null || crits.isEmpty()) {
                    crits = new ArrayList<Criteria>();
                } else {
                    RuleChooseJoinStrategy.filterOptionalCriteria(crits, false);
                    if (crits.isEmpty() && joinType == JoinType.JOIN_INNER) {
                        joinType = JoinType.JOIN_CROSS;
                    }
                }

                PlanNode left = node.getFirstChild();
                PlanNode right = node.getLastChild();

                /* special handling is needed to determine criteria placement.
                 *
                 * if the join is a left outer join, criteria from the right side will be added to the on clause
                 */
                Criteria savedCriteria = null;
                buildQuery(accessRoot, left, query, context, capFinder);
                if (joinType == JoinType.JOIN_LEFT_OUTER) {
                    savedCriteria = query.getCriteria();
                    query.setCriteria(null);
                }
                buildQuery(accessRoot, right, query, context, capFinder);
                if (joinType == JoinType.JOIN_LEFT_OUTER) {
                    moveWhereClauseIntoOnClause(query, crits);
                    query.setCriteria(savedCriteria);
                }

                if (joinType == JoinType.JOIN_LEFT_OUTER || joinType == JoinType.JOIN_FULL_OUTER) {
                    boolean subqueryOn = CapabilitiesUtil.supports(Capability.CRITERIA_ON_SUBQUERY, modelID, metadata, capFinder);
                    if (!subqueryOn) {
                        for (SubqueryContainer<?> subqueryContainer : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(crits)) {
                            if (subqueryContainer instanceof Evaluatable && subqueryContainer.getCommand().getCorrelatedReferences() == null) {
                                ((Evaluatable)subqueryContainer).setShouldEvaluate(true);
                            } else {
                                throw new AssertionError("On clause not expected to contain non-evaluatable subqueries"); //$NON-NLS-1$
                            }
                        }
                    }
                }

                // Get last two clauses added to the FROM and combine them into a JoinPredicate
                From from = query.getFrom();
                List<FromClause> clauses = from.getClauses();
                int lastClause = clauses.size()-1;
                FromClause clause1 = clauses.get(lastClause-1);
                FromClause clause2 = clauses.get(lastClause);

                //compensate if we only support outer and use a left outer join instead
                //TODO: moved the handling for the primary driver, salesforce, back into the translator
                //so this may not be needed moving forward
                if (!joinType.isOuter() && !CapabilitiesUtil.supports(Capability.QUERY_FROM_JOIN_INNER, modelID, metadata, capFinder)) {
                    joinType = JoinType.JOIN_LEFT_OUTER;
                    if (!crits.isEmpty()) {
                        if (!useLeftOuterJoin(query, metadata, crits, right.getGroups())) {
                            if (!useLeftOuterJoin(query, metadata, crits, left.getGroups())) {
                                throw new AssertionError("Could not convert inner to outer join."); //$NON-NLS-1$
                            }
                            FromClause temp = clause1;
                            clause1 = clause2;
                            clause2 = temp;
                        }
                    }
                }

                //correct the criteria or the join type if necessary
                if (joinType != JoinType.JOIN_CROSS && crits.isEmpty()) {
                    crits.add(QueryRewriter.TRUE_CRITERIA);
                } else if (joinType == JoinType.JOIN_CROSS && !crits.isEmpty()) {
                    joinType = JoinType.JOIN_INNER;
                }

                JoinPredicate jp = new JoinPredicate(clause1, clause2, joinType, crits);

                // Replace last two clauses with new predicate
                clauses.remove(lastClause);
                clauses.set(lastClause-1, jp);
                return;
            }
            case NodeConstants.Types.SOURCE:
            {
                boolean pushedTableProcedure = false;
                GroupSymbol symbol = node.getGroups().iterator().next();
                if (node.hasBooleanProperty(Info.INLINE_VIEW)) {
                    PlanNode child = node.getFirstChild();
                    QueryCommand newQuery = createQuery(context, capFinder, accessRoot, child);

                    //ensure that the group is consistent
                    SubqueryFromClause sfc = new SubqueryFromClause(symbol, newQuery);

                    SymbolMap map = (SymbolMap)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
                    if (map != null) {
                        ExpressionMappingVisitor visitor = new RulePlanSubqueries.ReferenceReplacementVisitor(map);
                        DeepPostOrderNavigator.doVisit(newQuery, visitor);
                        sfc.setLateral(true);
                    }

                    query.getFrom().addClause(sfc);
                    //ensure that the column names are consistent
                    Query q = newQuery.getProjectedQuery();
                    List<Expression> expressions = q.getSelect().getSymbols();
                    List<Expression> outputCols = (List<Expression>) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
                    Map<Expression, String> corrected = null;
                    for (int i = 0; i < outputCols.size(); i++) {
                        Expression ex = expressions.get(i);
                        Expression expected = outputCols.get(i);
                        String name = Symbol.getShortName(expected);
                        if (!name.equals(Symbol.getShortName(ex))) {
                            expressions.set(i, new AliasSymbol(name, SymbolMap.getExpression(ex)));
                            corrected = new HashMap<Expression, String>();
                            corrected.put(ex, name);
                        }
                    }
                    if (corrected != null && newQuery.getOrderBy() != null) {
                        for (OrderByItem item : newQuery.getOrderBy().getOrderByItems()) {
                            String name = corrected.get(item.getSymbol());
                            if (name != null) {
                                item.setSymbol(new AliasSymbol(name, SymbolMap.getExpression(item.getSymbol())));
                            }
                        }
                    }

                    //there is effectively an unnecessary inline view that can be removed in the procedure case
                    //so we'll unwrap that here
                    if (newQuery instanceof Query) {
                        q = (Query)newQuery;
                        if (q.getFrom() != null && q.getFrom().getClauses().size() == 1 && q.getFrom().getClauses().get(0) instanceof SubqueryFromClause) {
                            SubqueryFromClause nested = (SubqueryFromClause)q.getFrom().getClauses().get(0);
                            if (nested.getCommand() instanceof StoredProcedure) {
                                sfc.setCommand(nested.getCommand());
                            }
                        }
                    }

                    return;
                }
                //handle lateral join of a procedure
                Command command = (Command) node.getProperty(NodeConstants.Info.VIRTUAL_COMMAND);
                if (command instanceof StoredProcedure) {
                    StoredProcedure storedProcedure = (StoredProcedure)command;
                    storedProcedure.setPushedInQuery(true);
                    SubqueryFromClause subqueryFromClause = new SubqueryFromClause(symbol, storedProcedure);

                    //TODO: it would be better to directly add
                    query.getFrom().addClause(subqueryFromClause);
                    pushedTableProcedure=true;
                }

                PlanNode subPlan = (PlanNode) node.getProperty(Info.SUB_PLAN);
                if (subPlan != null) {
                    Map<GroupSymbol, PlanNode> subPlans = (Map<GroupSymbol, PlanNode>) accessRoot.getProperty(Info.SUB_PLANS);
                    if (subPlans == null) {
                        subPlans = new HashMap<GroupSymbol, PlanNode>();
                        accessRoot.setProperty(Info.SUB_PLANS, subPlans);
                    }
                    subPlans.put(symbol, subPlan);
                }
                if (!pushedTableProcedure) {
                    query.getFrom().addGroup(symbol);
                }
                break;
            }
        }

        for (PlanNode childNode : node.getChildren()) {
            buildQuery(accessRoot, childNode, query, context, capFinder);
        }

        switch(node.getType()) {
            case NodeConstants.Types.SELECT:
            {
                Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                prepareSubqueries(node.getSubqueryContainers());
                if(!node.hasBooleanProperty(NodeConstants.Info.IS_HAVING)) {
                    query.setCriteria( CompoundCriteria.combineCriteria(query.getCriteria(), crit) );
                } else {
                    query.setHaving( CompoundCriteria.combineCriteria(query.getHaving(), crit) );
                }
                break;
            }
            case NodeConstants.Types.SORT:
            {
                prepareSubqueries(node.getSubqueryContainers());
                processOrderBy(node, query, context);
                break;
            }
            case NodeConstants.Types.DUP_REMOVE:
            {
                boolean distinct = true;
                PlanNode grouping = NodeEditor.findNodePreOrder(node.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
                if (grouping != null) {
                   List groups = (List) grouping.getProperty(NodeConstants.Info.GROUP_COLS);
                   if(groups == null || groups.isEmpty()) {
                       distinct = false;
                   }
                }
                query.getSelect().setDistinct(distinct);
                break;
            }
            case NodeConstants.Types.GROUP:
            {
                List groups = (List) node.getProperty(NodeConstants.Info.GROUP_COLS);
                if(groups != null && !groups.isEmpty()) {
                    query.setGroupBy(new GroupBy(groups));
                    if (node.hasBooleanProperty(Info.ROLLUP)) {
                        query.getGroupBy().setRollup(true);
                    }
                }
                break;
            }
            case NodeConstants.Types.TUPLE_LIMIT:
            {
                processLimit(node, query, metadata);
                break;
            }
        }
    }

    private boolean useLeftOuterJoin(Query query, QueryMetadataInterface metadata,
            List<Criteria> crits, Set<GroupSymbol> innerGroups) {
        Criteria c = query.getCriteria();
        if (c != null) {
            List<Criteria> parts = Criteria.separateCriteriaByAnd(c);
            for (Criteria criteria : parts) {
                if (!JoinUtil.isNullDependent(metadata, innerGroups, criteria)) {
                    return true;
                }
            }
        }
        ElementSymbol es = null;
        for (Criteria criteria : crits) {
            if (!(criteria instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria cc = (CompareCriteria)criteria;
            if ((cc.getLeftExpression() instanceof ElementSymbol) && innerGroups.contains(((ElementSymbol)cc.getLeftExpression()).getGroupSymbol())) {
                es = (ElementSymbol) cc.getLeftExpression();
                break;
            }
            if ((cc.getRightExpression() instanceof ElementSymbol) && innerGroups.contains(((ElementSymbol)cc.getRightExpression()).getGroupSymbol())) {
                es = (ElementSymbol) cc.getRightExpression();
                break;
            }
        }
        if (es == null) {
            return false;
        }
        IsNullCriteria inc = new IsNullCriteria(es);
        inc.setNegated(true);
        query.setCriteria( CompoundCriteria.combineCriteria(c, inc) );
        return true;
    }

    private void prepareSubqueries(List<SubqueryContainer<?>> containers) {
        for (SubqueryContainer<?> container : containers) {
            prepareSubquery(container);
        }
    }

    public static void prepareSubquery(SubqueryContainer container) {
        RelationalPlan subqueryPlan = (RelationalPlan)container.getCommand().getProcessorPlan();
        AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(subqueryPlan);
        QueryCommand command = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
        if (command == null) {
            return;
        }
        final SymbolMap map = container.getCommand().getCorrelatedReferences();
        if (map != null) {
            ExpressionMappingVisitor visitor = new RulePlanSubqueries.ReferenceReplacementVisitor(map);
            DeepPostOrderNavigator.doVisit(command, visitor);
        }
        command.setProcessorPlan(container.getCommand().getProcessorPlan());
        boolean removeLimit = false;
        if (container instanceof ExistsCriteria) {
            removeLimit = !((ExistsCriteria)container).shouldEvaluate();
        } else if (container instanceof ScalarSubquery) {
            removeLimit = !((ScalarSubquery)container).shouldEvaluate();
        }
        if (removeLimit && command.getLimit() != null && command.getLimit().isImplicit()) {
            command.setLimit(null);
        }
        container.setCommand(command);
    }

    private void processLimit(PlanNode node,
                              QueryCommand query, QueryMetadataInterface metadata) {

        Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
        Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);

        PlanNode limitNode = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        Expression childLimit = null;
        Expression childOffset = null;
        if (query.getLimit() != null) {
            childLimit = query.getLimit().getRowLimit();
            childOffset = query.getLimit().getOffset();
        }
        RulePushLimit.combineLimits(limitNode, metadata, limit, offset, childLimit, childOffset);
        Limit lim = new Limit((Expression)limitNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT), (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT));
        lim.setImplicit(node.hasBooleanProperty(Info.IS_IMPLICIT_LIMIT) && (query.getLimit() == null || query.getLimit().isImplicit()));
        query.setLimit(lim);
    }

    /**
     * Will combine the where criteria with the on criteria.
     *
     * A full rewrite call is not necessary here, but it will attempt to flatten the criteria.
     *
     * @param query
     * @param joinCrits
     */
    private void moveWhereClauseIntoOnClause(Query query,
                                List joinCrits) {
        if (query.getCriteria() == null) {
            return;
        }
        LinkedHashSet combinedCrits = new LinkedHashSet();
        combinedCrits.addAll(joinCrits);
        combinedCrits.addAll(Criteria.separateCriteriaByAnd(query.getCriteria()));
        joinCrits.clear();
        joinCrits.addAll(combinedCrits);
        query.setCriteria(null);
    }

    private void processOrderBy(PlanNode node, QueryCommand query, CommandContext context) throws QueryMetadataException, TeiidComponentException {
        OrderBy orderBy = (OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER);
        query.setOrderBy(orderBy);
        if (query instanceof Query) {
            List<Expression> cols = query.getProjectedSymbols();
            List<Expression> exprs = new ArrayList<Expression>(cols.size());
            for (Expression expr : cols) {
                exprs.add(SymbolMap.getExpression(expr));
            }
            for (OrderByItem item : orderBy.getOrderByItems()) {
                item.setExpressionPosition(exprs.indexOf(SymbolMap.getExpression(item.getSymbol())));
            }
            try {
                QueryRewriter.rewriteOrderBy(query, orderBy, query.getProjectedSymbols(), context, context.getMetadata());
            } catch (TeiidProcessingException e) {
                throw new TeiidComponentException(e);
            }
        }
    }

   /**
    * Take the query, built straight from the subtree, and rebuild as a simple query
    * if possible.
    * @param query Query built from collapsing the source nodes
    */
    private void simplifyFromClause(Query query) {
        From from = query.getFrom();
        List<FromClause> clauses = from.getClauses();
        FromClause rootClause = clauses.get(0);

        // If all joins are inner joins, move criteria to WHERE and make
        // FROM a list of groups instead of a tree of JoinPredicates
        if(! hasOuterJoins(rootClause)) {
            from.setClauses(new ArrayList<FromClause>());
            shredJoinTree(rootClause, query);
        } // else leave as is
    }

    /**
     * Convert a join tree into a linear join list
     * @param clause
     * @param query
     */
    private void shredJoinTree(FromClause clause, Query query) {
        if(clause instanceof UnaryFromClause || clause instanceof SubqueryFromClause) {
            query.getFrom().addClause(clause);
        } else {
            JoinPredicate jp = (JoinPredicate) clause;

            List<Criteria> crits = jp.getJoinCriteria();
            if(crits != null && crits.size() > 0) {
                Criteria joinCrit = null;
                if (crits.size() > 1) {
                    joinCrit = new CompoundCriteria(crits);
                } else {
                    joinCrit = crits.get(0);
                }
                query.setCriteria(CompoundCriteria.combineCriteria(joinCrit, query.getCriteria()));
            }

            // Recurse through tree
            shredJoinTree(jp.getLeftClause(), query);
            shredJoinTree(jp.getRightClause(), query);
        }
    }

    /**
     * @param clause Clause to check recursively
     * @return True if tree has outer joins, false otherwise
     */
    static boolean hasOuterJoins(FromClause clause) {
        if (clause instanceof SubqueryFromClause) {
            if (((SubqueryFromClause)clause).isLateral()) {
                return true;
            }
            return false;
        }
        if(clause instanceof UnaryFromClause) {
            return false;
        }
        JoinPredicate jp = (JoinPredicate) clause;
        if(jp.getJoinType().isOuter()) {
            return true;
        }
        // Walk children
        boolean childHasOuter = hasOuterJoins(jp.getLeftClause());
        if(childHasOuter) {
            return true;
        }
        return hasOuterJoins(jp.getRightClause());
    }

    public String toString() {
           return "CollapseSource"; //$NON-NLS-1$
       }

    public static Query rewriteGroupByAsView(Query query, QueryMetadataInterface metadata, boolean addViewForOrderBy) {
        if (query.getGroupBy() == null) {
            return query;
        }
        Select select = query.getSelect();
        GroupBy groupBy = query.getGroupBy();
        if (!addViewForOrderBy) {
            query.setGroupBy(null);
        }
        Criteria having = query.getHaving();
        query.setHaving(null);
        OrderBy orderBy = query.getOrderBy();
        query.setOrderBy(null);
        Limit limit = query.getLimit();
        query.setLimit(null);
        Set<Expression> newSelectColumns = new LinkedHashSet<Expression>();
        for (final Iterator<Expression> iterator = groupBy.getSymbols().iterator(); iterator.hasNext();) {
            newSelectColumns.add(iterator.next());
        }
        Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
        aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(select, true));
        if (having != null) {
            aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(having, true));
        }
        for (AggregateSymbol aggregateSymbol : aggs) {
            for (Expression expr : aggregateSymbol.getArgs()) {
                newSelectColumns.add(SymbolMap.getExpression(expr));
            }
        }
        Select innerSelect = new Select();
        for (Expression expr : newSelectColumns) {
            innerSelect.addSymbol(expr);
        }
        query.setSelect(innerSelect);
        Query outerQuery = null;
        try {
            outerQuery = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), query, metadata, query.getSelect().getProjectedSymbols()); //$NON-NLS-1$
        } catch (TeiidException err) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30257, err);
        }
        Iterator<Expression> iter = outerQuery.getSelect().getProjectedSymbols().iterator();
        HashMap<Expression, Expression> expressionMap = new HashMap<Expression, Expression>();
        for (Expression symbol : query.getSelect().getProjectedSymbols()) {
            //need to unwrap on both sides as the select expression could be aliased
            //TODO: could add an option to createInlineViewQuery to disable alias creation
            expressionMap.put(SymbolMap.getExpression(symbol), SymbolMap.getExpression(iter.next()));
        }
        if (!addViewForOrderBy) {
            ExpressionMappingVisitor.mapExpressions(groupBy, expressionMap);
            outerQuery.setGroupBy(groupBy);
        }
        ExpressionMappingVisitor.mapExpressions(having, expressionMap, true);
        outerQuery.setHaving(having);
        ExpressionMappingVisitor.mapExpressions(orderBy, expressionMap, true);
        outerQuery.setOrderBy(orderBy);
        outerQuery.setLimit(limit);
        ExpressionMappingVisitor.mapExpressions(select, expressionMap, true);
        outerQuery.setSelect(select);
        outerQuery.setOption(query.getOption());
        query = outerQuery;
        return query;
    }

}
