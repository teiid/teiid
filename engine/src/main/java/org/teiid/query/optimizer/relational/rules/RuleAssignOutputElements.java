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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.PlanToProcessConverter;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p>This rule is responsible for assigning the output elements to every node in the
 * plan.  The output elements define the columns that are returned from every node.
 * This is generally done by figuring out top-down all the elements required to
 * execute the operation at each node and making sure those elements are selected
 * from the children nodes.
 */
public final class RuleAssignOutputElements implements OptimizerRule {

    private boolean finalRun;
    private boolean checkSymbols;

    public RuleAssignOutputElements(boolean finalRun) {
        this.finalRun = finalRun;
    }

    /**
     * Execute the rule.  This rule is executed exactly once during every planning
     * call.  The plan is modified in place - only properties are manipulated, structure
     * is unchanged.
     * @param plan The plan to execute rule on
     * @param metadata The metadata interface
     * @param rules The rule stack, not modified
     * @return The updated plan
     */
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // Record project node output columns in top node
        PlanNode projectNode = NodeEditor.findNodePreOrder(plan, NodeConstants.Types.PROJECT);

        if(projectNode == null) {
            return plan;
        }

        List<Expression> projectCols = (List<Expression>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

        assignOutputElements(plan, projectCols, metadata, capFinder, rules, analysisRecord, context);

        return plan;
    }

    /**
     * <p>Assign the output elements at a particular node and recurse the tree.  The
     * outputElements needed from above the node have been collected in
     * outputElements.
     *
     * <p>SOURCE nodes:  If we find a SOURCE node, this must define the top
     * of a virtual group.  Physical groups can be identified by ACCESS nodes
     * at this point in the planning stage.  So, we filter the virtual elements
     * in the virtual source based on the required output elements.
     *
     * <p>SET_OP nodes:  If we hit a SET_OP node, this must be a union.  Unions
     * require a lot of special care.  Unions have many branches and the projected
     * elements in each branch are "equivalent" in terms of nodes above the union.
     * This means that any filtering must occur in an identical way in all branches
     * of a union.
     *
     * @param root Node to assign
     * @param outputElements Output elements needed for this node
     * @param metadata Metadata implementation
     */
    private void assignOutputElements(PlanNode root, List<Expression> outputElements, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        int nodeType = root.getType();

        // Update this node's output columns based on parent's columns
        List<Expression> oldOutput = (List<Expression>) root.setProperty(NodeConstants.Info.OUTPUT_COLS, outputElements);

        if (root.getChildCount() == 0) {
            //update temp access
            if (root.getType() == NodeConstants.Types.SOURCE && root.getGroups().size() == 1) {
                GroupSymbol gs = root.getGroups().iterator().next();
                if (gs.getMetadataID() instanceof TempMetadataID) {
                    for (Expression ex : outputElements) {
                        if (ex instanceof ElementSymbol) {
                            Object id = ((ElementSymbol)ex).getMetadataID();
                            if (id instanceof TempMetadataID) {
                                context.addAccessed((TempMetadataID)id);
                            }
                        }
                    }
                }
            }
            return;
        }

        switch (nodeType) {
            case NodeConstants.Types.ACCESS:
                Command command = FrameUtil.getNonQueryCommand(root);
                if (command instanceof StoredProcedure) {
                    //if the access node represents a stored procedure, then we can't actually change the output symbols
                    root.setProperty(NodeConstants.Info.OUTPUT_COLS, command.getProjectedSymbols());
                } else {
                    ProcessorPlan plan = FrameUtil.getNestedPlan(root);
                    if (plan != null && (command == null || !RelationalNodeUtil.isUpdate(command))) {
                        //nested with clauses are handled as sub plans, which have a fixed set of output symbols
                        root.setProperty(NodeConstants.Info.OUTPUT_COLS, ResolverUtil.resolveElementsInGroup(root.getGroups().iterator().next(), metadata));
                    }
                    if (checkSymbols) {
                        Object modelId = RuleRaiseAccess.getModelIDFromAccess(root, metadata);
                        for (Expression symbol : outputElements) {
                            if(!RuleRaiseAccess.canPushSymbol(symbol, true, modelId, metadata, capFinder, analysisRecord)) {
                                 //if the capabilities are invalid, we may be effectively masking a problem
                                 PlanToProcessConverter.checkForValidCapabilities(modelId, metadata, capFinder);
                                 throw new QueryPlannerException(QueryPlugin.Event.TEIID30258, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30258, symbol, modelId));
                            }
                        }
                    }
                    if (NodeEditor.findParent(root, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE) != null) {
                        //there's a chance that partial projection was used.  we are not a defacto project node
                        //take credit for creating anything that is not an element symbol
                        LinkedHashSet<Expression> filteredElements = new LinkedHashSet<Expression>();
                        for (Expression element : outputElements) {
                            if(element instanceof ElementSymbol) {
                                filteredElements.add(element);
                            } else {
                                filteredElements.addAll(ElementCollectorVisitor.getElements(element, false));
                            }
                        }
                        outputElements = new ArrayList<Expression>(filteredElements);
                    }
                }
                assignOutputElements(root.getLastChild(), outputElements, metadata, capFinder, rules, analysisRecord, context);
                break;
            case NodeConstants.Types.DUP_REMOVE:
                //targeted optimization based upon swapping the dup remove for a limit 1
                //TODO: may need to also check for grouping over constants
                boolean allConstants = true;
                for (Expression ex : outputElements) {
                    if (!(EvaluatableVisitor.willBecomeConstant(SymbolMap.getExpression(ex)))) {
                        allConstants = false;
                        break;
                    }
                }
                if (allConstants && addLimit(rules, root, metadata, capFinder)) {
                    //TODO we could more gracefully handle the !addLimit case
                    PlanNode parent = root.getParent();
                    if (parent != null) {
                        NodeEditor.removeChildNode(root.getParent(), root);
                        execute(parent, metadata, capFinder, rules, analysisRecord, context);
                        return;
                    }
                }
            case NodeConstants.Types.SORT:
                //correct expression positions and update the unrelated flag
                OrderBy order = (OrderBy) root.getProperty(NodeConstants.Info.SORT_ORDER);
                //must rerun with unrelated, otherwise we'll drop that required input
                if (order != null && (oldOutput == null || !oldOutput.equals(outputElements) || root.hasBooleanProperty(Info.UNRELATED_SORT))) {
                    outputElements = new ArrayList<Expression>(outputElements);
                    boolean hasUnrelated = false;
                    for (OrderByItem item : order.getOrderByItems()) {
                        int index = outputElements.indexOf(item.getSymbol());
                        if (index != -1) {
                            item.setExpressionPosition(index);
                        } else {
                            hasUnrelated = true;
                            outputElements.add(item.getSymbol());
                        }
                    }
                    if (!hasUnrelated) {
                        root.setProperty(NodeConstants.Info.UNRELATED_SORT, false);
                    } else {
                        root.setProperty(NodeConstants.Info.UNRELATED_SORT, true);
                    }
                }
                assignOutputElements(root.getLastChild(), outputElements, metadata, capFinder, rules, analysisRecord, context);
                break;
            case NodeConstants.Types.TUPLE_LIMIT:
                assignOutputElements(root.getLastChild(), outputElements, metadata, capFinder, rules, analysisRecord, context);
                break;
            case NodeConstants.Types.SOURCE: {
                outputElements = (List<Expression>)determineSourceOutput(root, outputElements, metadata, capFinder);
                if (!finalRun && root.getProperty(Info.PARTITION_INFO) != null
                        && NodeEditor.findParent(root, NodeConstants.Types.JOIN) != null) {
                    GroupSymbol group = root.getGroups().iterator().next();
                    Object modelId = RuleDecomposeJoin.getEffectiveModelId(metadata, group);
                    String name = metadata.getExtensionProperty(modelId, RuleDecomposeJoin.IMPLICIT_PARTITION_COLUMN_NAME, true);
                    if (name != null) {
                        //keep projecting the implicit partitioning column through the source so that it can
                        //be used in the decomposition logic
                        ElementSymbol es = new ElementSymbol(name, group);
                        if (!outputElements.contains(es)) {
                            es.setMetadataID(metadata.getElementID(es.getName()));
                            outputElements.add(es);
                        }
                    }
                }
                root.setProperty(NodeConstants.Info.OUTPUT_COLS, outputElements);
                List<Expression> childElements = filterVirtualElements(root, outputElements, metadata);
                assignOutputElements(root.getFirstChild(), childElements, metadata, capFinder, rules, analysisRecord, context);
                break;
            }
            case NodeConstants.Types.SET_OP: {
                for (PlanNode childNode : root.getChildren()) {
                    PlanNode projectNode = NodeEditor.findNodePreOrder(childNode, NodeConstants.Types.PROJECT);
                    List<Expression> projectCols = (List<Expression>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);
                    assignOutputElements(childNode, projectCols, metadata, capFinder, rules, analysisRecord, context);
                }
                break;
            }
            default: {
                PlanNode sortNode = null;
                if (root.getType() == NodeConstants.Types.PROJECT) {
                    GroupSymbol intoGroup = (GroupSymbol)root.getProperty(NodeConstants.Info.INTO_GROUP);
                    if (intoGroup != null) { //if this is a project into, treat the nodes under the source as a new plan root
                        PlanNode intoRoot = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SOURCE);
                        execute(intoRoot.getFirstChild(), metadata, capFinder, rules, analysisRecord, context);
                        return;
                    }
                    List<Expression> projectCols = outputElements;
                    sortNode = NodeEditor.findParent(root, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
                    if (finalRun && sortNode != null && sortNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
                        root.getGroups().clear();
                        root.addGroups(GroupsUsedByElementsVisitor.getGroups(projectCols));
                        root.addGroups(GroupsUsedByElementsVisitor.getGroups(root.getCorrelatedReferenceElements()));
                    }
                    root.setProperty(NodeConstants.Info.PROJECT_COLS, projectCols);
                    if (root.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
                        Set<WindowFunction> windowFunctions = getWindowFunctions(projectCols);
                        if (windowFunctions.isEmpty()) {
                            root.setProperty(Info.HAS_WINDOW_FUNCTIONS, false);
                        }
                    }
                }

                List<Expression> requiredInput = collectRequiredInputSymbols(root, metadata, capFinder);
                //targeted optimization for unnecessary aggregation
                if (root.getType() == NodeConstants.Types.GROUP && root.hasBooleanProperty(Info.IS_OPTIONAL) && NodeEditor.findParent(root, NodeConstants.Types.ACCESS) == null) {
                    PlanNode parent = removeGroupBy(root, metadata);
                    if (!root.hasCollectionProperty(Info.GROUP_COLS)) {
                        //just lob off everything under the projection
                        PlanNode project = NodeEditor.findNodePreOrder(parent, NodeConstants.Types.PROJECT);
                        project.removeAllChildren();
                    } else if (!addLimit(rules, parent, metadata, capFinder)) {
                        throw new AssertionError("expected limit node to be added"); //$NON-NLS-1$
                    }
                    execute(parent, metadata, capFinder, rules, analysisRecord, context);
                    return;
                }

                // Call children recursively
                if(root.getChildCount() == 1) {
                    assignOutputElements(root.getLastChild(), requiredInput, metadata, capFinder, rules, analysisRecord, context);
                    if (!finalRun && root.getType() == NodeConstants.Types.PROJECT && sortNode != null && sortNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
                        //if this is the initial rule run, remove unrelated order to preserve the original projection
                        OrderBy elements = (OrderBy) sortNode.getProperty(NodeConstants.Info.SORT_ORDER);
                        outputElements = new ArrayList<Expression>(outputElements);
                        for (OrderByItem item : elements.getOrderByItems()) {
                            if (item.getExpressionPosition() == -1) {
                                outputElements.remove(item.getSymbol());
                            }
                        }
                        root.setProperty(NodeConstants.Info.PROJECT_COLS, outputElements);
                    }
                } else {
                    //determine which elements go to each side of the join
                    for (PlanNode childNode : root.getChildren()) {
                        Set<GroupSymbol> filterGroups = FrameUtil.findJoinSourceNode(childNode).getGroups();
                        List<Expression> filteredElements = filterElements(requiredInput, filterGroups);

                        // Call child recursively
                        assignOutputElements(childNode, filteredElements, metadata, capFinder, rules, analysisRecord, context);
                    }
                }
            }
        }
    }

    private boolean addLimit(RuleStack rules, PlanNode parent, QueryMetadataInterface metadata, CapabilitiesFinder capabilitiesFinder) throws QueryMetadataException, TeiidComponentException {
        PlanNode accessNode = NodeEditor.findParent(parent.getFirstChild(), NodeConstants.Types.ACCESS);

        if (accessNode != null) {
            Object mid = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
            if (!CapabilitiesUtil.supports(Capability.ROW_LIMIT, mid, metadata, capabilitiesFinder)) {
                if (NodeEditor.findParent(parent, NodeConstants.Types.SET_OP | NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS) != null) {
                    return false; //access node is too high
                }
                parent = accessNode;
            }
        }

        PlanNode limit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        limit.setProperty(Info.MAX_TUPLE_LIMIT, new Constant(1));

        if (!rules.contains(RuleConstants.PUSH_LIMIT)) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }

        if (parent.getParent() == null) {
            if (parent.getType() == NodeConstants.Types.ACCESS) {
                return false;
            }
            parent = parent.getFirstChild();
        }
        parent.addAsParent(limit);
        return true;
    }

    static PlanNode removeGroupBy(PlanNode root,
            QueryMetadataInterface metadata)
            throws QueryPlannerException {
        PlanNode next = root.getFirstChild();
        NodeEditor.removeChildNode(root.getParent(), root);

        SymbolMap symbolMap = (SymbolMap) root.getProperty(NodeConstants.Info.SYMBOL_MAP);
        if (!symbolMap.asMap().isEmpty()) {
            FrameUtil.convertFrame(next.getParent(), symbolMap.asMap().keySet().iterator().next().getGroupSymbol(), null, symbolMap.asMap(), metadata);
        }
        PlanNode parent = next.getParent();
        while (parent.getParent() != null && parent.getParent().getType() != NodeConstants.Types.SOURCE && parent.getParent().getType() != NodeConstants.Types.SET_OP) {
            parent = parent.getParent();
        }
        return parent;
    }

    public static Set<WindowFunction> getWindowFunctions(
            List<Expression> projectCols) {
        LinkedHashSet<WindowFunction> windowFunctions = new LinkedHashSet<WindowFunction>();
        for (Expression singleElementSymbol : projectCols) {
            AggregateSymbolCollectorVisitor.getAggregates(singleElementSymbol, null, null, null, windowFunctions, null);
        }
        return windowFunctions;
    }

    private List<Expression> filterElements(Collection<? extends Expression> requiredInput, Set<GroupSymbol> filterGroups) {
        List<Expression> filteredElements = new ArrayList<Expression>();
        for (Expression element : requiredInput) {
            if(filterGroups.containsAll(GroupsUsedByElementsVisitor.getGroups(element))) {
                filteredElements.add(element);
            }
        }
        return filteredElements;
    }

    /**
     * A special case to consider is when the virtual group is defined by a
     * UNION (no ALL) or a SELECT DISTINCT.  In this case, the dup removal means
     * that all columns need to be used to determine duplicates.  So, filtering the
     * columns at all will alter the number of rows flowing through the frame.
     * So, in this case filtering should not occur.  In fact the output columns
     * that were set on root above are filtered, but we actually want all the
     * virtual elements - so just reset it and proceed as before
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    static List<? extends Expression> determineSourceOutput(PlanNode root,
                                           List<Expression> outputElements,
                                           QueryMetadataInterface metadata,
                                           CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        PlanNode virtualRoot = root.getLastChild();

        if(hasDupRemoval(virtualRoot)) {
            // Reset the outputColumns for this source node to be all columns for the virtual group
            SymbolMap symbolMap = (SymbolMap) root.getProperty(NodeConstants.Info.SYMBOL_MAP);
            if (!symbolMap.asMap().keySet().containsAll(outputElements)) {
                outputElements.removeAll(symbolMap.asMap().keySet());
                 throw new QueryPlannerException(QueryPlugin.Event.TEIID30259, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30259, outputElements));
            }
            return symbolMap.getKeys();
        }
        PlanNode limit = NodeEditor.findNodePreOrder(root, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.PROJECT);
        if (limit == null) {
            return outputElements;
        }
        //reset the output elements to be the output columns + what's required by the sort
        PlanNode sort = NodeEditor.findNodePreOrder(limit, NodeConstants.Types.SORT, NodeConstants.Types.PROJECT);
        if (sort == null) {
            return outputElements;
        }
        PlanNode access = NodeEditor.findParent(sort, NodeConstants.Types.ACCESS);
        if (sort.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT) ||
                (access != null && capFinder != null && CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_UNRELATED, RuleRaiseAccess.getModelIDFromAccess(access, metadata), metadata, capFinder))) {
            return outputElements;
        }
        OrderBy sortOrder = (OrderBy)sort.getProperty(NodeConstants.Info.SORT_ORDER);
        List<Expression> topCols = FrameUtil.findTopCols(sort);

        SymbolMap symbolMap = (SymbolMap)root.getProperty(NodeConstants.Info.SYMBOL_MAP);

        List<ElementSymbol> symbolOrder = symbolMap.getKeys();

        for (OrderByItem item : sortOrder.getOrderByItems()) {
            final Expression expr = item.getSymbol();
            int index = topCols.indexOf(expr);
            if (index < 0) {
                continue;
            }
            ElementSymbol symbol = symbolOrder.get(index);
            if (!outputElements.contains(symbol)) {
                outputElements.add(symbol);
            }
        }
        return outputElements;
    }

    /**
     * <p>This method looks at a source node, which defines a virtual group, and filters the
     * virtual elements defined by the group down into just the output elements needed
     * by that source node.  This means, for instance, that the PROJECT node at the top
     * of the virtual group might need to have some elements removed from the project as
     * those elements are no longer needed.
     *
     * <p>One special case that is handled here is when a virtual group is defined by
     * a UNION ALL.  In this case, the various branches of the union have elements defined
     * and filtering must occur identically in all branches of the union.
     *
     * @param sourceNode Node to filter
     * @param metadata Metadata implementation
     * @return The filtered list of columns for this node (used in recursing tree)
     * @throws QueryPlannerException
     */
    static List<Expression> filterVirtualElements(PlanNode sourceNode, List<Expression> outputColumns, QueryMetadataInterface metadata) throws QueryPlannerException {

        PlanNode virtualRoot = sourceNode.getLastChild();

        // Update project cols - typically there is exactly one and that node can
        // just get the filteredCols determined above.  In the case of one or more
        // nested set operations (UNION, INTERSECT, EXCEPT) there will be 2 or more
        // projects.
        List<PlanNode> allProjects = NodeEditor.findAllNodes(virtualRoot, NodeConstants.Types.PROJECT, NodeConstants.Types.PROJECT);

        int[] filteredIndex = new int[outputColumns.size()];
        Arrays.fill(filteredIndex, -1);

        SymbolMap symbolMap = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

        List<ElementSymbol> originalOrder = symbolMap.getKeys();

        boolean updateGroups = outputColumns.size() != originalOrder.size();
        boolean[] seenIndex = new boolean[outputColumns.size()];
        boolean newSymbols = false;
        int newSymbolIndex = 0;
        for (int i = 0; i < outputColumns.size(); i++) {
            Expression expr = outputColumns.get(i);
            filteredIndex[i] = originalOrder.indexOf(expr);
            if (filteredIndex[i] == -1) {
                updateGroups = true;
                //we're adding this symbol, which needs to be updated against respective symbol maps
                newSymbols = true;
            } else {
                newSymbolIndex++;
            }
            if (!updateGroups) {
                seenIndex[filteredIndex[i]] = true;
            }
        }

        if (!updateGroups) {
            for (boolean b : seenIndex) {
                if (!b) {
                    updateGroups = true;
                    break;
                }
            }
        }

        List<Expression> newCols = null;
        for(int i=allProjects.size()-1; i>=0; i--) {
            PlanNode projectNode = allProjects.get(i);
            List<Expression> projectCols = (List<Expression>) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

            newCols = RelationalNode.projectTuple(filteredIndex, projectCols, true);

            if (newSymbols) {
                SymbolMap childMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), projectCols);
                for (int j = 0; j < filteredIndex.length; j++) {
                    if (filteredIndex[j] != -1) {
                        continue;
                    }
                    Expression ex = (Expression) outputColumns.get(j).clone();
                    ExpressionMappingVisitor.mapExpressions(ex, childMap.asMap());
                    newCols.set(j, ex);
                    if (i == 0) {
                        filteredIndex[j] = newSymbolIndex++;
                    }
                }
            }

            projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, newCols);
            if (updateGroups) {
                projectNode.getGroups().clear();
                projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(newCols));
                projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(projectNode.getCorrelatedReferenceElements()));
            }
        }

        if (!updateGroups) {
            for (int i : filteredIndex) {
                if (i != filteredIndex[i]) {
                    updateGroups = true;
                    break;
                }
            }
        }

        if (updateGroups) {
            SymbolMap newMap = new SymbolMap();
            List<Expression> originalExpressionOrder = symbolMap.getValues();

            for (int i = 0; i < filteredIndex.length; i++) {
                if (filteredIndex[i] < originalOrder.size()) {
                    newMap.addMapping(originalOrder.get(filteredIndex[i]), originalExpressionOrder.get(filteredIndex[i]));
                }
                //else TODO: we may need to create a fake symbol
            }
            sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, newMap);
        }

        // Create output columns for virtual group project
        return newCols;
    }

    /**
     * Check all branches for either a dup removal or a non all union.
     *
     * @param node Root of virtual group (node below source node)
     * @return True if the virtual group at this source node does dup removal
     */
    static boolean hasDupRemoval(PlanNode node) {

        List<PlanNode> nodes = NodeEditor.findAllNodes(node, NodeConstants.Types.DUP_REMOVE|NodeConstants.Types.SET_OP, NodeConstants.Types.DUP_REMOVE|NodeConstants.Types.PROJECT);

        for (PlanNode planNode : nodes) {
            if (planNode.getType() == NodeConstants.Types.DUP_REMOVE
                || (planNode.getType() == NodeConstants.Types.SET_OP && Boolean.FALSE.equals(planNode.getProperty(NodeConstants.Info.USE_ALL)))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Collect all required input symbols for a given node.  Input symbols
     * are any symbols that are required in the processing of this node,
     * for instance to create a new element symbol or sort on it, etc.
     * @param node Node to collect for
     * @param metadata
     * @param capFinder
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    private List<Expression> collectRequiredInputSymbols(PlanNode node, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {

        Set<Expression> requiredSymbols = new LinkedHashSet<Expression>();
        Set<Expression> createdSymbols = new HashSet<Expression>();

        List<Expression> outputCols = (List<Expression>) node.getProperty(NodeConstants.Info.OUTPUT_COLS);

        switch(node.getType()) {
            case NodeConstants.Types.PROJECT:
            {
                List<Expression> projectCols = (List<Expression>) node.getProperty(NodeConstants.Info.PROJECT_COLS);
                PlanNode accessParent = NodeEditor.findParent(node, NodeConstants.Types.ACCESS);
                PlanNode accessNode = null;
                if (accessParent == null) {
                    //find the direct access node
                    accessNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE
                            | NodeConstants.Types.JOIN | NodeConstants.Types.SET_OP | NodeConstants.Types.GROUP);
                }
                for (Expression ss : projectCols) {
                    if(ss instanceof AliasSymbol) {
                        createdSymbols.add(ss);

                        ss = ((AliasSymbol)ss).getSymbol();
                    }

                    if (ss instanceof WindowFunction || ss instanceof ExpressionSymbol) {
                        createdSymbols.add(ss);
                    }
                    if (!pushProjection(node, metadata, capFinder,
                            requiredSymbols, accessParent, accessNode, ss)) {
                        ElementCollectorVisitor.getElements(ss, requiredSymbols);
                    }
                }
                break;
            }
            case NodeConstants.Types.SELECT:
                Criteria selectCriteria = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                ElementCollectorVisitor.getElements(selectCriteria, requiredSymbols);
                break;
            case NodeConstants.Types.JOIN:
                List<Criteria> crits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                if(crits != null) {
                    for (Criteria joinCriteria : crits) {
                        ElementCollectorVisitor.getElements(joinCriteria, requiredSymbols);
                    }
                }
                break;
            case NodeConstants.Types.GROUP:
                List<Expression> groupCols = (List<Expression>) node.getProperty(NodeConstants.Info.GROUP_COLS);
                PlanNode accessParent = NodeEditor.findParent(node, NodeConstants.Types.ACCESS);
                PlanNode accessNode = null;
                if (accessParent == null) {
                    //find the direct access node
                    accessNode = NodeEditor.findNodePreOrder(node.getFirstChild(), NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE
                            | NodeConstants.Types.JOIN | NodeConstants.Types.SET_OP | NodeConstants.Types.GROUP);
                }
                if(groupCols != null) {
                    for (Expression expression : groupCols) {
                        if (!pushProjection(node, metadata, capFinder,
                                requiredSymbols, accessParent, accessNode, expression)) {
                            ElementCollectorVisitor.getElements(expression, requiredSymbols);
                        }
                    }
                }
                SymbolMap symbolMap = (SymbolMap) node.getProperty(NodeConstants.Info.SYMBOL_MAP);
                Set<ElementSymbol> usedAggregates = new HashSet<ElementSymbol>();

                // Take credit for creating any aggregates that are needed above
                for (Expression outputSymbol : outputCols) {
                    if (!(outputSymbol instanceof ElementSymbol)) {
                        continue;
                    }
                    createdSymbols.add(outputSymbol);
                    Expression ex = symbolMap.getMappedExpression((ElementSymbol) outputSymbol);
                    if(ex instanceof AggregateSymbol) {
                        AggregateSymbol agg = (AggregateSymbol)ex;
                        Expression[] aggExprs = agg.getArgs();
                        for (Expression expression : aggExprs) {
                            if (!pushProjection(node, metadata, capFinder,
                                    requiredSymbols, accessParent, accessNode, expression)) {
                                ElementCollectorVisitor.getElements(expression, requiredSymbols);
                            }
                        }
                        OrderBy orderBy = agg.getOrderBy();
                        if(orderBy != null) {
                            ElementCollectorVisitor.getElements(orderBy, requiredSymbols);
                        }
                        Expression condition = agg.getCondition();
                        if(condition != null) {
                            ElementCollectorVisitor.getElements(condition, requiredSymbols);
                        }
                        usedAggregates.add((ElementSymbol) outputSymbol);
                    }
                }
                //update the aggs in the symbolmap
                for (Map.Entry<ElementSymbol, Expression> entry : new ArrayList<Map.Entry<ElementSymbol, Expression>>(symbolMap.asMap().entrySet())) {
                    if (entry.getValue() instanceof AggregateSymbol && !usedAggregates.contains(entry.getKey())) {
                        symbolMap.asUpdatableMap().remove(entry.getKey());
                    }
                }
                if (requiredSymbols.isEmpty() && usedAggregates.isEmpty()) {
                    node.setProperty(Info.IS_OPTIONAL, true);
                }
                break;
        }

        // Gather elements from correlated subquery references;
        for (SymbolMap refs : node.getAllReferences()) {
            for (Expression expr : refs.asMap().values()) {
                ElementCollectorVisitor.getElements(expr, requiredSymbols);
            }
        }

/*        Set<SingleElementSymbol> tempRequired = requiredSymbols;
        requiredSymbols = new LinkedHashSet<SingleElementSymbol>(outputCols);
        requiredSymbols.removeAll(createdSymbols);
        requiredSymbols.addAll(tempRequired);
*/
        // Add any columns to required that are in this node's output but were not created here
        for (Expression currentOutputSymbol : outputCols) {
            if (!createdSymbols.contains(currentOutputSymbol) && (finalRun || node.getType() != NodeConstants.Types.PROJECT || currentOutputSymbol instanceof ElementSymbol)) {
                requiredSymbols.add(currentOutputSymbol);
            }
        }

        //further minimize the required symbols based upon underlying expression (accounts for aliasing)
        //TODO: this should depend upon whether the expressions are deterministic
        if (node.getType() == NodeConstants.Types.PROJECT) {
            Set<Expression> expressions = new HashSet<Expression>();
            for (Iterator<Expression> iterator = requiredSymbols.iterator(); iterator.hasNext();) {
                Expression ses = iterator.next();
                if (!expressions.add(SymbolMap.getExpression(ses))) {
                    iterator.remove();
                }
            }
        }

        return new ArrayList<Expression>(requiredSymbols);
    }

    private boolean pushProjection(PlanNode node,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            Set<Expression> requiredSymbols, PlanNode accessParent,
            PlanNode accessNode, Expression ss)
            throws QueryMetadataException, TeiidComponentException {
        if (finalRun && accessParent == null) {
            Expression ex = SymbolMap.getExpression(ss);
            if (ex instanceof ElementSymbol || ex instanceof Constant) {
                return false;
            }
            Object modelId = null;
            if (accessNode != null) {
                modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
                //narrow check for projection pushing
                if (RuleRaiseAccess.canPushSymbol(ss, true, modelId, metadata, capFinder, null)) {
                    boolean allowed = true;
                    // if there's window functions, we'll only pass through nodes that
                    // don't affect the order or cardinality - with deep introspection, this
                    // could be relaxed
                    if (!getWindowFunctions(Arrays.asList(ss)).isEmpty()) {
                        PlanNode current = accessNode.getParent();
                        while (current != null && current != node) {
                            if ((current.getType() & (NodeConstants.Types.PROJECT | NodeConstants.Types.SOURCE)) != current.getType()) {
                                allowed = false;
                            }
                            current = current.getParent();
                        }
                    }
                    if (allowed) {
                        requiredSymbols.add(ss);
                        return true;
                    }
                }
            }
            if (NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.ACCESS) == null) {
                Collection<Function> functions = FunctionCollectorVisitor.getFunctions(ss, false);
                List<Function> mustPushSubexpression = null;
                for (Function function : functions) {
                    if (function.getFunctionDescriptor().getPushdown() != PushDown.MUST_PUSHDOWN) {
                        continue;
                    }
                    //there is a special check in the evaluator for a must pushdown function to use
                    //the projected value
                    //TODO: we could try to get something in between everything and just the partial
                    if (accessNode != null && RuleRaiseAccess.canPushSymbol(function, true, modelId, metadata, capFinder, null)) {
                        if (mustPushSubexpression == null) {
                            mustPushSubexpression = new ArrayList<Function>();
                        }
                        mustPushSubexpression.add(function);
                        continue;
                    }

                    if (findFunctionTarget(function, function.getFunctionDescriptor(), capFinder, metadata) != null) {
                        continue;
                    }

                    //assume we need the whole thing
                    requiredSymbols.add(ss);
                    checkSymbols = true;
                    return true;
                }
                if (mustPushSubexpression != null) {
                    requiredSymbols.addAll(mustPushSubexpression);
                }
            }
        }
        return false;
    }

    /**
     * Find the first schema name against which this function can be executed, or null for no target
     * @param function
     * @param fd
     * @param capabiltiesFinder
     * @param metadata
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    public static String findFunctionTarget(Function function,
            final FunctionDescriptor fd,
            CapabilitiesFinder capabiltiesFinder, QueryMetadataInterface metadata)
            throws TeiidComponentException, QueryMetadataException {
        for (Object mid : metadata.getModelIDs()) {
            if (metadata.isVirtualModel(mid)) {
                continue;
            }
            String name = metadata.getName(mid);
            SourceCapabilities caps = capabiltiesFinder.findCapabilities(name);
            if (caps != null && caps.supportsCapability(Capability.SELECT_WITHOUT_FROM)
                    && CapabilitiesUtil.supportsScalarFunction(mid, function, metadata, capabiltiesFinder)) {
                return name;
            }
        }
        return null;
    }

    /**
     * Get name of the rule
     * @return Name of the rule
     */
    public String toString() {
        return "AssignOutputElements"; //$NON-NLS-1$
    }

}
