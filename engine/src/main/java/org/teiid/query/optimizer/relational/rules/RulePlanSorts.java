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
import java.util.LinkedHashSet;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


/**
 * Attempts to minimize the cost of sorting operations across the plan.
 *
 * Must be run after output elements are assigned
 */
public class RulePlanSorts implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, RuleStack rules,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        return optimizeSorts(false, plan, plan, metadata, capabilitiesFinder, analysisRecord, context);
    }

    private PlanNode optimizeSorts(boolean parentBlocking, PlanNode node, PlanNode root, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record, CommandContext context) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        node = NodeEditor.findNodePreOrder(node,
                NodeConstants.Types.SORT
                | NodeConstants.Types.DUP_REMOVE
                | NodeConstants.Types.GROUP
                | NodeConstants.Types.JOIN
                | NodeConstants.Types.SET_OP, NodeConstants.Types.ACCESS);
        if (node == null) {
            return root;
        }
        switch (node.getType()) {
        case NodeConstants.Types.SORT:
            parentBlocking = true;
            if (node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
                break;
            }
            if (mergeSortWithDupRemoval(node)) {
                node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
            } else {
                root = checkForProjectOptimization(node, root, metadata, capFinder, record, context);
                if (NodeEditor.findParent(node, NodeConstants.Types.ACCESS) != null) {
                    return root;
                }
            }
            OrderBy orderBy = (OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER);
            List<Expression> orderColumns = orderBy.getSortKeys();
            List<Expression> sortExpressions = new ArrayList<Expression>(orderColumns.size());
            PlanNode possibleSort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.ACCESS);
            if (possibleSort != null && !possibleSort.hasBooleanProperty(Info.ROLLUP)) {
                boolean otherExpression = false;
                SymbolMap groupMap = (SymbolMap)possibleSort.getProperty(Info.SYMBOL_MAP);
                for (Expression singleElementSymbol : orderColumns) {
                    Expression ex = SymbolMap.getExpression(singleElementSymbol);
                    if (ex instanceof ElementSymbol) {
                        sortExpressions.add(groupMap.getMappedExpression((ElementSymbol) ex));
                    } else {
                        otherExpression = true;
                        break;
                    }
                }

                List<Expression> exprs = (List<Expression>)possibleSort.getProperty(Info.GROUP_COLS);
                if (!otherExpression && exprs != null && exprs.containsAll(sortExpressions)) {
                    exprs.removeAll(sortExpressions);
                    exprs.addAll(0, sortExpressions);
                    if (node.getParent() == null) {
                        root = node.getFirstChild();
                        root.removeFromParent();
                        Object cols = node.getProperty(NodeConstants.Info.OUTPUT_COLS);
                        root.setProperty(NodeConstants.Info.OUTPUT_COLS, cols);
                        if (root.getType() == NodeConstants.Types.PROJECT) {
                            root.setProperty(NodeConstants.Info.PROJECT_COLS, cols);
                        }
                        node = root;
                    } else {
                        PlanNode nextNode = node.getFirstChild();
                        NodeEditor.removeChildNode(node.getParent(), node);
                        node = nextNode;
                    }
                    possibleSort.setProperty(Info.SORT_ORDER, orderBy);
                }
            }
            break;
        case NodeConstants.Types.DUP_REMOVE:
            if (parentBlocking) {
                node.setType(NodeConstants.Types.SORT);
                node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
            }
            break;
        case NodeConstants.Types.GROUP:
            if (!node.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
                break;
            }
            SymbolMap map = (SymbolMap)node.getProperty(Info.SYMBOL_MAP);
            boolean cardinalityDependent = false;
            boolean canOptimize = true;
            for (Expression ex : map.asMap().values()) {
                if (ex instanceof AggregateSymbol) {
                    AggregateSymbol agg = (AggregateSymbol)ex;
                    if (agg.isCardinalityDependent()) {
                        cardinalityDependent = true;
                        break;
                    }
                } else if (!(ex instanceof ElementSymbol)) {
                    //there is an expression in the grouping columns
                    canOptimize = false;
                    break;
                }
            }
            if (canOptimize && mergeSortWithDupRemovalAcrossSource(node, false)) {
                node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
                if (cardinalityDependent) {
                    PlanNode source = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SOURCE);
                    List<Expression> sourceOutput = (List<Expression>)source.getProperty(Info.OUTPUT_COLS);
                    PlanNode child = node.getFirstChild();
                    while (child != source) {
                        child.setProperty(Info.OUTPUT_COLS, sourceOutput);
                        child = child.getFirstChild();
                    }
                }
            }
            //TODO: check the join interesting order
            parentBlocking = true;
            break;
        case NodeConstants.Types.JOIN:
            if (node.getProperty(NodeConstants.Info.JOIN_STRATEGY) == JoinStrategyType.NESTED_LOOP
                    || node.getProperty(NodeConstants.Info.JOIN_STRATEGY) == JoinStrategyType.NESTED_TABLE) {
                break;
            }
            /*
             *  Look under the left and the right sources for a dup removal operation
             *  join
             *   [project]
             *     source
             *       dup remove | union not all
             */
            parentBlocking = true;
            PlanNode toTest = node.getFirstChild();
            if (mergeSortWithDupRemovalAcrossSource(toTest, true)) {
                node.setProperty(NodeConstants.Info.SORT_LEFT, SortOption.SORT_DISTINCT);
                if (node.getProperty(NodeConstants.Info.SORT_RIGHT) != SortOption.SORT) {
                    node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
                }
            }
            toTest = node.getLastChild();
            if (mergeSortWithDupRemovalAcrossSource(toTest, true)) {
                node.setProperty(NodeConstants.Info.SORT_RIGHT, SortOption.SORT_DISTINCT);
                if (node.getProperty(NodeConstants.Info.SORT_LEFT) != SortOption.SORT) {
                    node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
                }
            }
            break;
        case NodeConstants.Types.SET_OP:
            // assumes the use of the merge algorithm
            if (node.getProperty(NodeConstants.Info.SET_OPERATION) != SetQuery.Operation.UNION) {
                parentBlocking = true;
            } else if (!node.hasBooleanProperty(NodeConstants.Info.USE_ALL) && !parentBlocking) {
                //do the incremental dup removal for lower latency
                node.setProperty(NodeConstants.Info.IS_DUP_REMOVAL, true);
            }
            break;
        }
        for (PlanNode child : node.getChildren()) {
            root = optimizeSorts(parentBlocking, child, root, metadata, capFinder, record, context);
        }
        return root;
    }

    static PlanNode checkForProjectOptimization(PlanNode node, PlanNode root,
            QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord record, CommandContext context) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        PlanNode projectNode = node.getFirstChild();
        PlanNode parent = node.getParent();
        boolean raiseAccess = false;
        //special check for unrelated order by compensation
        if (projectNode.getType() == NodeConstants.Types.ACCESS && RuleRaiseAccess.canRaiseOverSort(projectNode, metadata, capFinder, node, record, true, context)) {
            projectNode = NodeEditor.findNodePreOrder(projectNode, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP);
            if (projectNode == null) {
                return root; //no interviening project
            }
            raiseAccess = true;
        } else if (projectNode.getType() == NodeConstants.Types.PROJECT && projectNode.getFirstChild() != null) {
            raiseAccess = projectNode.getFirstChild().getType() == NodeConstants.Types.ACCESS
                && RuleRaiseAccess.canRaiseOverSort(projectNode.getFirstChild(), metadata, capFinder, node, record, false, context);

            //if we can't raise the access node and this doesn't have a limit, there's no point in optimizing
            if (!raiseAccess && (parent == null || parent.getType() != NodeConstants.Types.TUPLE_LIMIT)) {
                return root;
            }
        } else {
            return root;
        }
        List<Expression> childOutputCols = (List<Expression>) projectNode.getFirstChild().getProperty(Info.OUTPUT_COLS);
        OrderBy orderBy = (OrderBy) node.getProperty(Info.SORT_ORDER);
        List<Expression> orderByKeys = orderBy.getSortKeys();
        LinkedHashSet<Expression> toProject = new LinkedHashSet();
        for (Expression ss : orderByKeys) {
            Expression original = ss;
            if(ss instanceof AliasSymbol) {
                ss = ((AliasSymbol)ss).getSymbol();
            }
            if (ss instanceof ExpressionSymbol) {
                if (!raiseAccess) {
                    return root; //TODO: insert a new project node to handle this case
                }
            }
            if (!childOutputCols.contains(ss)) {
                if (!raiseAccess) {
                    return root;
                }
                toProject.add(original);
            }
        }
        PlanNode toRepair = projectNode.getParent();
        if (!toProject.isEmpty()) {
            PlanNode intermediateProject = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
            toProject.addAll(childOutputCols);
            List<Expression> projectCols = new ArrayList<Expression>(toProject);
            childOutputCols = projectCols;
            intermediateProject.setProperty(NodeConstants.Info.PROJECT_COLS, projectCols);
            intermediateProject.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList<Expression>(projectCols));
            toRepair.getFirstChild().addAsParent(intermediateProject);
        }
        NodeEditor.removeChildNode(projectNode.getParent(), projectNode);
        if (parent != null && parent.getType() == NodeConstants.Types.TUPLE_LIMIT && parent.getParent() != null) {
            parent.addAsParent(projectNode);
        } else {
            if (parent == null) {
                root = projectNode;
            }
            if (parent != null && parent.getType() == NodeConstants.Types.TUPLE_LIMIT) {
                if (root == parent) {
                    root = projectNode;
                }
                projectNode.addFirstChild(parent);
            } else {
                projectNode.addFirstChild(node);
            }
        }
        List<Expression> orderByOutputSymbols = (List<Expression>) node.getProperty(Info.OUTPUT_COLS);
        boolean unrelated = false;
        if (node.hasBooleanProperty(Info.UNRELATED_SORT)) {
            node.setProperty(Info.UNRELATED_SORT, false);
            unrelated = true;
        }
        for (OrderByItem item : orderBy.getOrderByItems()) {
            if (unrelated || !toProject.isEmpty()) {
                //update sort order
                int index = childOutputCols.indexOf(item.getSymbol());
                item.setExpressionPosition(index);
            }
            if (toProject.isEmpty()) {
                //strip alias as project was raised
                if (item.getSymbol() instanceof AliasSymbol) {
                    item.setSymbol(((AliasSymbol)item.getSymbol()).getSymbol());
                }
            }
        }
        while (toRepair != node) {
            toRepair.setProperty(Info.OUTPUT_COLS, childOutputCols);
            toRepair = toRepair.getParent();
        }
        projectNode.setProperty(Info.OUTPUT_COLS, orderByOutputSymbols);
        projectNode.setProperty(Info.PROJECT_COLS, orderByOutputSymbols);
        node.setProperty(Info.OUTPUT_COLS, childOutputCols);
        if (parent != null) {
            parent.setProperty(Info.OUTPUT_COLS, childOutputCols);
        }
        if (raiseAccess) {
            PlanNode accessNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.ACCESS);

            //instead of just calling ruleraiseaccess, we're more selective
            //we do not want to raise the access node over a project that is handling an unrelated sort
            PlanNode newRoot = RuleRaiseAccess.raiseAccessNode(root, accessNode, metadata, capFinder, true, record, context);
            if (newRoot != null) {
                accessNode.setProperty(NodeConstants.Info.OUTPUT_COLS, childOutputCols);
                root = newRoot;
                if (!toProject.isEmpty()) {
                    newRoot = RuleRaiseAccess.raiseAccessNode(root, accessNode, metadata, capFinder, true, record, context);
                }
                if (newRoot != null) {
                    root = newRoot;
                    if (accessNode.getParent().getType() == NodeConstants.Types.TUPLE_LIMIT) {
                        newRoot = RulePushLimit.raiseAccessOverLimit(root, accessNode, metadata, capFinder, accessNode.getParent(), record);
                    }
                    if (newRoot != null) {
                        root = newRoot;
                    }
                }
            }
        }
        return root;
    }

    private boolean mergeSortWithDupRemovalAcrossSource(PlanNode toTest, boolean join) {
        PlanNode source = NodeEditor.findNodePreOrder(toTest, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS | NodeConstants.Types.JOIN);
        boolean result = source != null && mergeSortWithDupRemoval(source);
        if (result) {
            //since we pull up the operation, rather than tracking the interesting order
            //we need the output cols to match on the way up.  See TEIID-5434
            PlanNode rootNode = join?toTest:toTest.getFirstChild();
            List<Expression> cols = (List<Expression>) rootNode.getProperty(Info.OUTPUT_COLS);
            List<Expression> sourceCols = (List<Expression>) source.getProperty(Info.OUTPUT_COLS);
            if (!cols.containsAll(sourceCols)) {
                while (rootNode != source) {
                    //update output cols
                    LinkedHashSet<Expression> allCols = new LinkedHashSet<Expression>((List<Expression>) rootNode.getProperty(Info.OUTPUT_COLS));
                    allCols.addAll(sourceCols);
                    rootNode.setProperty(Info.OUTPUT_COLS, new ArrayList<Expression>(allCols));
                    rootNode = rootNode.getFirstChild();
                }
            }
        }
        return result;
    }

    private boolean mergeSortWithDupRemoval(PlanNode node) {
        if (node.getFirstChild() == null) {
            return false;
        }
        switch (node.getFirstChild().getType()) {
        case NodeConstants.Types.SET_OP:
            if (node.getFirstChild().getProperty(NodeConstants.Info.SET_OPERATION) == SetQuery.Operation.UNION && !node.getFirstChild().hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
                node.getFirstChild().setProperty(NodeConstants.Info.USE_ALL, true);
                return true;
            }
            break;
        case NodeConstants.Types.DUP_REMOVE:
            NodeEditor.removeChildNode(node, node.getFirstChild());
            return true;
        }
        if (node.hasBooleanProperty(Info.UNRELATED_SORT)) {
            PlanNode source = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SOURCE);
            if (source != null) {
                PlanNode parentProject = NodeEditor.findParent(source, NodeConstants.Types.PROJECT);
                if (parentProject != null && parentProject.getProperty(Info.PROJECT_COLS).equals(source.getProperty(Info.OUTPUT_COLS))) {
                    //can't sort on a derived expression
                    return mergeSortWithDupRemoval(source);
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "PlanSorts"; //$NON-NLS-1$
    }

}
