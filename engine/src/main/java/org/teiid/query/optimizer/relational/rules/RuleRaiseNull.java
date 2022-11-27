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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Assertion;
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
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;


/**
 * Will attempt to raise null nodes to their highest points
 */
public final class RuleRaiseNull implements OptimizerRule {

    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        List<PlanNode> nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.NULL);

        //create a new list to iterate over since the original will be modified
        for (PlanNode nullNode : new LinkedList<PlanNode>(nodes)) {
            while (nullNode.getParent() != null && nodes.contains(nullNode)) {
                // Attempt to raise the node
                PlanNode newRoot = raiseNullNode(plan, nodes, nullNode, metadata, capFinder);
                if(newRoot != null) {
                    plan = newRoot;
                } else {
                    break;
                }
            }

            if (nullNode.getParent() == null) {
                nodes.remove(nullNode);
            }
        }

        return plan;
    }

    /**
     * @param nullNode
     * @param metadata
     * @param capFinder
     * @return null if the raising should not continue, else the newRoot
     */
    PlanNode raiseNullNode(PlanNode rootNode, List<PlanNode> nodes, PlanNode nullNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        PlanNode parentNode = nullNode.getParent();

        switch(parentNode.getType()) {
            case NodeConstants.Types.JOIN:
            {
                JoinType jt = (JoinType)parentNode.getProperty(NodeConstants.Info.JOIN_TYPE);
                if (jt == JoinType.JOIN_CROSS || jt == JoinType.JOIN_INNER) {
                    return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                }
                //for outer joins if the null node is on the outer side, then the join itself is null
                //if the null node is on the inner side, then the join can be removed but the null values
                //coming from the inner side will need to be placed into the frame
                if (jt == JoinType.JOIN_LEFT_OUTER) {
                    if (nullNode == parentNode.getFirstChild()) {
                        return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                    }
                    raiseNullThroughJoin(metadata, parentNode, parentNode.getLastChild());
                    return null;
                }
                if (jt == JoinType.JOIN_RIGHT_OUTER) {
                    if (nullNode == parentNode.getLastChild()) {
                        return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                    }
                    raiseNullThroughJoin(metadata, parentNode, parentNode.getFirstChild());
                    return null;
                }
                if (jt == JoinType.JOIN_FULL_OUTER) {
                    if (nullNode == parentNode.getLastChild()) {
                        raiseNullThroughJoin(metadata, parentNode, parentNode.getLastChild());
                    } else {
                        raiseNullThroughJoin(metadata, parentNode, parentNode.getFirstChild());
                    }
                    return null;
                }
                break;
            }
            case NodeConstants.Types.SET_OP:
            {
                boolean isLeftChild = parentNode.getFirstChild() == nullNode;
                SetQuery.Operation operation = (SetQuery.Operation)parentNode.getProperty(NodeConstants.Info.SET_OPERATION);
                boolean raiseOverSetOp = (operation == SetQuery.Operation.INTERSECT || (operation == SetQuery.Operation.EXCEPT && isLeftChild));

                if (raiseOverSetOp) {
                    return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                }

                boolean isAll = parentNode.hasBooleanProperty(NodeConstants.Info.USE_ALL);

                if (isLeftChild) {
                    PlanNode firstProject = NodeEditor.findNodePreOrder(parentNode, NodeConstants.Types.PROJECT);

                    if (firstProject == null) { // will only happen if the other branch has only null nodes
                        return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                    }

                    List<Expression> newProjectSymbols = (List<Expression>)firstProject.getProperty(NodeConstants.Info.PROJECT_COLS);
                    List<Expression> oldProjectSymbols = (List<Expression>)nullNode.getProperty(NodeConstants.Info.PROJECT_COLS);

                    for (int i = 0; i < newProjectSymbols.size(); i++) {
                        Expression newSes = newProjectSymbols.get(i);
                        Expression oldSes = oldProjectSymbols.get(i);
                        if (!(newSes instanceof Symbol) || !Symbol.getShortName(newSes).equals(Symbol.getShortName(oldSes))) {
                            if (newSes instanceof AliasSymbol) {
                                newSes = ((AliasSymbol)newSes).getSymbol();
                            }
                            newProjectSymbols.set(i, new AliasSymbol(Symbol.getShortName(oldSes), newSes));
                        }
                    }

                    PlanNode sort = NodeEditor.findParent(parentNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);

                    if (sort != null) { //correct the sort to the new columns as well
                        OrderBy sortOrder = (OrderBy)sort.getProperty(NodeConstants.Info.SORT_ORDER);
                        for (OrderByItem item : sortOrder.getOrderByItems()) {
                            Expression sortElement = item.getSymbol();
                            sortElement = newProjectSymbols.get(oldProjectSymbols.indexOf(sortElement));
                            item.setSymbol(sortElement);
                        }
                    }

                    //repair the upper symbol map if needed
                    PlanNode sourceNode = NodeEditor.findParent(parentNode, NodeConstants.Types.SOURCE);
                    if (sourceNode != null && NodeEditor.findNodePreOrder(sourceNode, NodeConstants.Types.PROJECT) == firstProject) {
                        SymbolMap symbolMap = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
                        if (!firstProject.hasProperty(Info.INTO_GROUP) && symbolMap != null) {
                            symbolMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), newProjectSymbols);
                            sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, symbolMap);
                        }
                    }
                }

                NodeEditor.removeChildNode(parentNode, nullNode);

                PlanNode grandParent = parentNode.getParent();
                PlanNode nestedSetOp = NodeEditor.findNodePreOrder(parentNode.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);

                if (!isAll) { //ensure that the new child is distinct
                    if (nestedSetOp != null) {
                        nestedSetOp.setProperty(NodeConstants.Info.USE_ALL, false);
                    } else if (NodeEditor.findNodePreOrder(parentNode.getFirstChild(), NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.SOURCE) == null) {
                        parentNode.getFirstChild().addAsParent(NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE));
                    }
                }

                if (grandParent == null) {
                    PlanNode newRoot = parentNode.getFirstChild();
                    parentNode.removeChild(newRoot);
                    return newRoot;
                }

                //the old estimates should be considered invalid
                grandParent.setProperty(Info.EST_CARDINALITY, null);
                grandParent.setProperty(Info.EST_COL_STATS, null);

                //remove the set op
                NodeEditor.removeChildNode(grandParent, parentNode);

                PlanNode sourceNode = NodeEditor.findParent(grandParent.getFirstChild(), NodeConstants.Types.SOURCE, NodeConstants.Types.SET_OP);
                PlanNode accessNode = NodeEditor.findParent(sourceNode, NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE);

                //remove the source node only if it doesn't create a situation that is invalid for dependent join processing
                if (sourceNode != null && (nestedSetOp == null || accessNode == null || !accessNode.hasBooleanProperty(Info.IS_DEPENDENT_SET))) {
                    return RuleMergeVirtual.doMerge(sourceNode, rootNode, false, metadata, capFinder);
                }
                return null;
            }
            case NodeConstants.Types.GROUP:
            {
                //if there are grouping columns, then we can raise
                if (parentNode.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
                    return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                }
                break; //- the else case could be implemented, but it's a lot of work for little gain, since the null node can't raise higher
            }
            case NodeConstants.Types.PROJECT:
            {
                // check for project into
                PlanNode upperProject = NodeEditor.findParent(parentNode.getParent(), NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);

                if (upperProject == null
                        || upperProject.getProperty(NodeConstants.Info.INTO_GROUP) == null) {
                    return raiseNullNode(rootNode, parentNode, nullNode, nodes);
                }
                break;
            }
            case NodeConstants.Types.SOURCE:
            {
                PlanNode upperProject = parentNode.getParent();
                if (upperProject != null && upperProject.getType() == NodeConstants.Types.PROJECT && upperProject.hasProperty(Info.INTO_GROUP)) {
                    break; //an insert plan
                }
                return raiseNullNode(rootNode, parentNode, nullNode, nodes);
            }
            default:
            {
                return raiseNullNode(rootNode, parentNode, nullNode, nodes);
            }
        }
        return null;
    }

    private PlanNode raiseNullNode(PlanNode rootNode, PlanNode parentNode, PlanNode nullNode, List<PlanNode> nodes) {
        if (parentNode.getType() == NodeConstants.Types.SOURCE) {
            nullNode.getGroups().clear();
        } else if (parentNode.getType() == NodeConstants.Types.PROJECT) {
            nullNode.setProperty(NodeConstants.Info.PROJECT_COLS, parentNode.getProperty(NodeConstants.Info.PROJECT_COLS));
        }
        nullNode.addGroups(parentNode.getGroups());
        parentNode.removeChild(nullNode);
        nodes.removeAll(NodeEditor.findAllNodes(parentNode, NodeConstants.Types.NULL));
        if (parentNode.getParent() != null) {
            parentNode.getParent().replaceChild(parentNode, nullNode);
        } else {
            rootNode = nullNode;
        }
        return rootNode;
    }

    /**
     * Given a joinNode that should be an outer join and a null node as one of its children, replace elements in
     * the current frame from the null node groups with null values
     *
     * @param metadata
     * @param joinNode
     * @param nullNode
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    static void raiseNullThroughJoin(QueryMetadataInterface metadata,
                                      PlanNode joinNode,
                                      PlanNode nullNode) throws QueryPlannerException,
                                                         QueryMetadataException,
                                                         TeiidComponentException {
        Assertion.assertTrue(joinNode.getType() == NodeConstants.Types.JOIN);
        Assertion.assertTrue(nullNode.getType() == NodeConstants.Types.NULL);
        Assertion.assertTrue(nullNode.getParent() == joinNode);

        PlanNode frameStart = joinNode.getParent();

        NodeEditor.removeChildNode(joinNode, nullNode);
        NodeEditor.removeChildNode(joinNode.getParent(), joinNode);

        for (GroupSymbol group : nullNode.getGroups()) {
            Map<ElementSymbol, Expression> nullSymbolMap = FrameUtil.buildSymbolMap(group, null, metadata);
            FrameUtil.convertFrame(frameStart, group, null, nullSymbolMap, metadata);
        }

    }

    public String toString() {
        return "RaiseNull"; //$NON-NLS-1$
    }

}