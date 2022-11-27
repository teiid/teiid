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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
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
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Inserts sort nodes for specific join strategies.
 */
public class RuleImplementJoinStrategy implements OptimizerRule {

    /**
     * @see org.teiid.query.optimizer.relational.OptimizerRule#execute(org.teiid.query.optimizer.relational.plantree.PlanNode, org.teiid.query.metadata.QueryMetadataInterface, org.teiid.query.optimizer.capabilities.CapabilitiesFinder, org.teiid.query.optimizer.relational.RuleStack, org.teiid.query.analysis.AnalysisRecord, org.teiid.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {

        for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS)) {
            SymbolMap references = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            if (references != null) {
                Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(references.getValues());
                PlanNode joinNode = NodeEditor.findParent(sourceNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
                while (joinNode != null) {
                    if (joinNode.getGroups().containsAll(groups)) {
                        joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_TABLE);
                        Info info = Info.RIGHT_NESTED_REFERENCES;
                        if (!FrameUtil.findJoinSourceNode(joinNode.getFirstChild()).getGroups().containsAll(groups)) {
                            throw new AssertionError("Should not have reordered the join tree to reverse the lateral join");  //$NON-NLS-1$
                        }
                        SymbolMap map = (SymbolMap) joinNode.getProperty(info);
                        if (map == null) {
                            map = new SymbolMap();
                        }
                        joinNode.setProperty(info, map);
                        map.asUpdatableMap().putAll(references.asMap());
                        if (joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) != null) {
                            //sanity check
                            throw new AssertionError("Cannot use a depenedent join when the join involves a correlated nested table.");  //$NON-NLS-1$
                        }
                        break;
                    }
                    joinNode = NodeEditor.findParent(joinNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
                }
            }
        }

        for (PlanNode joinNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
            JoinStrategyType stype = (JoinStrategyType) joinNode.getProperty(NodeConstants.Info.JOIN_STRATEGY);
            if (!JoinStrategyType.MERGE.equals(stype)) {
                continue;
            }

            List<Expression> leftExpressions = (List<Expression>) joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
            List<Expression> rightExpressions = (List<Expression>) joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
            int origExpressionCount = leftExpressions.size();

            //check index information on each side
            //TODO: don't do null order compensation - in fact we should check what the order actually is, but we don't have that metadata
            Object key = null;
            boolean right = true;
            //we check the right first, since it should be larger
            if (joinNode.getLastChild().getType() == NodeConstants.Types.ACCESS && NewCalculateCostUtil.isSingleTable(joinNode.getLastChild())) {
                key = NewCalculateCostUtil.getKeyUsed(rightExpressions, null, metadata, null);
            }
            if (key == null && joinNode.getFirstChild().getType() == NodeConstants.Types.ACCESS && NewCalculateCostUtil.isSingleTable(joinNode.getFirstChild())) {
                key = NewCalculateCostUtil.getKeyUsed(leftExpressions, null, metadata, null);
                right = false;
            }
            JoinType joinType = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
            /**
             * Don't push sorts for unbalanced inner joins, we prefer to use a processing time cost based decision
             */
            boolean pushLeft = true;
            boolean pushRight = true;
            if ((joinType == JoinType.JOIN_INNER || joinType == JoinType.JOIN_LEFT_OUTER) && context != null) {
                float leftCost = NewCalculateCostUtil.computeCostForTree(joinNode.getFirstChild(), metadata);
                float rightCost = NewCalculateCostUtil.computeCostForTree(joinNode.getLastChild(), metadata);
                if (leftCost != NewCalculateCostUtil.UNKNOWN_VALUE && rightCost != NewCalculateCostUtil.UNKNOWN_VALUE
                        && (leftCost > context.getProcessorBatchSize() || rightCost > context.getProcessorBatchSize())) {
                    //we use a larger constant here to ensure that we don't unwisely prevent pushdown
                    pushLeft = leftCost < context.getProcessorBatchSize() || leftCost / rightCost < 8 || (key != null && !right);
                    pushRight = rightCost < context.getProcessorBatchSize() || rightCost / leftCost < 8 || joinType == JoinType.JOIN_LEFT_OUTER || (key != null && right);
                }
            }

            if (key != null && joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) == null) {
                //redo the join predicates based upon the key alone
                List<Object> keyCols = metadata.getElementIDsInKey(key);
                int[] reorder = new int[keyCols.size()];
                LinkedHashSet<Integer> toCriteria = new LinkedHashSet<Integer>();
                List<Expression> keyExpressions = right?rightExpressions:leftExpressions;
                Map<Object, Integer> indexMap = new LinkedHashMap<Object, Integer>();
                for (int i = 0; i < keyExpressions.size(); i++) {
                    Expression ses = keyExpressions.get(i);
                    if (!(ses instanceof ElementSymbol)) {
                        toCriteria.add(i);
                        continue;
                    }
                    Integer existing = indexMap.put(((ElementSymbol)ses).getMetadataID(), i);
                    if (existing != null) {
                        toCriteria.add(existing);
                    }
                }
                boolean found = true;
                for (int i = 0; i < keyCols.size(); i++) {
                    Object id = keyCols.get(i);
                    Integer index = indexMap.remove(id);
                    if (index == null) {
                        found = false;
                        break;
                    }
                    reorder[i] = index;
                }
                if (found) {
                    toCriteria.addAll(indexMap.values());
                    List<Criteria> joinCriteria = (List<Criteria>) joinNode.getProperty(Info.NON_EQUI_JOIN_CRITERIA);
                    for (int index : toCriteria) {
                        Expression lses = leftExpressions.get(index);
                        Expression rses = rightExpressions.get(index);
                        CompareCriteria cc = new CompareCriteria(lses, CompareCriteria.EQ, rses);
                        if (joinCriteria == null || joinCriteria.isEmpty()) {
                            joinCriteria = new ArrayList<Criteria>();
                        }
                        joinCriteria.add(cc);
                    }
                    joinNode.setProperty(Info.NON_EQUI_JOIN_CRITERIA, joinCriteria);
                    leftExpressions = RelationalNode.projectTuple(reorder, leftExpressions);
                    rightExpressions = RelationalNode.projectTuple(reorder, rightExpressions);
                    joinNode.setProperty(NodeConstants.Info.LEFT_EXPRESSIONS, leftExpressions);
                    joinNode.setProperty(NodeConstants.Info.RIGHT_EXPRESSIONS, rightExpressions);
                }
            }

            boolean pushedLeft = insertSort(joinNode.getFirstChild(), leftExpressions, joinNode, metadata, capabilitiesFinder, pushLeft, context);

            //TODO: this check could be performed, as it implies we're using enhanced and can back out of the sort
            //      but this not valid in all circumstances
            //if (!pushedLeft && joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) != null && joinType == JoinType.JOIN_INNER) {
                //pushRight = true; //this sort will not be used if more than one source command is generated
            //}

            if (origExpressionCount == 1
                    && joinType == JoinType.JOIN_INNER
                    && joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE) != null
                    && !joinNode.hasCollectionProperty(Info.NON_EQUI_JOIN_CRITERIA)) {
                Collection<Expression> output = (Collection<Expression>) joinNode.getProperty(NodeConstants.Info.OUTPUT_COLS);
                Collection<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(output);
                if (Collections.disjoint(groups, FrameUtil.findJoinSourceNode(joinNode.getFirstChild()).getGroups())) {
                    pushRight = false;
                    joinNode.setProperty(Info.IS_SEMI_DEP, Boolean.TRUE);
                }
            }

            boolean pushedRight = insertSort(joinNode.getLastChild(), rightExpressions, joinNode, metadata, capabilitiesFinder, pushRight, context);
            if ((!pushedRight || !pushedLeft) && (joinType == JoinType.JOIN_INNER || (joinType == JoinType.JOIN_LEFT_OUTER && !pushedLeft))) {
                joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.ENHANCED_SORT);
            }
        }

        return plan;
    }

    /**
     * Insert a sort node under the merge join node.  If necessary, also insert a project
     * node to handle function evaluation.
     * @param expressions The expressions that need to be sorted on
     * @param jnode The planner merge join node to attach to
     * @return returns true if a project node needs added
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    static boolean insertSort(PlanNode childNode, List<Expression> expressions, PlanNode jnode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
            boolean attemptPush, CommandContext context) throws QueryMetadataException, TeiidComponentException {
        Set<Expression> orderSymbols = new LinkedHashSet<Expression>(expressions);

        PlanNode sourceNode = FrameUtil.findJoinSourceNode(childNode);
        PlanNode joinNode = childNode.getParent();

        Set<Expression> outputSymbols = new LinkedHashSet<Expression>((List<Expression>)sourceNode.getProperty(NodeConstants.Info.OUTPUT_COLS));

        int oldSize = outputSymbols.size();

        outputSymbols.addAll(expressions);

        //TODO: generally we're compensating for expressions used in predicates
        //and end up pulling redundant values
        boolean needsCorrection = outputSymbols.size() > oldSize;

        PlanNode sortNode = createSortNode(new ArrayList<Expression>(orderSymbols), outputSymbols);

        boolean distinct = false;
        if (sourceNode.getFirstChild() != null && sourceNode.getType() == NodeConstants.Types.SOURCE && outputSymbols.size() == expressions.size() && outputSymbols.containsAll(expressions)) {
            PlanNode setOp = NodeEditor.findNodePreOrder(sourceNode.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
            if (setOp != null) {
                if (!setOp.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
                    distinct = true;
                }
            } else if (NodeEditor.findNodePreOrder(sourceNode.getFirstChild(), NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.PROJECT) != null) {
                distinct = true;
            }
        }

        boolean sort = true;

        if (sourceNode.getType() == NodeConstants.Types.ACCESS) {
            boolean usesKey = NewCalculateCostUtil.usesKey(sourceNode, expressions, metadata);
            if (distinct || usesKey) {
                joinNode.setProperty(joinNode.getFirstChild() == childNode ? NodeConstants.Info.IS_LEFT_DISTINCT : NodeConstants.Info.IS_RIGHT_DISTINCT, true);
            }
            if (!usesKey && RuleRaiseAccess.getModelIDFromAccess(sourceNode, metadata) == TempMetadataAdapter.TEMP_MODEL) {
                attemptPush = false;
            }
            if (attemptPush && RuleRaiseAccess.canRaiseOverSort(sourceNode, metadata, capFinder, sortNode, null, false, context, true)) {
                sourceNode.getFirstChild().addAsParent(sortNode);

                if (needsCorrection) {
                    correctOutputElements(joinNode, expressions, sortNode.getParent());
                }
                return true;
            }
        } else if (sourceNode.getType() == NodeConstants.Types.GROUP && !sourceNode.hasBooleanProperty(Info.ROLLUP)) {
            sourceNode.addAsParent(sortNode);
            sort = false; // the grouping columns must contain all of the ordering columns
        }

        if (distinct) {
            joinNode.setProperty(joinNode.getFirstChild() == childNode ? NodeConstants.Info.IS_LEFT_DISTINCT : NodeConstants.Info.IS_RIGHT_DISTINCT, true);
        }

        if (sort) {
            joinNode.setProperty(joinNode.getFirstChild() == childNode ? NodeConstants.Info.SORT_LEFT : NodeConstants.Info.SORT_RIGHT, SortOption.SORT);
        }

        if (needsCorrection) {
            PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
            projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, new ArrayList<Expression>(outputSymbols));
            projectNode.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList<Expression>(outputSymbols));
            sourceNode.addAsParent(projectNode);
            correctOutputElements(joinNode, expressions, projectNode.getParent());
        }
        return false;
    }

    private static PlanNode createSortNode(List<Expression> orderSymbols,
                                           Collection<Expression> outputElements) {
        PlanNode sortNode = NodeFactory.getNewNode(NodeConstants.Types.SORT);
        OrderBy order = new OrderBy(orderSymbols);
        order.setUserOrdering(false);
        sortNode.setProperty(NodeConstants.Info.SORT_ORDER, order);
        sortNode.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList<Expression>(outputElements));
        return sortNode;
    }

    private static void correctOutputElements(PlanNode endNode,
                                              Collection<Expression> outputElements,
                                              PlanNode startNode) {
        while (startNode != endNode) {
            LinkedHashSet<Expression> outputSymbols = new LinkedHashSet<Expression>((List<Expression>)startNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
            outputSymbols.addAll(outputElements);
            startNode.setProperty(NodeConstants.Info.OUTPUT_COLS, new ArrayList<Expression>(outputSymbols));
            startNode = startNode.getParent();
        }
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "ImplementJoinStrategy"; //$NON-NLS-1$
    }

}
