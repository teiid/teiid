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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
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
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RuleMergeVirtual implements
                                   OptimizerRule {

    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
        boolean beforeDecomposeJoin = rules.contains(RuleConstants.DECOMPOSE_JOIN);
        for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            if (sourceNode.getChildCount() > 0) {
                plan = doMerge(sourceNode, plan, beforeDecomposeJoin, metadata, capFinder);
            }
        }

        return plan;
    }

    static PlanNode doMerge(PlanNode frame,
                            PlanNode root, boolean beforeDecomposeJoin,
                            QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        if (frame.hasBooleanProperty(Info.NO_UNNEST)) {
            return root;
        }
        GroupSymbol virtualGroup = frame.getGroups().iterator().next();

        // check to see if frame represents a proc relational query.
        if (virtualGroup.isProcedure()) {
            return root;
        }

        List<PlanNode> sources = NodeEditor.findAllNodes(frame.getFirstChild(), NodeConstants.Types.SOURCE | NodeConstants.Types.NULL, NodeConstants.Types.SOURCE);

        SymbolMap references = (SymbolMap)frame.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
        if (references != null) {
            if (!sources.isEmpty()) {
                return root; //correlated nested table commands should not be merged
            }

            //this is ok only if all of the references go above the correlating join
            //currently this check is simplistic - just look at the parent join more nested scenarios won't work
            PlanNode parentJoin = NodeEditor.findParent(frame, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP);
            if (parentJoin != null && !parentJoin.getGroups().containsAll(GroupsUsedByElementsVisitor.getGroups(references.getValues()))) {
                return root;
            }
        }

        PlanNode parentProject = NodeEditor.findParent(frame, NodeConstants.Types.PROJECT);

        // Check whether the upper frame is a SELECT INTO
        if (parentProject.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
            return root;
        }

        if (!FrameUtil.canConvertAccessPatterns(frame)) {
            return root;
        }

        PlanNode projectNode = frame.getFirstChild();

        // Check if lower frame has only a stored procedure execution - this cannot be merged to parent frame
        if (FrameUtil.isProcedure(projectNode)) {
            return root;
        }

        SymbolMap symbolMap = (SymbolMap)frame.getProperty(NodeConstants.Info.SYMBOL_MAP);

        PlanNode sortNode = NodeEditor.findParent(parentProject, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);

        if (sortNode != null && sortNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
            OrderBy sortOrder = (OrderBy)sortNode.getProperty(NodeConstants.Info.SORT_ORDER);
            boolean unrelated = false;
            for (OrderByItem item : sortOrder.getOrderByItems()) {
                if (!item.isUnrelated()) {
                    continue;
                }
                Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(item.getSymbol(), true);
                for (ElementSymbol elementSymbol : elements) {
                    if (virtualGroup.equals(elementSymbol.getGroupSymbol())) {
                        unrelated = true;
                    }
                }
            }
            // the lower frame cannot contain DUP_REMOVE, GROUP, UNION if unrelated
            if (unrelated && NodeEditor.findNodePreOrder(frame, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.PROJECT) != null
                    || NodeEditor.findNodePreOrder(frame, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE) != null
                    || NodeEditor.findNodePreOrder(frame, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE) != null) {
                return root;
            }
        }

        PlanNode parentJoin = NodeEditor.findParent(frame, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP);

        //try to remove the virtual layer if we are only doing a simple projection in the following cases:
        // 1. if the frame root is something other than a project (SET_OP, SORT, LIMIT, etc.)
        // 2. if the frame has a grouping node
        // 3. if the frame has no sources
        if (projectNode.getType() != NodeConstants.Types.PROJECT
            || NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE
                                                                                             | NodeConstants.Types.JOIN) != null
            || sources.isEmpty()) {

            PlanNode parentSource = NodeEditor.findParent(parentProject, NodeConstants.Types.SOURCE);
            if (beforeDecomposeJoin && parentSource != null && parentSource.hasProperty(Info.PARTITION_INFO)
                    && !NodeEditor.findAllNodes(frame.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE).isEmpty()) {
                return root; //don't bother to merge until after
            }

            root = checkForSimpleProjection(frame, root, parentProject, metadata, capFinder);
            if (frame.getParent() == null || !sources.isEmpty() || projectNode.getType() != NodeConstants.Types.PROJECT || parentJoin == null) {
                return root; //only consider no sources when the frame is simple and there is a parent join
            }
            if (sources.isEmpty()) {
                JoinType jt = (JoinType) parentJoin.getProperty(Info.JOIN_TYPE);
                if (jt.isOuter()) {
                    return root; //cannot remove if the no source side is an outer side, or if it can change the meaning of the plan
                }
                PlanNode joinToTest = parentJoin;
                while (joinToTest != null) {
                    if (FrameUtil.findJoinSourceNode(joinToTest.getFirstChild()).getGroups().contains(virtualGroup)) {
                        //scan all sources under the other side as there could be a join structure
                        for (PlanNode node : NodeEditor.findAllNodes(joinToTest.getLastChild(), NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE)) {
                            SymbolMap map = (SymbolMap)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
                            if (map != null && GroupsUsedByElementsVisitor.getGroups(map.getValues()).contains(virtualGroup)) {
                                //TODO: we don't have the logic yet to then replace the correlated references
                                return root;
                            }
                        }
                    }
                    joinToTest = NodeEditor.findParent(joinToTest, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP);
                }
            }
        }

        if (!checkJoinCriteria(frame.getFirstChild(), virtualGroup, parentJoin, metadata)) {
            return root;
        }

        //we don't have to check for null dependent with no source without criteria since there must be a row
        if (!checkProjectedSymbols(projectNode, virtualGroup, parentJoin, metadata, sources, !sources.isEmpty() || frame.getParent() != parentJoin, parentProject)) {
            //TODO: propagate constants if just inhibited by subquery/non-deterministic expressions
            return root;
        }

        // Otherwise merge should work

        // Convert parent frame before merge
        Set<GroupSymbol> groups = Collections.emptySet();
        if (!sources.isEmpty()) {
            groups = FrameUtil.findJoinSourceNode(projectNode).getGroups();
        } else if (references != null) {
            //convert from correlated form to regular references
            RulePlanSubqueries.ReferenceReplacementVisitor rrv = new RulePlanSubqueries.ReferenceReplacementVisitor(references);
            for (Map.Entry<ElementSymbol, Expression> entry : symbolMap.asUpdatableMap().entrySet()) {
                if (entry.getValue() instanceof Reference) {
                    Expression ex = rrv.replaceExpression(entry.getValue());
                    entry.setValue(ex);
                } else {
                    PreOrPostOrderNavigator.doVisit(entry.getValue(), rrv, PreOrPostOrderNavigator.PRE_ORDER);
                }
            }
        }
        FrameUtil.convertFrame(frame, virtualGroup, groups, symbolMap.asMap(), metadata);

        PlanNode parentBottom = frame.getParent();
        prepareFrame(frame);

        if (projectNode.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
            parentProject.setProperty(Info.HAS_WINDOW_FUNCTIONS, true);
        }

        if (sources.isEmpty() && parentJoin != null) {
            //special handling for no sources
            PlanNode parent = frame;
            List<PlanNode> criteriaNodes = new ArrayList<PlanNode>();
            while (parent.getParent() != parentJoin) {
                parent = parent.getParent();
                if (!parent.hasBooleanProperty(Info.IS_PHANTOM)) {
                    criteriaNodes.add(parent);
                }
            }
            PlanNode parentNode = parentJoin.getParent();
            parentJoin.removeChild(parent);
            PlanNode other = parentJoin.getFirstChild();
            NodeEditor.removeChildNode(parentNode, parentJoin);
            JoinType jt = (JoinType) parentJoin.getProperty(Info.JOIN_TYPE);
            if (!jt.isOuter()) {
                //if we are not an outer join then the join/parent criteria is effectively
                //applied to the other side
                List<Criteria> joinCriteria = (List<Criteria>) parentJoin.getProperty(Info.JOIN_CRITERIA);
                if (joinCriteria != null) {
                    for (Criteria crit : joinCriteria) {
                        PlanNode critNode = RelationalPlanner.createSelectNode(crit, false);
                        criteriaNodes.add(critNode);
                    }
                }
                if (!criteriaNodes.isEmpty()) {
                    for (PlanNode selectNode : criteriaNodes) {
                        selectNode.removeAllChildren();
                        selectNode.removeFromParent();
                        other.addAsParent(selectNode);
                    }
                }
            }
        } else {
            // Remove top 2 nodes (SOURCE, PROJECT) of virtual group - they're no longer needed
            NodeEditor.removeChildNode(parentBottom, frame);
            NodeEditor.removeChildNode(parentBottom, projectNode);
        }

        return root;
    }

    private static void prepareFrame(PlanNode frame) {
        // find the new root of the frame so that access patterns can be propagated
        PlanNode newRoot = FrameUtil.findJoinSourceNode(frame.getFirstChild());
        if (newRoot != null) {
            Collection<AccessPattern> ap = (Collection)frame.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
            if (ap != null) {
                Collection<AccessPattern> newAp = (Collection)newRoot.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
                if (newAp == null) {
                    newRoot.setProperty(NodeConstants.Info.ACCESS_PATTERNS, ap);
                } else {
                    newAp.addAll(ap);
                }
            }
            RulePlaceAccess.copyProperties(frame, newRoot);
        }
    }

    /**
     * Removes source layers that only do a simple projection of the elements below.
     * @param capFinder
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    private static PlanNode checkForSimpleProjection(PlanNode frame,
                                                     PlanNode root,
                                                     PlanNode parentProject,
                                                     QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        // check that the parent only performs projection
        PlanNode nodeToCheck = parentProject.getFirstChild();
        while (nodeToCheck != frame) {
            if (nodeToCheck.getType() != NodeConstants.Types.SELECT
                || !nodeToCheck.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                return root;
            }
            nodeToCheck = nodeToCheck.getFirstChild();
        }

        if (frame.getFirstChild().getType() == NodeConstants.Types.TUPLE_LIMIT
            && NodeEditor.findParent(parentProject,
                                     NodeConstants.Types.SORT | NodeConstants.Types.DUP_REMOVE,
                                     NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP) != null) {
            return root;
        }

        List<? extends Expression> requiredElements = RuleAssignOutputElements.determineSourceOutput(frame, new ArrayList<Expression>(), metadata, null);
        List<Expression> selectSymbols = (List<Expression>)parentProject.getProperty(NodeConstants.Info.PROJECT_COLS);

        // check that it only performs simple projection and that all required symbols are projected
        LinkedHashSet<Expression> symbols = new LinkedHashSet<Expression>(); //ensuring there are no duplicates prevents problems with subqueries
        for (Expression symbol : selectSymbols) {
            Expression expr = SymbolMap.getExpression(symbol);
            if (expr instanceof Constant) {
                if (!symbols.add(new ExpressionSymbol("const" + symbols.size(), expr))) { //$NON-NLS-1$
                    return root;
                }
                continue;
            }
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

        PlanNode sort = NodeEditor.findParent(parentProject, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
        if (sort != null && sort.hasBooleanProperty(Info.UNRELATED_SORT)) {
            return root;
        }

        // re-order the lower projects
        RuleAssignOutputElements.filterVirtualElements(frame, new ArrayList<Expression>(symbols), metadata);

        // remove phantom select nodes
        nodeToCheck = parentProject.getFirstChild();
        while (nodeToCheck != frame) {
            PlanNode current = nodeToCheck;
            nodeToCheck = nodeToCheck.getFirstChild();
            NodeEditor.removeChildNode(current.getParent(), current);
        }

        if (NodeEditor.findParent(parentProject, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.SOURCE) != null) {
            PlanNode lowerDup = NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.PROJECT);
            if (lowerDup != null) {
                NodeEditor.removeChildNode(lowerDup.getParent(), lowerDup);
            }

            PlanNode setOp = NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
            if (setOp != null) {
                setOp.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
                distributeDupRemove(metadata, capFinder, setOp);
                if (parentProject.getParent().getParent() != null) {
                    NodeEditor.removeChildNode(parentProject.getParent().getParent(), parentProject.getParent());
                } else {
                    parentProject.removeFromParent();
                    root = parentProject;
                }
            }
        }

        correctOrderBy(frame, sort, selectSymbols, parentProject);

        PlanNode parentSource = NodeEditor.findParent(frame, NodeConstants.Types.SOURCE);

        if (parentSource != null && NodeEditor.findNodePreOrder(parentSource, NodeConstants.Types.PROJECT) == parentProject) {
            FrameUtil.correctSymbolMap(((SymbolMap)frame.getProperty(NodeConstants.Info.SYMBOL_MAP)).asMap(), parentSource);
        }

        prepareFrame(frame);
        //remove the parent project and the source node
        NodeEditor.removeChildNode(parentProject, frame);
        if (parentProject.getParent() == null) {
            root = parentProject.getFirstChild();
            parentProject.removeChild(root);
            return root;
        }
        NodeEditor.removeChildNode(parentProject.getParent(), parentProject);

        return root;
    }
    /**
     * special handling is needed since we are retaining the child aliases
     */
    private static void correctOrderBy(PlanNode frame, PlanNode sort,
            List<Expression> selectSymbols, PlanNode parentProject) {
        if (sort == null || NodeEditor.findNodePreOrder(sort, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE) != parentProject) {
            return;
        }
        List<Expression> childProject = (List<Expression>)NodeEditor.findNodePreOrder(frame, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS);
        OrderBy elements = (OrderBy)sort.getProperty(NodeConstants.Info.SORT_ORDER);
        for (OrderByItem item : elements.getOrderByItems()) {
            item.setSymbol(childProject.get(selectSymbols.indexOf(item.getSymbol())));
        }
        sort.getGroups().clear();
        sort.addGroups(GroupsUsedByElementsVisitor.getGroups(elements));
    }

    /**
     * Check to ensure that we are not projecting a subquery or null dependent expressions
     */
    private static boolean checkProjectedSymbols(PlanNode projectNode,
                                                 GroupSymbol virtualGroup,
                                                 PlanNode parentJoin,
                                                 QueryMetadataInterface metadata, List<PlanNode> sources, boolean checkForNullDependent, PlanNode parentProject) {
        if (projectNode.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)
                && (parentProject.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)
                        || NodeEditor.findParent(projectNode, NodeConstants.Types.PROJECT,
                                NodeConstants.Types.SORT | NodeConstants.Types.GROUP | NodeConstants.Types.SELECT | NodeConstants.Types.JOIN) == null)) {
            //if there something above using the window function symbols, then we can move the projection
            return false;
        }

        List<Expression> selectSymbols = (List<Expression>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

        HashSet<GroupSymbol> groups = new HashSet<GroupSymbol>();
        for (PlanNode sourceNode : sources) {
            groups.addAll(sourceNode.getGroups());
        }

        return checkProjectedSymbols(virtualGroup, parentJoin, metadata,
                selectSymbols, groups, checkForNullDependent);
    }

    static boolean checkProjectedSymbols(GroupSymbol virtualGroup,
            PlanNode parentJoin, QueryMetadataInterface metadata,
            List<? extends Expression> selectSymbols, Set<GroupSymbol> groups, boolean checkForNullDependent) {
        if (checkForNullDependent) {
            checkForNullDependent = false;
            // check to see if there are projected literal on the inner side of an outer join that needs to be preserved
            if (parentJoin != null) {
                PlanNode joinToTest = parentJoin;
                while (joinToTest != null) {
                    JoinType joinType = (JoinType)joinToTest.getProperty(NodeConstants.Info.JOIN_TYPE);
                    if (joinType == JoinType.JOIN_FULL_OUTER) {
                        checkForNullDependent = true;
                        break;
                    } else if (joinType == JoinType.JOIN_LEFT_OUTER
                               && FrameUtil.findJoinSourceNode(joinToTest.getLastChild()).getGroups().contains(virtualGroup)) {
                        checkForNullDependent = true;
                        break;
                    }
                    joinToTest = NodeEditor.findParent(joinToTest.getParent(), NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
                }
            }
        }

        for (int i = 0; i < selectSymbols.size(); i++) {
            Expression symbol = selectSymbols.get(i);
            Collection scalarSubqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(symbol);
            if (!scalarSubqueries.isEmpty()) {
                return false;
            }
            if (checkForNullDependent && JoinUtil.isNullDependent(metadata, groups, SymbolMap.getExpression(symbol))) {
                return false;
            }
            // TEIID-16: We do not want to merge a non-deterministic scalar function
            if (FunctionCollectorVisitor.isNonDeterministic(symbol)) {
                return false;
            }
        }

        return true;
    }

    /**
     * check to see if criteria is used in a full outer join or has no groups and is on the inner side of an outer join. if this
     * is the case then the layers cannot be merged, since merging would possibly force the criteria to change it's position (into
     * the on clause or above the join).
     * @param metadata
     */
    static boolean checkJoinCriteria(PlanNode frameRoot,
                                             GroupSymbol virtualGroup,
                                             PlanNode parentJoin, QueryMetadataInterface metadata) {
        List<PlanNode> selectNodes = null;
        Set<GroupSymbol> groups = null;

        while (parentJoin != null) {
            if (selectNodes == null) {
                selectNodes = NodeEditor.findAllNodes(frameRoot,
                        NodeConstants.Types.SELECT,
                        NodeConstants.Types.SOURCE);
                groups = Collections.singleton(virtualGroup);
            }
            for (PlanNode selectNode : selectNodes) {
                if (selectNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                    continue;
                }
                JoinType jt = JoinUtil.getJoinTypePreventingCriteriaOptimization(parentJoin, groups);

                if (jt != null && (jt == JoinType.JOIN_FULL_OUTER
                        || selectNode.getGroups().size() == 0
                        || JoinUtil.isNullDependent(metadata, selectNode.getGroups(), (Criteria)selectNode.getProperty(Info.SELECT_CRITERIA)))) {
                    return false;
                }
            }
            //check against all joins in the frame
            parentJoin = NodeEditor.findParent(parentJoin, NodeConstants.Types.JOIN);
        }
        return true;
    }

    static void distributeDupRemove(QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, PlanNode unionNode)
            throws QueryMetadataException, TeiidComponentException {
        PlanNode unionParentSource = NodeEditor.findParent(unionNode, NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP);
        if (unionNode.hasBooleanProperty(Info.USE_ALL)
                || unionParentSource == null
                || unionParentSource.getType() != NodeConstants.Types.SOURCE
                || !unionParentSource.hasProperty(Info.PARTITION_INFO)) {
            return;
        }

        PlanNode accessNode = NodeEditor.findParent(unionNode, NodeConstants.Types.ACCESS);
        if (accessNode != null) {
            Object mid = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
            if (!CapabilitiesUtil.supports(Capability.QUERY_SELECT_DISTINCT, mid, metadata, capabilitiesFinder)) {
                return;
            }
        }

        //distribute dup remove
        LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
        RulePushAggregates.findUnionChildren(unionChildren, false, unionNode);
        unionNode.setProperty(Info.USE_ALL, true);
        for (PlanNode node : unionChildren) {
            if (node.getType() == NodeConstants.Types.SET_OP) {
                node.setProperty(Info.USE_ALL, false);
            } else {
                PlanNode projectNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.DUP_REMOVE | NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
                if (projectNode != null && projectNode.getType() == NodeConstants.Types.PROJECT) {
                    accessNode = NodeEditor.findParent(projectNode, NodeConstants.Types.ACCESS);
                    PlanNode dup = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
                    if (accessNode == null) {
                        projectNode.addAsParent(dup);
                    } else {
                        Object mid = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
                        if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_DISTINCT, mid, metadata, capabilitiesFinder)) {
                            projectNode.addAsParent(dup);
                        } else {
                            accessNode.addAsParent(dup);
                        }
                    }
                }
            }
        }
    }

    public String toString() {
        return "MergeVirtual"; //$NON-NLS-1$
    }

}
