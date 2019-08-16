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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.util.CommandContext;

/**
 * Perform the optimization:<pre>
 *                  source
 * inner join         union all
 *   source             inner join
 *     union all  =&gt;      source
 *       a                  a
 *       b                source
 *   source                 c
 *     union all        inner join
 *       c                source
 *       d                  b
 *                        source
 *                          d
 * </pre>
 */
public class RuleDecomposeJoin implements OptimizerRule {

    public final static String IMPLICIT_PARTITION_COLUMN_NAME = "implicit_partition.columnName";  //$NON-NLS-1$

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, RuleStack rules,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {

        for (PlanNode joinNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
            plan = decomposeJoin(joinNode, plan, metadata, context);
        }

        return plan;
    }

    public PlanNode decomposeJoin(PlanNode joinNode, PlanNode root, QueryMetadataInterface metadata, CommandContext context) throws TeiidComponentException, QueryPlannerException {
        if (joinNode.getParent() == null) {
            return root; //already processed
        }

        JoinType joinType = (JoinType)joinNode.getProperty(Info.JOIN_TYPE);
        if (joinType == JoinType.JOIN_ANTI_SEMI || joinType == JoinType.JOIN_CROSS) {
            return root;
        }

        PlanNode left = joinNode.getFirstChild();
        while (left.getType() != NodeConstants.Types.SOURCE) {
            if (left.getType() == NodeConstants.Types.SELECT && left.hasBooleanProperty(Info.IS_PHANTOM)) {
                left = left.getFirstChild();
            } else {
                return root;
            }
        }

        Map<ElementSymbol, List<Set<Constant>>> partitionInfo = (Map<ElementSymbol, List<Set<Constant>>>)left.getProperty(Info.PARTITION_INFO);

        if (partitionInfo == null) {
            return root;
        }

        PlanNode unionNode = left.getFirstChild();
        if (unionNode.getType() != NodeConstants.Types.SET_OP) {
            return root;
        }

        PlanNode right = joinNode.getLastChild();

        while (right.getType() != NodeConstants.Types.SOURCE) {
            if (right.getType() == NodeConstants.Types.SELECT && right.hasBooleanProperty(Info.IS_PHANTOM)) {
                right = right.getFirstChild();
            } else {
                return root;
            }
        }

        Map<ElementSymbol, List<Set<Constant>>> rightPartionInfo = (Map<ElementSymbol, List<Set<Constant>>>)right.getProperty(Info.PARTITION_INFO);

        if (rightPartionInfo == null) {
            return root;
        }

        List<Criteria> criteria = (List<Criteria>)joinNode.getProperty(Info.JOIN_CRITERIA);

        List<Expression> expr = new ArrayList<Expression>();
        List<Expression> exprOther = new ArrayList<Expression>();
        RuleChooseJoinStrategy.separateCriteria(unionNode.getParent().getGroups(), right.getGroups(), expr, exprOther, criteria, new LinkedList<Criteria>());

        //if implicit, we assume that partitions match
        ElementSymbol es = getImplicitPartitionColumn(metadata, left);
        ElementSymbol esOther = getImplicitPartitionColumn(metadata, right);
        if (es != null && esOther != null
                && getEffectiveModelId(metadata, es.getGroupSymbol()) == getEffectiveModelId(metadata, esOther.getGroupSymbol())) {
            expr.add(es);
            exprOther.add(esOther);
        }

        if (expr.isEmpty()) {
            return root; //no equi-join
        }

        List<int[]> matches = findMatches(partitionInfo, rightPartionInfo, expr, exprOther);

        if (matches == null) {
            return root; //no non-overlapping partitions
        }

        int branchSize = partitionInfo.values().iterator().next().size();
        int otherBranchSize = rightPartionInfo.values().iterator().next().size();

        if (matches.isEmpty()) {
            if (joinType == JoinType.JOIN_INNER || joinType == JoinType.JOIN_SEMI) {
                //no matches mean that we can just insert a null node (false criteria) and be done with it
                PlanNode critNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
                critNode.setProperty(Info.SELECT_CRITERIA, QueryRewriter.FALSE_CRITERIA);
                unionNode.addAsParent(critNode);
            } else if (joinType == JoinType.JOIN_LEFT_OUTER) {
                joinNode.getParent().replaceChild(joinNode, left);
            } else if (joinType == JoinType.JOIN_FULL_OUTER) {
                joinNode.setProperty(Info.JOIN_CRITERIA, QueryRewriter.FALSE_CRITERIA);
            }
            return root;
        }

        List<PlanNode> branches = new ArrayList<PlanNode>();
        //TODO: find union children from RulePushAggregates
        RulePushSelectCriteria.collectUnionChildren(unionNode, branches);

        if (branches.size() != branchSize) {
            return root; //sanity check
        }

        List<PlanNode> otherBranches = new ArrayList<PlanNode>();
        RulePushSelectCriteria.collectUnionChildren(right.getFirstChild(), otherBranches);

        if (otherBranches.size() != otherBranchSize) {
            return root; //sanity check
        }

        PlanNode newUnion = buildUnion(unionNode, right, criteria, matches, branches, otherBranches, joinType);
        GroupSymbol leftGroup = left.getGroups().iterator().next();
        PlanNode view = rebuild(leftGroup, joinNode, newUnion, metadata, context, left, right);

        //preserve the model of the virtual group as we'll look for this when checking for implicit behavior
        ((TempMetadataID)(view.getGroups().iterator().next().getMetadataID())).getTableData().setModel(getEffectiveModelId(metadata, leftGroup));

        SymbolMap symbolmap = (SymbolMap)view.getProperty(Info.SYMBOL_MAP);
        HashMap<ElementSymbol, List<Set<Constant>>> newPartitionInfo = new LinkedHashMap<ElementSymbol, List<Set<Constant>>>();
        Map<Expression, ElementSymbol> inverse = symbolmap.inserseMapping();
        for (int[] match : matches) {
            updatePartitionInfo(partitionInfo, matches, inverse, newPartitionInfo, match[0]);
            updatePartitionInfo(rightPartionInfo, matches, inverse, newPartitionInfo, match[1]);
        }
        view.setProperty(Info.PARTITION_INFO, newPartitionInfo);

        //since we've created a new union node, there's a chance we can decompose again
        if (view.getParent().getType() == NodeConstants.Types.JOIN) {
            return decomposeJoin(view.getParent(), root, metadata, context);
        }
        return root;
    }

    static Object getEffectiveModelId(QueryMetadataInterface metadata,
            GroupSymbol gs)
            throws TeiidComponentException, QueryMetadataException {
        if (gs.getModelMetadataId() != null) {
            return gs.getModelMetadataId();
        }
        return metadata.getModelID(gs.getMetadataID());
    }

    private ElementSymbol getImplicitPartitionColumn(QueryMetadataInterface metadata,
            PlanNode node)
            throws TeiidComponentException, QueryMetadataException {
        GroupSymbol gs = node.getGroups().iterator().next();
        Object modelId = getEffectiveModelId(metadata, gs);
        String name = metadata.getExtensionProperty(modelId, IMPLICIT_PARTITION_COLUMN_NAME, true);
        if (name != null) {
            return new ElementSymbol(name, gs);
        }
        return null;
    }

    private void updatePartitionInfo(
            Map<ElementSymbol, List<Set<Constant>>> partitionInfo,
            List<int[]> matches, Map<Expression, ElementSymbol> inverse,
            HashMap<ElementSymbol, List<Set<Constant>>> newPartitionInfo, int index) {
        for (Map.Entry<ElementSymbol, List<Set<Constant>>> entry : partitionInfo.entrySet()) {
            ElementSymbol newSymbol = inverse.get(entry.getKey());
            List<Set<Constant>> values = newPartitionInfo.get(newSymbol);
            if (values == null) {
                values = new ArrayList<Set<Constant>>(matches.size());
                newPartitionInfo.put(newSymbol, values);
            }
            values.add(entry.getValue().get(index));
        }
    }

    /**
     * Add the new union back in under a view
     */
    static PlanNode rebuild(GroupSymbol group, PlanNode toReplace, PlanNode newUnion, QueryMetadataInterface metadata, CommandContext context,
            PlanNode... toMap)
            throws TeiidComponentException, QueryPlannerException,
            QueryMetadataException {
        Set<String> groups = context.getGroups();

        group = RulePlaceAccess.recontextSymbol(group, groups);

        PlanNode projectNode = NodeEditor.findNodePreOrder(newUnion, NodeConstants.Types.PROJECT);
        List<? extends Expression> projectedSymbols = (List<? extends Expression>)projectNode.getProperty(Info.PROJECT_COLS);

        SymbolMap newSymbolMap = RulePushAggregates.createSymbolMap(group, projectedSymbols, newUnion, metadata);
        PlanNode view = RuleDecomposeJoin.createSource(group, newUnion, newSymbolMap);

        Map<Expression, ElementSymbol> inverseMap = newSymbolMap.inserseMapping();
        if (toReplace != null) {
            toReplace.getParent().replaceChild(toReplace, view);
        }
        Set<GroupSymbol> newGroups = Collections.singleton(group);
        for (PlanNode node : toMap) {
            FrameUtil.convertFrame(view, node.getGroups().iterator().next(), newGroups, inverseMap, metadata);
        }

        return view;
    }

    /**
     * Search each equi-join for partitioning
     */
    private List<int[]> findMatches(
            Map<ElementSymbol, List<Set<Constant>>> partitionInfo,
            Map<ElementSymbol, List<Set<Constant>>> partitionInfoOther,
            List<Expression> expr, List<Expression> exprOther) {
        List<int[]> matches = null;
        for (int i = 0; i < expr.size() && matches == null; i++) {
            if (!(expr.get(i) instanceof ElementSymbol) || !(exprOther.get(i) instanceof ElementSymbol)) {
                continue;
            }
            ElementSymbol es = (ElementSymbol)expr.get(i);
            ElementSymbol esOther = (ElementSymbol)exprOther.get(i);
            List<Set<Constant>> partLists = partitionInfo.get(es);
            List<Set<Constant>> partListsOther = partitionInfoOther.get(esOther);
            if (partLists == null || partListsOther == null) {
                continue;
            }
            matches = findMatches(partLists, partListsOther);
        }
        return matches;
    }

    /**
     * Find overlaps in the given partition lists
     */
    private List<int[]> findMatches(List<Set<Constant>> partLists,
            List<Set<Constant>> partListsOther) {
        List<int[]> matches = new LinkedList<int[]>();
        for (int j = 0; j < partLists.size(); j++) {
            int[] match = null;
            Set<Constant> vals = partLists.get(j);
            for (int k = 0; k < partListsOther.size(); k++) {
                if (!Collections.disjoint(vals, partListsOther.get(k))) {
                    if (match == null) {
                        match = new int[] {j, k};
                    } else {
                        //TODO: we currently do handle a situation where multiple
                        //partitions overlap.
                        return null;
                    }
                }
            }
            if (match != null) {
                matches.add(match);
            }
        }
        return matches;
    }

    private PlanNode buildUnion(PlanNode unionNode, PlanNode otherSide,
            List<Criteria> criteria, List<int[]> matches,
            List<PlanNode> branches, List<PlanNode> otherBranches, JoinType joinType) {
        SymbolMap symbolMap = (SymbolMap)unionNode.getParent().getProperty(Info.SYMBOL_MAP);
        SymbolMap otherSymbolMap = (SymbolMap)otherSide.getProperty(Info.SYMBOL_MAP);

        List<PlanNode> joins = new LinkedList<PlanNode>();
        for (int i = 0; i < matches.size(); i++) {
            int[] is = matches.get(i);
            PlanNode branch = branches.get(is[0]);
            PlanNode branchSource = createSource(unionNode.getParent().getGroups().iterator().next(), branch, symbolMap);

            PlanNode otherBranch = otherBranches.get(is[1]);
            PlanNode otherBranchSource = createSource(otherSide.getGroups().iterator().next(), otherBranch, otherSymbolMap);

            PlanNode newJoinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            newJoinNode.addLastChild(branchSource);
            newJoinNode.addLastChild(otherBranchSource);

            newJoinNode.setProperty(Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
            newJoinNode.setProperty(Info.JOIN_TYPE, joinType);
            newJoinNode.setProperty(Info.JOIN_CRITERIA, LanguageObject.Util.deepClone(criteria, Criteria.class));
            newJoinNode.addGroups(branchSource.getGroups());
            newJoinNode.addGroups(otherBranchSource.getGroups());

            PlanNode projectPlanNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
            newJoinNode.addAsParent(projectPlanNode);

            Select allSymbols = new Select(symbolMap.getKeys());
            allSymbols.addSymbols(otherSymbolMap.getKeys());
            if (i == 0) {
                QueryRewriter.makeSelectUnique(allSymbols, false);
            }
            projectPlanNode.setProperty(NodeConstants.Info.PROJECT_COLS, allSymbols.getSymbols());
            projectPlanNode.addGroups(newJoinNode.getGroups());

            joins.add(projectPlanNode);
        }

        PlanNode newUnion = RulePlanUnions.buildUnionTree(unionNode, joins);
        return newUnion;
    }

    static PlanNode createSource(GroupSymbol group, PlanNode child, SymbolMap symbolMap) {
        return createSource(group, child, symbolMap.getKeys());
    }

    static PlanNode createSource(GroupSymbol group, PlanNode child, List<ElementSymbol> newProject) {
        PlanNode branchSource = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        branchSource.addGroup(group);
        PlanNode projectNode = NodeEditor.findNodePreOrder(child, NodeConstants.Types.PROJECT);
        branchSource.setProperty(Info.SYMBOL_MAP, SymbolMap.createSymbolMap(newProject, (List<? extends Expression>)projectNode.getProperty(Info.PROJECT_COLS)));
        child.addAsParent(branchSource);
        return branchSource;
    }

    @Override
    public String toString() {
        return "DecomposeJoin"; //$NON-NLS-1$
    }

}
