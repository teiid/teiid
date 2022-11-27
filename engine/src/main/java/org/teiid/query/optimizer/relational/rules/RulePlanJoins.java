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

import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
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
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Permutation;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;


/**
 *  Determines join orderings based upon dependency and cost information
 *
 *  The algorithm works as follows:
 *
 *  Stage 1.  Find join regions.  A join region is an set of inner and cross joins
 *  (with the join and intermediate criteria removed).
 *
 *  Dependency Phase
 *
 *  Stage 2.  Determine if dependencies found can be satisfied.
 *      a. Throw an exception if a quick check fails.
 *
 *  Stage 3.  A satisfying set of access patterns and join ordering will be found
 *  for each join region.
 *      a. If this is not possible, an exception will be thrown
 *      b. only one possible set of access patterns will be considered
 *
 *  Optimization Phase
 *
 *  Stage 4.  Heuristically push joins down. Join regions (with more than one join source) will be
 *  exhaustively searched (bottom up) for join pairs that can be pushed to a source.
 *      a. A join is eligible for pushing if the access node can be raised and
 *         there is at least one join criteria that can also be pushed.
 *         -- costing information is not considered at this point.
 *      b. Once a pair has been pushed, they will be replaced in the join region
 *         with a single access node.
 *
 *  Stage 5.  The remaining join regions will be ordered in a left linear tree based
 *  upon a an exhaustive, or random, algorithm that considers costing and criteria information.
 *
 */
public class RulePlanJoins implements OptimizerRule {

    public static final int EXHAUSTIVE_SEARCH_GROUPS = 7;

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



        List<JoinRegion> joinRegions = new LinkedList<JoinRegion>();

        findJoinRegions(plan, null, joinRegions);

        //dependency phase

        List<JoinRegion> leftOuterJoinRegions = new LinkedList<JoinRegion>();

        for (Iterator<JoinRegion> joinRegionIter = joinRegions.iterator(); joinRegionIter.hasNext();) {
            JoinRegion joinRegion = joinRegionIter.next();

            //skip regions that have nothing to plan
            if (joinRegion.getJoinSourceNodes().size() + joinRegion.getDependentJoinSourceNodes().size() < 2) {
                joinRegionIter.remove();
                if (joinRegion.getLeft() != null) {
                    leftOuterJoinRegions.add(joinRegion);
                }
                continue;
            }

            joinRegion.initializeJoinInformation();

            //account for nested table correlations
            for (PlanNode joinSource : joinRegion.getJoinSourceNodes().keySet()) {
                SymbolMap map = (SymbolMap)joinSource.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
                if (map !=null) {
                    joinSource.setProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS, GroupsUsedByElementsVisitor.getGroups(map.getValues()));
                    joinRegion.setContainsNestedTable(true);
                }
            }

            //check for unsatisfied dependencies
            if (joinRegion.getUnsatisfiedAccessPatterns().isEmpty()) {
                continue;
            }

            //quick check for satisfiability
            if (!joinRegion.isSatisfiable()) {
                 throw new QueryPlannerException(QueryPlugin.Event.TEIID30275, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30275, joinRegion.getUnsatisfiedAccessPatterns()));
            }

            planForDependencies(joinRegion);
        }

        //optimization phase
        for (JoinRegion joinRegion : joinRegions) {
            groupJoinsForPushing(metadata, capabilitiesFinder, joinRegion, context);
        }

        //check for optimizing across left outer joins
        for (JoinRegion joinRegion : leftOuterJoinRegions) {
            groupAcrossLeftOuter(metadata, capabilitiesFinder, context,
                    joinRegion);
        }

        for (Iterator<JoinRegion> joinRegionIter = joinRegions.iterator(); joinRegionIter.hasNext();) {
            JoinRegion joinRegion = joinRegionIter.next();

            //move the dependent nodes back into all joinSources
            joinRegion.getJoinSourceNodes().putAll(joinRegion.getDependentJoinSourceNodes());
            joinRegion.getCriteriaNodes().addAll(joinRegion.getDependentCriteriaNodes());
            joinRegion.getDependentJoinSourceNodes().clear();
            joinRegion.getDependentCriteriaNodes().clear();

            if (joinRegion.getJoinSourceNodes().size() < 2) {
                joinRegion.reconstructJoinRegoin();
                joinRegionIter.remove();
                continue;
            }

            joinRegion.initializeCostingInformation(metadata);

            Object[] bestOrder = findBestJoinOrder(joinRegion, metadata, capabilitiesFinder, context);

            //if no best order was found, just stick with how the user entered the query
            if (bestOrder == null) {
                continue;
            }

            joinRegion.changeJoinOrder(bestOrder);
            joinRegion.reconstructJoinRegoin();
        }

        return plan;
    }

    private void groupAcrossLeftOuter(QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, CommandContext context,
            JoinRegion joinRegion) throws QueryMetadataException,
            TeiidComponentException, AssertionError {
        if (joinRegion.getLeft() == null || joinRegion.getJoinRoot().getLastChild().getType() != NodeConstants.Types.ACCESS
                || joinRegion.getJoinRoot().getFirstChild().getType() == NodeConstants.Types.ACCESS) {
            return;
        }

        PlanNode planNodeRight = joinRegion.getJoinRoot().getLastChild();

        Object modelId = RuleRaiseAccess.getModelIDFromAccess(planNodeRight, metadata);

        Map<Object, List<PlanNode>> accessMapLeft = getAccessMap(metadata, capabilitiesFinder, joinRegion.getLeft());

        //TODO: what about same connector, but not the same model
        List<PlanNode> joinSourcesLeft = accessMapLeft.get(modelId);
        if (joinSourcesLeft == null) {
            return;
        }

        SupportedJoinCriteria sjc = CapabilitiesUtil.getSupportedJoinCriteria(modelId, metadata, capabilitiesFinder);
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        List<Criteria> joinCriteria = (List<Criteria>)joinRegion.getJoinRoot().getProperty(Info.JOIN_CRITERIA);
        for (Criteria crit : joinCriteria) {
            if (!RuleRaiseAccess.isSupportedJoinCriteria(sjc, crit, modelId, metadata, capabilitiesFinder, null)) {
                return;
            }
        }
        GroupsUsedByElementsVisitor.getGroups(joinCriteria, groups);
        groups.removeAll(planNodeRight.getGroups());
        for (PlanNode planNode : joinSourcesLeft) {
            if (!planNode.getGroups().containsAll(groups)) {
                continue;
            }

            //see if we can group the planNode with the other side
            if (RuleRaiseAccess.canRaiseOverJoin(Arrays.asList(planNode, planNodeRight), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, null, context, false, false) == null) {
                continue;
            }

            //remove the parent loj, create a new loj
            joinRegion.getLeft().getJoinSourceNodes().remove(planNode);

            PlanNode joinNode = createJoinNode(planNode, planNodeRight, joinCriteria, JoinType.JOIN_LEFT_OUTER);
            PlanNode newAccess = RuleRaiseAccess.raiseAccessOverJoin(joinNode, joinNode.getFirstChild(), modelId, capabilitiesFinder, metadata, false);

            for (Set<PlanNode> source : joinRegion.getLeft().getCritieriaToSourceMap().values()) {
                if (source.remove(planNode)) {
                    source.add(newAccess);
                }
            }

            joinRegion.getLeft().getJoinSourceNodes().put(newAccess, newAccess);

            PlanNode root = joinRegion.getJoinRoot();

            root.getParent().replaceChild(root, root.getFirstChild());

            joinRegion.getLeft().reconstructJoinRegoin();
            break;
        }
    }

    /**
     * This is a heuristic that checks for joins that may be pushed so they can be removed
     * before considering the joins that must be evaluated in MetaMatrix.
     *
     * By running this, we eliminate the need for running RuleRaiseAccess during join ordering
     *
     * @param metadata
     * @param joinRegion
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @throws QueryPlannerException
     */
    private void groupJoinsForPushing(QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
                                      JoinRegion joinRegion, CommandContext context) throws QueryMetadataException,
                                                            TeiidComponentException, QueryPlannerException {
        //TODO: consider moving select criteria if it is preventing a join from being pushed down
        //TODO: make the criteria checks based upon a guess at selectivity

        Map accessMap = getAccessMap(metadata, capFinder, joinRegion);

        boolean structureChanged = false;

        //search for combinations of join sources that should be pushed down
        for (Iterator accessNodeIter = accessMap.entrySet().iterator(); accessNodeIter.hasNext();) {
            Map.Entry entry = (Map.Entry)accessNodeIter.next();

            List<PlanNode> accessNodes = (List)entry.getValue();

            if (accessNodes.size() < 2) {
                continue;
            }

            int secondPass = -1;

            for (int i = accessNodes.size() - 1; i >= 0; i--) {

                PlanNode accessNode1 = accessNodes.get(i);
                Object modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode1, metadata);
                SupportedJoinCriteria sjc = CapabilitiesUtil.getSupportedJoinCriteria(modelId, metadata, capFinder);

                int discoveredJoin = -1;

                for (int k = (secondPass==-1?accessNodes.size() - 1:secondPass); k >= 0; k--) {
                    if (k == i) {
                        continue;
                    }

                    PlanNode accessNode2 = accessNodes.get(k);

                    List<PlanNode> criteriaNodes = joinRegion.getCriteriaNodes();

                    List<PlanNode> joinCriteriaNodes = new LinkedList<PlanNode>();

                    /* hasJoinCriteria will be true if
                     *  1. there is criteria between accessNode1 and accessNode2 exclusively
                     *  2. there is criteria between some other source (not the same logical connector) and accessNode1 or accessNode2
                     *
                     *  Ideally we should be a little smarter in case 2
                     *    - pushing down a same source cross join can be done if we know that a dependent join will be performed
                     */
                    boolean hasJoinCriteria = false;
                    LinkedList<Criteria> joinCriteria = new LinkedList<Criteria>();
                    for (PlanNode critNode : criteriaNodes) {
                        Set<PlanNode> sources = joinRegion.getCritieriaToSourceMap().get(critNode);

                        if (sources == null) {
                            continue;
                        }

                        if (sources.contains(accessNode1)) {
                            if (sources.contains(accessNode2) && sources.size() == 2) {
                                Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                                if (RuleRaiseAccess.isSupportedJoinCriteria(sjc, crit, modelId, metadata, capFinder, null)) {
                                    joinCriteriaNodes.add(critNode);
                                    joinCriteria.add(crit);
                                }
                            } else if (!accessNodes.containsAll(sources)) {
                                hasJoinCriteria = true;
                            }
                        } else if (sources.contains(accessNode2) && !accessNodes.containsAll(sources)) {
                           hasJoinCriteria = true;
                        }
                    }

                    /*
                     * If we failed to find direct criteria, a cross join may still be acceptable
                     */
                    if (joinCriteriaNodes.isEmpty() && (hasJoinCriteria || !canPushCrossJoin(metadata, accessNode1, accessNode2))) {
                        continue;
                    }

                    List<PlanNode> toTest = Arrays.asList(accessNode1, accessNode2);

                    JoinType joinType = joinCriteria.isEmpty()?JoinType.JOIN_CROSS:JoinType.JOIN_INNER;

                    /*
                     * We need to limit the heuristic grouping as we don't want to create larger source queries than necessary
                     */
                    boolean shouldPush = true;
                    int sourceCount = NodeEditor.findAllNodes(accessNode1, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE).size();
                    sourceCount += NodeEditor.findAllNodes(accessNode2, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE).size();

                    if (!context.getOptions().isAggressiveJoinGrouping() && accessMap.size() > 1 && joinType == JoinType.JOIN_INNER
                            && (sourceCount > 2 && (accessNode1.hasProperty(Info.MAKE_DEP) || accessNode2.hasProperty(Info.MAKE_DEP)) || sourceCount > 3)
                            && !canPushCrossJoin(metadata, accessNode1, accessNode2)) {
                        Collection<GroupSymbol> leftGroups = accessNode1.getGroups();
                        Collection<GroupSymbol> rightGroups = accessNode2.getGroups();

                        List<Expression> leftExpressions = new ArrayList<Expression>();
                        List<Expression> rightExpressions = new ArrayList<Expression>();
                        List<Criteria> nonEquiJoinCriteria = new ArrayList<Criteria>();

                        RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, joinCriteria, nonEquiJoinCriteria);

                        //allow a 1-1 join
                        if (!NewCalculateCostUtil.usesKey(accessNode1, leftExpressions, metadata)
                                || !NewCalculateCostUtil.usesKey(accessNode2, rightExpressions, metadata)) {
                            shouldPush = false; //don't push heuristically
                        }
                    }

                    //try to push to the source
                    if (!shouldPush || RuleRaiseAccess.canRaiseOverJoin(toTest, metadata, capFinder, joinCriteria, joinType, null, context, secondPass != -1, false) == null) {
                        if (secondPass == - 1 && sjc != SupportedJoinCriteria.KEY && discoveredJoin == -1) {
                            for (Criteria criteria : joinCriteria) {
                                if (criteria instanceof CompareCriteria && ((CompareCriteria) criteria).isOptional()) {
                                    discoveredJoin = k;
                                }
                            }
                        }
                        continue;
                    }

                    secondPass = -1;
                    discoveredJoin = -1;

                    structureChanged = true;

                    //remove the information that is no longer relevant to the join region
                    joinRegion.getCritieriaToSourceMap().keySet().removeAll(joinCriteriaNodes);
                    joinRegion.getCriteriaNodes().removeAll(joinCriteriaNodes);
                    joinRegion.getJoinSourceNodes().remove(accessNode1);
                    joinRegion.getJoinSourceNodes().remove(accessNode2);
                    accessNodes.remove(i);
                    accessNodes.remove(k < i ? k : k - 1);

                    //build a new join node
                    PlanNode joinNode = createJoinNode(accessNode2, accessNode1, joinCriteria, joinType);
                    PlanNode newAccess = RuleRaiseAccess.raiseAccessOverJoin(joinNode, joinNode.getFirstChild(), entry.getKey(), capFinder, metadata, false);
                    for (PlanNode critNode : joinCriteriaNodes) {
                        critNode.removeFromParent();
                        critNode.removeAllChildren();
                    }

                    //update with the new source

                    for (Set<PlanNode> source : joinRegion.getCritieriaToSourceMap().values()) {
                        if (source.remove(accessNode1) || source.remove(accessNode2)) {
                            source.add(newAccess);
                        }
                    }

                    joinRegion.getJoinSourceNodes().put(newAccess, newAccess);
                    accessNodes.add(newAccess);
                    i = accessNodes.size();
                    k = accessNodes.size();
                    break;
                }

                if (discoveredJoin != -1) {
                    i++; //rerun with the discoveredJoin criteria
                    secondPass = discoveredJoin;
                }
            }
        }

        if (structureChanged) {
            joinRegion.reconstructJoinRegoin();
        }
    }

    /**
     * create a join node with accessNode1 as the left child and accessNode2 as the right
     * @param accessNode1
     * @param accessNode2
     * @param joinCriteria
     * @param joinType
     * @return
     */
    private PlanNode createJoinNode(PlanNode accessNode1, PlanNode accessNode2,
            List<Criteria> joinCriteria, JoinType joinType) {
        PlanNode joinNode = createJoinNode();
        joinNode.getGroups().addAll(accessNode1.getGroups());
        joinNode.getGroups().addAll(accessNode2.getGroups());
        joinNode.addFirstChild(accessNode1);
        joinNode.addLastChild(accessNode2);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, joinType);
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
        return joinNode;
    }

    private boolean canPushCrossJoin(QueryMetadataInterface metadata,
            PlanNode accessNode1, PlanNode accessNode2)
            throws QueryMetadataException, TeiidComponentException {
        float cost1 = NewCalculateCostUtil.computeCostForTree(accessNode1, metadata);
        float cost2 = NewCalculateCostUtil.computeCostForTree(accessNode2, metadata);
        float acceptableCost = 64;
        return !((cost1 == NewCalculateCostUtil.UNKNOWN_VALUE || cost2 == NewCalculateCostUtil.UNKNOWN_VALUE || (cost1 > acceptableCost && cost2 > acceptableCost)));
    }

    /**
     * Return a map of Access Nodes to JoinSources that may be eligible for pushdown as
     * joins.
     */
    private Map<Object, List<PlanNode>> getAccessMap(QueryMetadataInterface metadata,
                             CapabilitiesFinder capFinder,
                             JoinRegion joinRegion) throws QueryMetadataException,
                                                   TeiidComponentException {
        Map<Object, List<PlanNode>> accessMap = new HashMap();

        for (PlanNode node : joinRegion.getJoinSourceNodes().values()) {
            /* check to see if we are directly over an access node.  in the event that the join source root
             * looks like select->access, we still won't consider this node for pushing
             */
            if (node.getType() != NodeConstants.Types.ACCESS) {
                continue;
            }
            Object accessModelID = RuleRaiseAccess.getModelIDFromAccess(node, metadata);

            if (accessModelID == null || !CapabilitiesUtil.supportsJoin(accessModelID, JoinType.JOIN_INNER, metadata, capFinder)) {
                continue;
            }

            RulePlanUnions.buildModelMap(metadata, capFinder, accessMap, node, accessModelID);
        }
        return accessMap;
    }

    /**
     * Greedily choose the first set of access patterns that can be satisfied
     * TODO: this is greedy.  the first access pattern that can be satisfied will be
     * TODO: order access patterns by number of dependent groups
     *
     * If we could flatten to a single set of dependencies, then a topological sort would be faster
     *
     * @param joinRegion
     * @throws QueryPlannerException
     */
    private void planForDependencies(JoinRegion joinRegion) throws QueryPlannerException {

        if (joinRegion.getJoinSourceNodes().isEmpty()) {
             throw new QueryPlannerException(QueryPlugin.Event.TEIID30275, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30275, joinRegion.getUnsatisfiedAccessPatterns()));
        }

        HashSet<GroupSymbol> currentGroups = new HashSet<GroupSymbol>();

        for (PlanNode joinSource : joinRegion.getJoinSourceNodes().keySet()) {
            currentGroups.addAll(joinSource.getGroups());
        }

        HashMap<PlanNode, PlanNode> dependentNodes = new HashMap<PlanNode, PlanNode>(joinRegion.getDependentJoinSourceNodes());

        boolean satisfiedAP = true;

        while (!dependentNodes.isEmpty() && satisfiedAP) {

            satisfiedAP = false;

            for (Iterator<Map.Entry<PlanNode, PlanNode>> joinSources = dependentNodes.entrySet().iterator(); joinSources.hasNext();) {
                Map.Entry<PlanNode, PlanNode> entry = joinSources.next();
                PlanNode joinSource = entry.getKey();

                Collection accessPatterns = (Collection)joinSource.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
                for (Iterator i = accessPatterns.iterator(); i.hasNext();) {
                    AccessPattern ap = (AccessPattern)i.next();

                    boolean foundGroups = true;
                    HashSet<GroupSymbol> allRequiredGroups = new HashSet<GroupSymbol>();
                    for (ElementSymbol symbol : ap.getUnsatisfied()) {
                        Set<Collection<GroupSymbol>> requiredGroupsSet = joinRegion.getDependentCriteriaElements().get(symbol);
                        boolean elementSatisfied = false;
                        if (requiredGroupsSet != null) {
                            for (Collection<GroupSymbol> requiredGroups : requiredGroupsSet) {
                                if (currentGroups.containsAll(requiredGroups)) {
                                    elementSatisfied = true;
                                    allRequiredGroups.addAll(requiredGroups);
                                    break;
                                }
                            }
                        }
                        if (!elementSatisfied) {
                            foundGroups = false;
                            break;
                        }
                    }

                    if (!foundGroups) {
                        continue;
                    }

                    joinSources.remove();
                    currentGroups.addAll(joinSource.getGroups());
                    satisfiedAP = true;
                    joinSource.setProperty(NodeConstants.Info.ACCESS_PATTERN_USED, ap.clone());
                    joinSource.setProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS, allRequiredGroups);
                    break;
                }
            }
        }

        if (!dependentNodes.isEmpty()) {
             throw new QueryPlannerException(QueryPlugin.Event.TEIID30275, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30275, joinRegion.getUnsatisfiedAccessPatterns()));
        }

    }

    static PlanNode createJoinNode() {
        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
        joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
        return joinNode;
    }

    /**
     * Finds all regions of inner and cross joins
     *
     * Join regions have boundaries at source nodes, outer joins, and unsatisfied dependencies
     *
     * @param root
     * @param currentRegion
     * @param joinRegions
     */
    static void findJoinRegions(PlanNode root, JoinRegion currentRegion, List<JoinRegion> joinRegions) {
        switch (root.getType()) {
            case NodeConstants.Types.JOIN:
            {
                if (currentRegion == null) {
                    currentRegion = new JoinRegion();
                    joinRegions.add(currentRegion);
                }
                JoinType jt = (JoinType)root.getProperty(NodeConstants.Info.JOIN_TYPE);

                boolean treatJoinAsSource = root.getProperty(NodeConstants.Info.ACCESS_PATTERNS) != null
                || root.hasProperty(NodeConstants.Info.MAKE_DEP) || root.hasProperty(NodeConstants.Info.MAKE_IND)
                || !root.getExportedCorrelatedReferences().isEmpty() || root.hasBooleanProperty(Info.PRESERVE);

                JoinRegion next = currentRegion;

                if (treatJoinAsSource || jt.isOuter()) {
                    next = null;
                    currentRegion.addJoinSourceNode(root);
                    //check if this a left outer join that we may optimize across
                    //TODO: look for more general left outer join associativity
                    if (!treatJoinAsSource && jt == JoinType.JOIN_LEFT_OUTER
                            && root.getFirstChild().getType() == NodeConstants.Types.JOIN
                            && root.getLastChild().getType() == NodeConstants.Types.ACCESS
                            && !((JoinType)root.getFirstChild().getProperty(Info.JOIN_TYPE)).isOuter()) {
                        next = new JoinRegion();
                        joinRegions.add(next);
                        findJoinRegions(root.getFirstChild(), next, joinRegions);
                        currentRegion.setLeft(next);
                        return;
                    }
                } else {
                    currentRegion.addParentCriteria(root);
                    currentRegion.addJoinCriteriaList((List)root.getProperty(NodeConstants.Info.JOIN_CRITERIA));
                }

                for (PlanNode child : root.getChildren()) {
                    findJoinRegions(child, next, joinRegions);
                }

                return;
            }
            case NodeConstants.Types.SOURCE:
            {
                if (currentRegion != null) {
                    currentRegion.addJoinSourceNode(root);
                }
                currentRegion = null;
                break;
            }
            case NodeConstants.Types.NULL:
            case NodeConstants.Types.ACCESS:
            {
                if (currentRegion != null) {
                    currentRegion.addJoinSourceNode(root);
                }
                return;
            }
        }
        if (root.getChildCount() == 0) {
            return;
        }
        for (PlanNode child : root.getChildren()) {
            findJoinRegions(child, root.getChildCount()==1?currentRegion:null, joinRegions);
        }
    }

    /**
     * The scoring algorithm is partially exhaustive and partially greedy.  For
     * regions up to the exhaustive search group size all possible left linear join
     * trees will be searched in O(n!) time.
     *
     * Beyond this number, every join will be determined greedily in O(n^2) time.
     *
     * TODO: this method together with scoreRegion have not been optimized
     *
     * @param region
     * @param metadata
     * @return
     * @throws QueryPlannerException
     */
    Object[] findBestJoinOrder(JoinRegion region, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        int regionCount = region.getJoinSourceNodes().size();

        List<Integer> orderList = new ArrayList<Integer>(regionCount);
        for(int i=0; i<regionCount; i++) {
            orderList.add(new Integer(i));
        }

        double bestSubScore = Double.MAX_VALUE;
        Object[] bestSubOrder = null;

        Permutation perms = new Permutation(orderList.toArray());

        int exhaustive = regionCount;

        //after 16 sources this will be completely greedy. before that it will try to strike a compromise between the exhaustive
        //and non-exhaustive searches
        boolean partial = false;
        if (regionCount > EXHAUSTIVE_SEARCH_GROUPS) {
            exhaustive = Math.max(2, EXHAUSTIVE_SEARCH_GROUPS - (int)Math.ceil(Math.sqrt((regionCount - EXHAUSTIVE_SEARCH_GROUPS))));
            partial = true;
        }

        Iterator<Object[]> permIter = perms.generate(exhaustive);

        while(permIter.hasNext()) {
            Object[] order = permIter.next();

            double score = region.scoreRegion(order, 0, metadata, capFinder, context, partial);
            if(score < bestSubScore) {
                bestSubScore = score;
                bestSubOrder = order;
            }
        }

        if (bestSubOrder == null) {
            return null;
        }

        if (regionCount <= exhaustive) {
            return bestSubOrder;
        }

        Integer[] result = new Integer[regionCount];

        //remove the joins that have already been placed
        for(int i=0; i<bestSubOrder.length; i++) {
            result[i] = (Integer)bestSubOrder[i];
            orderList.remove(bestSubOrder[i]);
        }

        while(!orderList.isEmpty()) {

            double bestPartialScore = Double.MAX_VALUE;
            List<Object> bestOrder = null;

            for (int i = 0; i < orderList.size(); i++) {
                Integer index = orderList.get(i);

                List<Object> order = new ArrayList<Object>(Arrays.asList(bestSubOrder));
                order.add(index);
                double partialScore = region.scoreRegion(order.toArray(), bestSubOrder.length - 1, metadata, capFinder, context, true);

                if (partialScore < bestPartialScore) {
                    bestPartialScore = partialScore;
                    bestOrder = order;
                }
            }

            if (bestOrder == null) {
                return null;
            }

            Integer next = (Integer)bestOrder.get(bestOrder.size() - 1);
            result[regionCount - orderList.size()] = next;
            orderList.remove(next);
            bestSubOrder = bestOrder.toArray();
        }

        return result;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PlanJoins"; //$NON-NLS-1$
    }

}
